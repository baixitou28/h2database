/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import java.util.HashMap;

import org.h2.store.Data;
import org.h2.store.FileStore;
import org.h2.table.Table;
import org.h2.util.Utils;

/**
 * Each session keeps a undo log if rollback is required.
 */
public class UndoLog {//TODO: tiger 重做日志的机制

    private final Database database;
    private final ArrayList<Long> storedEntriesPos = Utils.newSmallArrayList();
    private final ArrayList<UndoLogRecord> records = Utils.newSmallArrayList();//内存里的重做日志
    private FileStore file;
    private Data rowBuff;
    private int memoryUndo;
    private int storedEntries;//文件里的重做日志
    private HashMap<Integer, Table> tables;

    /**
     * Create a new undo log for the given session.
     *
     * @param database the database
     */
    UndoLog(Database database) {
        this.database = database;
    }

    /**
     * Get the number of active rows in this undo log.
     *
     * @return the number of rows
     */
    public int size() {
        return storedEntries + records.size();
    }//内存的日志+文件里的日志

    /**
     * Clear the undo log. This method is called after the transaction is
     * committed.
     */
    void clear() {//删除
        records.clear();
        storedEntries = 0;
        storedEntriesPos.clear();
        memoryUndo = 0;
        if (file != null) {
            file.closeAndDeleteSilently();
            file = null;
            rowBuff = null;
        }
    }

    /**
     * Get the last record and remove it from the list of operations.
     *
     * @return the last record
     */

    public UndoLogRecord getLast() {//得到最后的重做日志
        int i = records.size() - 1;//先内存里面看看有没有
        if (i < 0 && storedEntries > 0) {//如果内存里面没有，则从文件里找
            int last = storedEntriesPos.size() - 1;//最后一个
            long pos = storedEntriesPos.remove(last);//溢出
            long end = file.length();
            int bufferLength = (int) (end - pos);//删除最后一个，还剩的长度
            Data buff = Data.create(database, bufferLength, true);
            file.seek(pos);//文件中找到这条记录
            file.readFully(buff.getBytes(), 0, bufferLength);
            while (buff.length() < bufferLength) {//反复读入
                UndoLogRecord e = UndoLogRecord.loadFromBuffer(buff, this);
                records.add(e);
                memoryUndo++;
            }
            storedEntries -= records.size();//保存的重做日志数目减少
            file.setLength(pos);//设置当前位置
            file.seek(pos);//跳到相应位置。因为前面是流读取
        }
        i = records.size() - 1;//现在内存里有记录了
        UndoLogRecord entry = records.get(i);//获取记录
        if (entry.isStored()) {//如果是文件里的
            int start = Math.max(0, i - database.getMaxMemoryUndo() / 2);
            UndoLogRecord first = null;
            for (int j = start; j <= i; j++) {//前面的记录处理
                UndoLogRecord e = records.get(j);
                if (e.isStored()) {
                    e.load(rowBuff, file, this);
                    memoryUndo++;
                    if (first == null) {
                        first = e;
                    }
                }
            }
            for (int k = 0; k < i; k++) {//前面的失效？
                UndoLogRecord e = records.get(k);
                e.invalidatePos();
            }
            seek(first.getFilePos());//更新位置
        }
        return entry;
    }

    /**
     * Go to the right position in the file.
     *
     * @param filePos the position in the file
     */
    void seek(long filePos) {
        file.seek(filePos * Constants.FILE_BLOCK_SIZE);
    }

    /**
     * Remove the last record from the list of operations.
     */
    void removeLast() {//移除最后一个
        int i = records.size() - 1;
        UndoLogRecord r = records.remove(i);
        if (!r.isStored()) {
            memoryUndo--;
        }
    }

    /**
     * Append an undo log entry to the log.
     *
     * @param entry the entry
     */
    void add(UndoLogRecord entry) {//增加一个重做日志，一般5万次才刷新一次
        records.add(entry);
        memoryUndo++;//增加统计
        if (memoryUndo > database.getMaxMemoryUndo() &&//只有满足最大次数，默认是5万，有点大，虽然性能好，但是断电危险比较大
                database.isPersistent() &&//是磁盘,需要持久化
                !database.isMVStore()) {//但不mv
            if (file == null) {//如果没有文件则创建
                String fileName = database.createTempFile();
                file = database.openFile(fileName, "rw", false);
                file.autoDelete();//正常关闭，会自动删除，
                file.setCheckedWriting(false);
                file.setLength(FileStore.HEADER_LENGTH);
            }
            Data buff = Data.create(database, Constants.DEFAULT_PAGE_SIZE, true);//创建一个读写buff，用于序列化行记录
            for (int i = 0; i < records.size(); i++) {//循环读取已有的日志
                UndoLogRecord r = records.get(i);
                buff.checkCapacity(Constants.DEFAULT_PAGE_SIZE);
                r.append(buff, this);//序列化到buff里
                if (i == records.size() - 1 || buff.length() > Constants.UNDO_BLOCK_SIZE) {
                    storedEntriesPos.add(file.getFilePointer());
                    file.write(buff.getBytes(), 0, buff.length());//写二进制
                    buff.reset();//重置buff
                }
            }
            storedEntries += records.size();//增加统计值
            memoryUndo = 0;//清零
            records.clear();
        }
    }

    /**
     * Get the table id for this undo log. If the table is not registered yet,
     * this is done as well.
     *
     * @param table the table
     * @return the id
     */
    int getTableId(Table table) {
        int id = table.getId();
        if (tables == null) {
            tables = new HashMap<>();
        }
        // need to overwrite the old entry, because the old object
        // might be deleted in the meantime
        tables.put(id, table);
        return id;
    }

    /**
     * Get the table for this id. The table must be registered for this undo log
     * first by calling getTableId.
     *
     * @param id the table id
     * @return the table object
     */
    Table getTable(int id) {
        return tables.get(id);
    }

}
