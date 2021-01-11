/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import org.h2.api.ErrorCode;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.store.Data;
import org.h2.store.FileStore;
import org.h2.table.Table;
import org.h2.value.Value;

/**
 * An entry in a undo log.
 */
public class UndoLogRecord {//tiger log 重做一条记录，保存在内存里，或者文件里

    /**
     * Operation type meaning the row was inserted.
     */
    public static final short INSERT = 0;

    /**
     * Operation type meaning the row was deleted.
     */
    public static final short DELETE = 1;

    private static final int IN_MEMORY = 0, STORED = 1, IN_MEMORY_INVALID = 2;//重做记录是内存，还是STORED文件里
    private Table table;//哪个表
    private Row row;//哪一列
    private short operation;//增加还是删除
    private short state;//内存还是文件里
    private int filePos;//文件位置

    /**
     * Create a new undo log record
     *
     * @param table the table
     * @param op the operation type
     * @param row the row that was deleted or inserted
     */
    UndoLogRecord(Table table, short op, Row row) {
        this.table = table;
        this.row = row;
        this.operation = op;
        this.state = IN_MEMORY;
    }

    /**
     * Check if the log record is stored in the file.
     *
     * @return true if it is
     */
    boolean isStored() {
        return state == STORED;
    }

    /**
     * Check if this undo log record can be store. Only record can be stored if
     * the table has a unique index.
     *
     * @return if it can be stored
     */
    boolean canStore() {
        // if large transactions are enabled, this method is not called
        return table.getUniqueIndex() != null;//只有唯一索引的表，才能保存
    }

    /**
     * Un-do the operation. If the row was inserted before, it is deleted now,
     * and vice versa.
     *
     * @param session the session
     */
    void undo(SessionLocal session) {//真正的重做 //核心函数
        switch (operation) {
        case INSERT://如果是插入操作
            if (state == IN_MEMORY_INVALID) {
                state = IN_MEMORY;//放入内存
            }
            try {
                table.removeRow(session, row);//删除列
                table.fireAfterRow(session, row, null, true);//事件
            } catch (DbException e) {
                if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF
                        && e.getErrorCode() == ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1) {
                    // it might have been deleted by another thread
                    // ignore
                } else {
                    throw e;
                }
            }
            break;
        case DELETE://如果是删除
            try {
                table.addRow(session, row);//把记录加回去
                table.fireAfterRow(session, null, row, true);//事件
            } catch (DbException e) {
                if (session.getDatabase().getLockMode() == Constants.LOCK_MODE_OFF
                        && e.getSQLException().getErrorCode() == ErrorCode.DUPLICATE_KEY_1) {
                    // it might have been added by another thread
                    // ignore
                } else {
                    throw e;
                }
            }
            break;
        default:
            throw DbException.getInternalError("op=" + operation);
        }
    }

    /**
     * Append the row to the buffer.
     *
     * @param buff the buffer
     * @param log the undo log
     */
    void append(Data buff, UndoLog log) {//追加日志// TIGER LOG 日志的格式
        int p = buff.length();
        buff.writeInt(0);//先写零
        buff.writeInt(operation);//再写操作日志
        buff.writeInt(log.getTableId(table));//再写表的ID
        buff.writeLong(row.getKey());//写行或记录的主键
        int count = row.getColumnCount();
        buff.writeInt(count);//写列的个数
        for (int i = 0; i < count; i++) {//写所有列
            Value v = row.getValue(i);
            buff.checkCapacity(buff.getValueLen(v));
            buff.writeValue(v);
        }
        buff.fillAligned();//对齐
        buff.setInt(p, (buff.length() - p) / Constants.FILE_BLOCK_SIZE);//写长度
    }

    /**
     * Save the row in the file using a buffer.
     *
     * @param buff the buffer
     * @param file the file
     * @param log the undo log
     */
    void save(Data buff, FileStore file, UndoLog log) {//保存一个日志记录到文件里
        buff.reset();//buff清零
        append(buff, log);//追加一条
        filePos = (int) (file.getFilePointer() / Constants.FILE_BLOCK_SIZE);
        file.write(buff.getBytes(), 0, buff.length());//写入二进制
        row = null;
        state = STORED;//状态变为store，已保存
    }

    /**
     * Load an undo log record row using a buffer.
     *
     * @param buff the buffer
     * @param log the log
     * @return the undo log record
     */
    static UndoLogRecord loadFromBuffer(Data buff, UndoLog log) {//从buffer里面加载日志
        UndoLogRecord rec = new UndoLogRecord(null, (short) 0, null);
        int pos = buff.length();//
        int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;//长度
        rec.load(buff, log);//加载
        buff.setPos(pos + len);//位置设置在这条日志后面。
        return rec;
    }

    /**
     * Load an undo log record row using a buffer.
     *
     * @param buff the buffer
     * @param file the source file
     * @param log the log
     */
    void load(Data buff, FileStore file, UndoLog log) {//从
        int min = Constants.FILE_BLOCK_SIZE;
        log.seek(filePos);//找到相应位置
        buff.reset();
        file.readFully(buff.getBytes(), 0, min);//读入长度的字节部分
        int len = buff.readInt() * Constants.FILE_BLOCK_SIZE;//计算块的长度
        buff.checkCapacity(len);//尺寸ok吗？
        if (len - min > 0) {
            file.readFully(buff.getBytes(), min, len - min);//真正读入后续的二进制字节
        }
        int oldOp = operation;
        load(buff, log);//从buff里面读入一个日志记录
        if (operation != oldOp) {//如果读如的操作不对
            throw DbException.getInternalError("operation=" + operation + " op=" + oldOp);
        }
    }

    private void load(Data buff, UndoLog log) {//从buffer里面加载log，读的过程就是日志保存的具体格式
        operation = (short) buff.readInt();//操作类型
        table = log.getTable(buff.readInt());//表
        long key = buff.readLong();//key
        int columnCount = buff.readInt();//列数
        Value[] values = new Value[columnCount];//列的内容
        for (int i = 0; i < columnCount; i++) {//逐个读入
            values[i] = buff.readValue();
        }
        row = table.createRow(values, SearchRow.MEMORY_CALCULATE, key);//创建列
        state = IN_MEMORY_INVALID;//改变状态
    }

    /**
     * Get the table.
     *
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * Get the position in the file.
     *
     * @return the file position
     */
    public long getFilePos() {
        return filePos;
    }

    /**
     * Get the row that was deleted or inserted.
     *
     * @return the row
     */
    public Row getRow() {
        return row;
    }

    /**
     * Change the state from IN_MEMORY to IN_MEMORY_INVALID. This method is
     * called if a later record was read from the temporary file, and therefore
     * the position could have changed.
     */
    void invalidatePos() {//如果从临时文件中读取，当前的位置可能就不对了。
        if (this.state == IN_MEMORY) {
            state = IN_MEMORY_INVALID;
        }
    }
}
