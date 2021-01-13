/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import java.util.ArrayList;

import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.Utils;

/**
 * The scan index is not really an 'index' in the strict sense, because it can
 * not be used for direct lookup. It can only be used to iterate over all rows
 * of a table. Each regular table has one such object, even if no primary key or
 * indexes are defined.
 */
public class ScanIndex extends Index {//tiger INDEX 是一个逻辑的概念，不是严格意思的索引，其中删除一行，并没有直接删除，而是标记，仍然放在rows中
    private long firstFree = -1;
    private ArrayList<Row> rows = Utils.newSmallArrayList();
    private final PageStoreTable tableData;
    private long rowCount;

    public ScanIndex(PageStoreTable table, int id, IndexColumn[] columns,
            IndexType indexType) {
        super(table, id, table.getName() + "_DATA", columns, indexType);
        tableData = table;
    }

    @Override
    public void remove(SessionLocal session) {
        truncate(session);
    }

    @Override
    public void truncate(SessionLocal session) {
        rows = Utils.newSmallArrayList();
        firstFree = -1;
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            database.getLobStorage().removeAllForTable(table.getId());
        }
        tableData.setRowCount(0);
        rowCount = 0;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }//没有建表语句

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        return rows.get((int) key);
    }

    @Override
    public void add(SessionLocal session, Row row) {
        // in-memory
        if (firstFree == -1) {//如果以前删除过，即firstFree 不为空
            int key = rows.size();
            row.setKey(key);
            rows.add(row);//直接加入
        } else {
            long key = firstFree;//最后一个
            Row free = rows.get((int) key);
            firstFree = free.getKey();//因为是单向列表，firstFree更新为指向的下一个
            row.setKey(key);//更新
            rows.set((int) key, row);//插入
        }
        rowCount++;
    }

    @Override
    public void remove(SessionLocal session, Row row) {//删除一行，并没有直接删除，而是标记，仍然放在rows中
        // in-memory
        if (rowCount == 1) {//如果只有一个，简单模式处理
            rows = Utils.newSmallArrayList();
            firstFree = -1;
        } else {
            Row free = new PageStoreRow.RemovedRow(firstFree);//生成一个free row
            long key = row.getKey();
            if (rows.size() <= key) {//如果范围超出
                throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
                        rows.size() + ": " + key);
            }
            rows.set((int) key, free);//将rows里第key个用free来替代
            firstFree = key;//更新第一个空闲的值==>这样所有的空闲row，形成了一个单向列表，但适合并发吗？它是如何处理的，是上层吗？
        }
        rowCount--;//整体减1
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {//返回自己的游标
        return new ScanCursor(this);
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return tableData.getRowCountApproximation(session) + Constants.COST_ROW_OFFSET;//常规设置为1000+行数，就是很不推荐的意思
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return rowCount;
    }

    /**
     * Get the next row that is stored after this row.
     *
     * @param row the current row or null to start the scan
     * @return the next row or null if there are no more rows
     */
    Row getNextRow(Row row) {//下一个行
        long key;
        if (row == null) {
            key = -1;
        } else {
            key = row.getKey();
        }
        while (true) {
            key++;
            if (key >= rows.size()) {//如果key 超出范围
                return null;
            }
            row = rows.get((int) key);//按key值获取row，若RemovedRow.getValueList()为空
            if (row.getValueList() != null) {//如果不是RemovedRow则继续循环，
                return row;
            }//如果有很多删除，这里有很多RemovedRow
        }
    }

    @Override
    public int getColumnIndex(Column col) {
        // the scan index cannot use any columns
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("SCAN");
    }

    @Override
    public boolean needRebuild() {
        return false;
    }//重来不需要重建

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return rowCount;
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL(new StringBuilder(), TRACE_SQL_FLAGS).append(".tableScan").toString();//explian时候的标记
    }

}
