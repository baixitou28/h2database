/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.Index;
import org.h2.index.IndexCondition;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.Utils;
import org.h2.value.DataType;
import org.h2.value.Value;

/**
 * A non-unique index based on an in-memory hash map.
 *
 * @author Sergi Vladykin
 */
public class NonUniqueHashIndex extends Index {//tiger INDEX

    /**
     * The index of the indexed column.
     */
    private final int indexColumn;
    private final boolean totalOrdering;
    private Map<Value, ArrayList<Long>> rows;
    private final PageStoreTable tableData;
    private long rowCount;

    public NonUniqueHashIndex(PageStoreTable table, int id, String indexName,
            IndexColumn[] columns, IndexType indexType) {
        super(table, id, indexName, columns, indexType);
        Column column = columns[0].column;
        indexColumn = column.getColumnId();
        totalOrdering = DataType.hasTotalOrdering(column.getType().getValueType());
        tableData = table;
        reset();
    }

    private void reset() {
        rows = totalOrdering ? new HashMap<>() : new TreeMap<>(database.getCompareMode());
        rowCount = 0;
    }

    @Override
    public void truncate(SessionLocal session) {
        reset();
    }

    @Override
    public void add(SessionLocal session, Row row) {
        Value key = row.getValue(indexColumn);
        ArrayList<Long> positions = rows.get(key);//每个key 对应一个list，就是所谓的桶
        if (positions == null) {
            positions = Utils.newSmallArrayList();
            rows.put(key, positions);
        }
        positions.add(row.getKey());//在
        rowCount++;
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        if (rowCount == 1) {
            // last row in table
            reset();//如果只有一个元素，简单的reset就可以了
        } else {
            Value key = row.getValue(indexColumn);
            ArrayList<Long> positions = rows.get(key);//找到桶
            if (positions.size() == 1) {
                // last row with such key
                rows.remove(key);
            } else {
                positions.remove(row.getKey());//删除桶里的一个元素
            }
            rowCount--;
        }
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {//
        if (first == null || last == null) {
            throw DbException.getInternalError(first + " " + last);
        }
        if (first != last) {
            if (TreeIndex.compareKeys(first, last) != 0) {//利用TreeIndex来比对区间是否合理
                throw DbException.getInternalError();
            }
        }
        Value v = first.getValue(indexColumn);
        /*
         * Sometimes the incoming search is a similar, but not the same type
         * e.g. the search value is INT, but the index column is LONG. In which
         * case we need to convert, otherwise the HashMap will not find the
         * result.
         */
        v = v.convertTo(tableData.getColumn(indexColumn).getType(), session);//int 和long 有时候也会成为困难，如果仅用一种类型，可能会浪费存储空间
        ArrayList<Long> positions = rows.get(v);
        return new NonUniqueHashCursor(session, tableData, positions);//返回cursor
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return rowCount;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return rowCount;
    }

    @Override
    public void close(SessionLocal session) {
        // nothing to do
    }

    @Override
    public void remove(SessionLocal session) {
        // nothing to do
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        for (Column column : columns) {
            int index = column.getColumnId();
            int mask = masks[index];
            if ((mask & IndexCondition.EQUALITY) != IndexCondition.EQUALITY) {//仅支持等号
                return Long.MAX_VALUE;
            }
        }
        return 2;
    }

    @Override
    public boolean needRebuild() {
        return true;
    }

    @Override
    public boolean canScan() {
        return false;
    }

}
