/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * Page Store implementation of a row.
 */
public final class PageStoreRow {//tiger page 列的实现：内部定义了一个空的数组EMPTY_ARRAY和空的搜索数组EMPTY_SEARCH_ARRAY

    /**
     * An empty array of Row objects.
     */
    static final Row[] EMPTY_ARRAY = new Row[0];

    /**
     * An empty array of SearchRow objects.
     */
    static final SearchRow[] EMPTY_SEARCH_ARRAY = new SearchRow[0];

    /**
     * The implementation of a removed row in an in-memory table.
     */
    static final class RemovedRow extends Row {//定义一个RemovedRow 包含一个构造函数，setValue，getValue

        RemovedRow(long key) {
            setKey(key);
        }//构造是用key

        @Override
        public Value getValue(int i) {//
            if (i == ROWID_INDEX) {
                return ValueBigint.get(key);//一个伪列，返回key值
            }
            throw DbException.getInternalError();
        }

        @Override
        public void setValue(int i, Value v) {//一个伪列，设置key值
            if (i == ROWID_INDEX) {
                key = v.getLong();
            } else {
                throw DbException.getInternalError();
            }
        }

        @Override
        public int getColumnCount() {
            return 0;
        }//已删除，没有列了

        @Override
        public String toString() {
            return "( /* key:" + key + " */ )";
        }

        @Override
        public int getMemory() {
            return Constants.MEMORY_ROW;
        }

        @Override
        public Value[] getValueList() {//删除了，就没有值了
            return null;
        }

        @Override
        public void copyFrom(SearchRow source) {
            setKey(source.getKey());
        }
    }

    private PageStoreRow() {
    }

}
