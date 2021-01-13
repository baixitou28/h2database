/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import org.h2.index.Cursor;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;

/**
 * The cursor implementation for the scan index.
 */
public class ScanCursor implements Cursor {//tiger 简单的游标ScanCursor，可以实现简单的下一行next()
    private final ScanIndex scan;
    private Row row;

    ScanCursor(ScanIndex scan) {//构造函数，只需要索引即可
        this.scan = scan;
        row = null;
    }

    @Override
    public Row get() {
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return row;
    }

    @Override
    public boolean next() {
        row = scan.getNextRow(row);//根据索引获取下一个行，并更新
        return row != null;//返回状态
    }

    @Override
    public boolean previous() {
        throw DbException.getInternalError(toString());
    }

}
