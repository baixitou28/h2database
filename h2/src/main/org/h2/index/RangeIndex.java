/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.index;

import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.SessionLocal;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.IndexColumn;
import org.h2.table.RangeTable;
import org.h2.table.TableFilter;
import org.h2.value.Value;
import org.h2.value.ValueBigint;

/**
 * An index for the SYSTEM_RANGE table.
 * This index can only scan through all rows, search is not supported.
 */
public class RangeIndex extends VirtualTableIndex {

    private final RangeTable rangeTable;

    public RangeIndex(RangeTable table, IndexColumn[] columns) {
        super(table, "RANGE_INDEX", columns);
        this.rangeTable = table;
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
        long min = rangeTable.getMin(session);
        long max = rangeTable.getMax(session);
        long step = rangeTable.getStep(session);
        if (first != null) {//如果有最小值
            try {
                long v = first.getValue(0).getLong();//得到值
                if (step > 0) {
                    if (v > min) {
                        min += (v - min + step - 1) / step * step;//如果比min要大，跳一个step
                    }
                } else if (v > max) {//如果超过最大值，明显有问题
                    max = v;
                }
            } catch (DbException e) {
                // error when converting the value - ignore
            }
        }
        if (last != null) {//如果有最大值
            try {
                long v = last.getValue(0).getLong();//取值
                if (step > 0) {
                    if (v < max) {//比最大值大，明显有问题
                        max = v;
                    }
                } else if (v < min) {//比最小值小，明显有问题
                    min -= (min - v - step - 1) / step * step;//比最小值小，则取min
                }
            } catch (DbException e) {
                // error when converting the value - ignore
            }
        }
        return new RangeCursor(min, max, step);
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        return 1d;
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public boolean canGetFirstOrLast() {
        return true;
    }

    @Override
    public Cursor findFirstOrLast(SessionLocal session, boolean first) {
        long min = rangeTable.getMin(session);
        long max = rangeTable.getMax(session);
        long step = rangeTable.getStep(session);
        return new SingleRowCursor((step > 0 ? min <= max : min >= max)//tiger todo 没看明白
                ? Row.get(new Value[]{ ValueBigint.get(first ^ min >= max ? min : max) }, 1) : null);
    }

    @Override
    public String getPlanSQL() {
        return "range index";
    }

}
