/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore.db;

import org.h2.api.ErrorCode;
import org.h2.command.query.AllColumnsForPlan;
import org.h2.engine.Constants;
import org.h2.engine.SessionLocal;
import org.h2.index.Cursor;
import org.h2.index.IndexType;
import org.h2.message.DbException;
import org.h2.pagestore.Page;
import org.h2.pagestore.PageStore;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.result.SortOrder;
import org.h2.table.Column;
import org.h2.table.IndexColumn;
import org.h2.table.TableFilter;
import org.h2.util.MathUtils;
import org.h2.value.Value;
import org.h2.value.ValueLob;
import org.h2.value.ValueNull;

/**
 * The scan index allows to access a row by key. It can be used to iterate over
 * all rows of a table. Each regular table has one such object, even if no
 * primary key or indexes are defined.
 */
public class PageDataIndex extends PageIndex {

    private final PageStore store;
    private final PageStoreTable tableData;
    private long lastKey;
    private long rowCount;
    private int mainIndexColumn = -1;
    private DbException fastDuplicateKeyException;

    /**
     * The estimated heap memory per page, in number of double words (4 bytes
     * each).
     */
    private int memoryPerPage;
    private int memoryCount;

    public PageDataIndex(PageStoreTable table, int id, IndexColumn[] columns,
            IndexType indexType, boolean create, SessionLocal session) {
        super(table, id, table.getName() + "_DATA", columns, indexType);

        // trace = database.getTrace(Trace.PAGE_STORE + "_di");
        // trace.setLevel(TraceSystem.DEBUG);
        tableData = table;
        this.store = database.getPageStore();
        store.addIndex(this);//加入本索引
        if (!database.isPersistent()) {//必须持久化
            throw DbException.getInternalError(table.getName());
        }
        if (create) {//如果第一次创建
            rootPageId = store.allocatePage();//创建根节点
            store.addMeta(this, session);//增加元信息
            PageDataLeaf root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);//创建一个页
            store.update(root);
        } else {
            rootPageId = store.getRootPageId(id);//获取根节点
            PageData root = getPage(rootPageId, 0);//分配页
            lastKey = root.getLastKey();//一般是主键索引
            rowCount = root.getRowCount();
        }
        if (trace.isDebugEnabled()) {
            trace.debug("{0} opened rows: {1}", this, rowCount);
        }
        table.setRowCount(rowCount);
        memoryPerPage = (PageData.MEMORY_PAGE_DATA + store.getPageSize()) >> 2;//1/4倍的值分配 ,那一般只有4k/4?
    }

    @Override
    public DbException getDuplicateKeyException(String key) {
        if (fastDuplicateKeyException == null) {
            fastDuplicateKeyException = super.getDuplicateKeyException(null);
        }
        return fastDuplicateKeyException;
    }

    @Override
    public void add(SessionLocal session, Row row) {//tiger 加入和删除是最能看出结构的地方
        boolean retry = false;
        if (mainIndexColumn != -1) {//有主键，设置主键
            row.setKey(row.getValue(mainIndexColumn).getLong());//使用最近的key
        } else {
            if (row.getKey() == 0) {
                row.setKey((int) ++lastKey);//没有主索引就用lastKey，不需要老查找了。//这个lastKey好像不利于并发插入
                retry = true;//如果不是主键索引，可以再次尝试
            }
        }
        if (tableData.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v instanceof ValueLob) {
                    ValueLob lob = ((ValueLob) v).copy(database, getId());
                    session.removeAtCommitStop(lob);//标记，如果失败，记得删除
                    if (v != lob) {
                        row.setValue(i, lob);//把id，记录在row中
                    }
                }
            }
        }
        // when using auto-generated values, it's possible that multiple
        // tries are required (specially if there was originally a primary key)
        if (trace.isDebugEnabled()) {
            trace.debug("{0} add {1}", getName(), row);
        }
        long add = 0;
        while (true) {
            try {
                addTry(session, row);
                break;
            } catch (DbException e) {
                if (e != fastDuplicateKeyException) {//tiger
                    throw e;
                }
                if (!retry) {//如果是主键，不允许重复
                    e = DbException.get(ErrorCode.DUPLICATE_KEY_1,
                            getDuplicatePrimaryKeyMessage(mainIndexColumn).toString());
                    e.setSource(this);
                    throw e;
                }
                if (add == 0) {//因为使用最近的key失败，所以增加一个随机值，随后再随机值上加1，
                    // in the first re-try add a small random number,
                    // to avoid collisions after a re-start
                    row.setKey((long) (row.getKey() + Math.random() * 10_000));//add为0，则换一个key
                } else {
                    row.setKey(row.getKey() + add);
                }
                add++;
            } finally {
                store.incrementChangeCount();//增加
            }
        }
        lastKey = Math.max(lastKey, row.getKey());//取最大值
    }

    private void addTry(SessionLocal session, Row row) {//尝试插入 //TODO: tiger
        while (true) {//循环操作
            PageData root = getPage(rootPageId, 0);
            int splitPoint = root.addRowTry(row);
            if (splitPoint == -1) {
                break;
            }
            if (trace.isDebugEnabled()) {
                trace.debug("{0} split", this);
            }
            long pivot = splitPoint == 0 ? row.getKey() : root.getKey(splitPoint - 1);
            PageData page1 = root;
            PageData page2 = root.split(splitPoint);
            int id = store.allocatePage();
            page1.setPageId(id);
            page1.setParentPageId(rootPageId);
            page2.setParentPageId(rootPageId);
            PageDataNode newRoot = PageDataNode.create(this, rootPageId, PageData.ROOT);
            newRoot.init(page1, pivot, page2);
            store.update(page1);
            store.update(page2);
            store.update(newRoot);
            root = newRoot;
        }
        invalidateRowCount();
        rowCount++;
        store.logAddOrRemoveRow(session, tableData.getId(), row, true);
    }

    /**
     * Read an overflow page.
     *
     * @param id the page id
     * @return the page
     */
    PageDataOverflow getPageOverflow(int id) {
        Page p = store.getPage(id);
        if (p instanceof PageDataOverflow) {
            return (PageDataOverflow) p;
        }
        throw DbException.get(ErrorCode.FILE_CORRUPTED_1,
                p == null ? "null" : p.toString());
    }

    /**
     * Read the given page.
     *
     * @param id the page id
     * @param parent the parent, or -1 if unknown
     * @return the page
     */
    PageData getPage(int id, int parent) {
        Page pd = store.getPage(id);//通过store来统一获取页
        if (pd == null) {//如果没有，创建PageDataLeaf
            PageDataLeaf empty = PageDataLeaf.create(this, id, parent);
            // could have been created before, but never committed
            store.logUndo(empty, null);//不理解//TODO: tiger 重要 getPage
            store.update(empty);
            return empty;
        } else if (!(pd instanceof PageData)) {//如果不是需要的类型
            throw DbException.get(ErrorCode.FILE_CORRUPTED_1, String.valueOf(pd));
        }
        PageData p = (PageData) pd;
        if (parent != -1) {
            if (p.getParentPageId() != parent) {//如果父节点不对 //TIGER 什么时候需要？
                throw DbException.getInternalError(p + " parent " + p.getParentPageId() + " expected " + parent);
            }
        }
        return p;
    }

    /**
     * Get the key from the row.
     *
     * @param row the row
     * @param ifEmpty the value to use if the row is empty
     * @param ifNull the value to use if the column is NULL
     * @return the key
     */
    long getKey(SearchRow row, long ifEmpty, long ifNull) {
        if (row == null) {
            return ifEmpty;
        }
        Value v = row.getValue(mainIndexColumn);
        if (v == null) {
            return row.getKey();
        } else if (v == ValueNull.INSTANCE) {
            return ifNull;
        }
        return v.getLong();
    }

    @Override
    public Cursor find(SessionLocal session, SearchRow first, SearchRow last) {
        long from = first == null ? Long.MIN_VALUE : first.getKey();
        long to = last == null ? Long.MAX_VALUE : last.getKey();
        PageData root = getPage(rootPageId, 0);
        return root.find(session, from, to);

    }

    /**
     * Search for a specific row or a set of rows.
     *
     * @param session the session
     * @param first the key of the first row
     * @param last the key of the last row
     * @return the cursor
     */
    Cursor find(SessionLocal session, long first, long last) {
        PageData root = getPage(rootPageId, 0);
        return root.find(session, first, last);
    }

    long getLastKey() {
        PageData root = getPage(rootPageId, 0);
        return root.getLastKey();
    }

    @Override
    public double getCost(SessionLocal session, int[] masks,
            TableFilter[] filters, int filter, SortOrder sortOrder,
            AllColumnsForPlan allColumnsSet) {
        // The +200 is so that indexes that can return the same data, but have less
        // columns, will take precedence. This all works out easier in the MVStore case,
        // because MVStore uses the same cost calculation code for the ScanIndex (i.e.
        // the MVPrimaryIndex) and all other indices.
        return 10 * (tableData.getRowCountApproximation(session) +
                Constants.COST_ROW_OFFSET) + 200;//成本也有点太高了
    }

    @Override
    public boolean needRebuild() {
        return false;
    }

    @Override
    public void remove(SessionLocal session, Row row) {
        if (tableData.getContainsLargeObject()) {
            for (int i = 0, len = row.getColumnCount(); i < len; i++) {
                Value v = row.getValue(i);
                if (v instanceof ValueLob) {
                    session.removeAtCommit((ValueLob) v);
                }
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("{0} remove {1}", getName(), row);
        }
        if (rowCount == 1) {
            removeAllRows();
        } else {
            try {
                long key = row.getKey();
                PageData root = getPage(rootPageId, 0);
                root.remove(key);
                invalidateRowCount();
                rowCount--;
            } finally {
                store.incrementChangeCount();
            }
        }
        store.logAddOrRemoveRow(session, tableData.getId(), row, false);
    }

    @Override
    public void remove(SessionLocal session) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} remove", this);
        }
        removeAllRows();//删除所有行
        store.free(rootPageId);//删除根节点，和元数据
        store.removeMeta(this, session);
    }

    @Override
    public void truncate(SessionLocal session) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} truncate", this);
        }
        store.logTruncate(session, tableData.getId());//删除日志
        removeAllRows();//删除所有列
        if (tableData.getContainsLargeObject() && tableData.isPersistData()) {
            // unfortunately, the data is gone on rollback
            session.commit(false);//必须设置session commit 为false
            database.getLobStorage().removeAllForTable(table.getId());//删除lob
        }
        tableData.setRowCount(0);//设为0
    }

    private void removeAllRows() {
        try {
            PageData root = getPage(rootPageId, 0);//获取根节点
            root.freeRecursive();//递归删除
            root = PageDataLeaf.create(this, rootPageId, PageData.ROOT);//创建一个新的根节点
            store.removeFromCache(rootPageId);//从cache中删除
            store.update(root);//更新为新的节点
            rowCount = 0;
            lastKey = 0;
        } finally {
            store.incrementChangeCount();
        }
    }

    @Override
    public void checkRename() {
        throw DbException.getUnsupportedException("PAGE");
    }

    @Override
    public Row getRow(SessionLocal session, long key) {
        return getRowWithKey(key);
    }

    /**
     * Get the row with the given key.
     *
     * @param key the key
     * @return the row
     */
    public Row getRowWithKey(long key) {
        PageData root = getPage(rootPageId, 0);
        return root.getRowWithKey(key);
    }

    PageStore getPageStore() {
        return store;
    }

    @Override
    public long getRowCountApproximation(SessionLocal session) {
        return rowCount;
    }

    @Override
    public long getRowCount(SessionLocal session) {
        return rowCount;
    }

    @Override
    public long getDiskSpaceUsed() {
        PageData root = getPage(rootPageId, 0);
        return root.getDiskSpaceUsed();
    }

    @Override
    public String getCreateSQL() {
        return null;
    }

    @Override
    public int getColumnIndex(Column col) {
        // can not use this index - use the PageDelegateIndex instead
        return -1;
    }

    @Override
    public boolean isFirstColumn(Column column) {
        return false;
    }

    @Override
    public void close(SessionLocal session) {
        if (trace.isDebugEnabled()) {
            trace.debug("{0} close", this);
        }
        // can not close the index because it might get used afterwards,
        // for example after running recovery
        writeRowCount();
    }

    /**
     * The root page has changed.
     *
     * @param session the session
     * @param newPos the new position
     */
    void setRootPageId(SessionLocal session, int newPos) {
        store.removeMeta(this, session);
        this.rootPageId = newPos;
        store.addMeta(this, session);
        store.addIndex(this);
    }

    public void setMainIndexColumn(int mainIndexColumn) {
        this.mainIndexColumn = mainIndexColumn;
    }

    public int getMainIndexColumn() {
        return mainIndexColumn;
    }

    @Override
    public String toString() {
        return getName();
    }

    private void invalidateRowCount() {
        PageData root = getPage(rootPageId, 0);
        root.setRowCountStored(PageData.UNKNOWN_ROWCOUNT);
    }

    @Override
    public void writeRowCount() {
        try {
            PageData root = getPage(rootPageId, 0);
            root.setRowCountStored(MathUtils.convertLongToInt(rowCount));
        } finally {
            store.incrementChangeCount();
        }
    }

    @Override
    public String getPlanSQL() {
        return table.getSQL(new StringBuilder(), TRACE_SQL_FLAGS).append(".tableScan").toString();//这里也返回tableScan
    }

    int getMemoryPerPage() {
        return memoryPerPage;
    }

    /**
     * The memory usage of a page was changed. The new value is used to adopt
     * the average estimated memory size of a page.
     *
     * @param x the new memory size
     */
    void memoryChange(int x) {//tiger 未知
        if (memoryCount < Constants.MEMORY_FACTOR) {
            memoryPerPage += (x - memoryPerPage) / ++memoryCount;
        } else {
            memoryPerPage += (x > memoryPerPage ? 1 : -1) +
                    ((x - memoryPerPage) / Constants.MEMORY_FACTOR);
        }
    }

    @Override
    public boolean isRowIdIndex() {
        return true;
    }

}
