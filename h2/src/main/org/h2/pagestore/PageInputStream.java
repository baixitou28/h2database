/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.pagestore;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.BitSet;

import org.h2.message.DbException;
import org.h2.message.Trace;

/**
 * An input stream that reads from a page store.
 */
public class PageInputStream extends InputStream {//tiger

    private final PageStore store;
    private final Trace trace;
    private final int firstTrunkPage;
    private final PageStreamTrunk.Iterator trunkIterator;
    private int dataPage;
    private PageStreamTrunk trunk;
    private int trunkIndex;
    private PageStreamData data;
    private int dataPos;
    private boolean endOfFile;
    private int remaining;
    private final byte[] buffer = { 0 };
    private int logKey;

    PageInputStream(PageStore store, int logKey, int firstTrunkPage, int dataPage) {//构造函数
        this.store = store;
        this.trace = store.getTrace();
        // minus one because we increment before comparing
        this.logKey = logKey - 1;
        this.firstTrunkPage = firstTrunkPage;
        trunkIterator = new PageStreamTrunk.Iterator(store, firstTrunkPage);
        this.dataPage = dataPage;
    }

    @Override
    public int read() throws IOException {
        int len = read(buffer);//只有1个直接的buffer
        return len < 0 ? -1 : (buffer[0] & 255);//收个是长度
    }

    @Override
    public int read(byte[] b) throws IOException {//读入整个二进制文件
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {
            int r = readBlock(b, off, len);//反复读入，len可能很大，page很多？不一定一次能读完，所以用block
            if (r < 0) {
                break;//直到没有
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] buff, int off, int len) throws IOException {
        try {
            fillBuffer();
            if (endOfFile) {
                return -1;//文件末尾了
            }
            int l = Math.min(remaining, len);//避免溢出
            data.read(dataPos, buff, off, l);//读入
            remaining -= l;
            dataPos += l;
            return l;
        } catch (DbException e) {
            throw new EOFException();
        }
    }

    private void fillBuffer() {//TIGER
        if (remaining > 0 || endOfFile) {
            return;
        }
        int next;
        while (true) {
            if (trunk == null) {//如果是0
                trunk = trunkIterator.next();//获取
                trunkIndex = 0;
                logKey++;
                if (trunk == null || trunk.getLogKey() != logKey) {//如果获取不到或者key不等
                    endOfFile = true;
                    return;
                }
            }
            if (trunk != null) {//
                next = trunk.getPageData(trunkIndex++);//取第n个page
                if (next == -1) {
                    trunk = null;
                } else if (dataPage == -1 || dataPage == next) {
                    break;
                }
            }
        }
        if (trace.isDebugEnabled()) {
            trace.debug("pageIn.readPage " + next);
        }
        dataPage = -1;
        data = null;
        Page p = store.getPage(next);//获取PageStreamData页
        if (p instanceof PageStreamData) {
            data = (PageStreamData) p;
        }
        if (data == null || data.getLogKey() != logKey) {
            endOfFile = true;
            return;
        }
        dataPos = PageStreamData.getReadStart();//位置
        remaining = store.getPageSize() - dataPos;//还剩多少
    }

    /**
     * Set all pages as 'allocated' in the page store.
     *
     * @return the bit set
     */
    BitSet allocateAllPages() {//返回BitSet标识占用的页
        BitSet pages = new BitSet();//01.
        int key = logKey;
        PageStreamTrunk.Iterator it = new PageStreamTrunk.Iterator(//02. 取iterator
                store, firstTrunkPage);
        while (true) {
            PageStreamTrunk t = it.next();
            key++;
            if (it.canDelete()) {//03.如果
                store.allocatePage(it.getCurrentPageId());//分配
            }
            if (t == null || t.getLogKey() != key) {
                break;
            }
            pages.set(t.getPos());//BitSet上标记
            for (int i = 0;; i++) {
                int n = t.getPageData(i);
                if (n == -1) {
                    break;
                }
                pages.set(n);//循环标记
                store.allocatePage(n);
            }
        }
        return pages;
    }

    int getDataPage() {
        return data.getPos();
    }

    @Override
    public void close() {
        // nothing to do
    }

}
