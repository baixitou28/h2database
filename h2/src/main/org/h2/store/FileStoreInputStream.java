/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import org.h2.engine.Constants;
import org.h2.message.DbException;
import org.h2.tools.CompressTool;
import org.h2.util.Utils;

/**
 * An input stream that is backed by a file store.
 */
public class FileStoreInputStream extends InputStream {//在自定义的文件类FileStore上，再定义一个流FileStoreInputStream，开考虑了压缩的功能

    private FileStore store;//文件读入处理
    private final Data page;//行和列的序列化操作
    private int remainingInBuffer;//比output多这个
    private final CompressTool compress;
    private boolean endOfFile;
    private final boolean alwaysClose;

    public FileStoreInputStream(FileStore store, DataHandler handler,
            boolean compression, boolean alwaysClose) {
        this.store = store;
        this.alwaysClose = alwaysClose;
        if (compression) {
            compress = CompressTool.getInstance();
        } else {
            compress = null;
        }
        page = Data.create(handler, Constants.FILE_BLOCK_SIZE, true);//创建一个行的读写类
        try {
            if (store.length() <= FileStore.HEADER_LENGTH) {
                close();//数据长度不够
            } else {
                fillBuffer();//读入数据
            }
        } catch (IOException e) {
            throw DbException.convertIOException(e, store.name);
        }
    }

    @Override
    public int available() {
        return remainingInBuffer <= 0 ? 0 : remainingInBuffer;
    }

    @Override
    public int read(byte[] buff) throws IOException {//读入一个buff的所有内容，返回的是实际读入长度
        return read(buff, 0, buff.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {//读入 为什么要分多次block读入呢？
        if (len == 0) {
            return 0;
        }
        int read = 0;
        while (len > 0) {//TODO tiger 验证：为什么要多次readBlock，这里文件不一定是本地文件？
            int r = readBlock(b, off, len);//不停读入block，==>如果块太大，操作系统不能返回这么大块的值，所以这样这里设计的接口可以相对比较简单了
            if (r < 0) {
                break;
            }
            read += r;
            off += r;
            len -= r;
        }
        return read == 0 ? -1 : read;
    }

    private int readBlock(byte[] buff, int off, int len) throws IOException {//从buff里面读数据
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int l = Math.min(remainingInBuffer, len);//计算能读的长度
        page.read(buff, off, l);//从buffer读入
        remainingInBuffer -= l;//流里面的buffer变小
        return l;
    }

    private void fillBuffer() throws IOException {//读入
        if (remainingInBuffer > 0 || endOfFile) {
            return;//如果已经打开，且到末尾，则结束
        }
        page.reset();//重置
        store.openFile();//文件打开了吗？
        if (store.length() == store.getFilePointer()) {//如果已经到末尾了
            close();
            return;
        }
        store.readFully(page.getBytes(), 0, Constants.FILE_BLOCK_SIZE);//读入一个page
        page.reset();//位置重新设置以便读
        remainingInBuffer = page.readInt();//读入int
        if (remainingInBuffer < 0) {
            close();
            return;
        }
        page.checkCapacity(remainingInBuffer);//检查长度
        // get the length to read
        if (compress != null) {
            page.checkCapacity(Data.LENGTH_INT);
            page.readInt();
        }
        page.setPos(page.length() + remainingInBuffer);//设置长度
        page.fillAligned();//设置对齐
        int len = page.length() - Constants.FILE_BLOCK_SIZE;
        page.reset();
        page.readInt();
        store.readFully(page.getBytes(), Constants.FILE_BLOCK_SIZE, len);
        page.reset();
        page.readInt();
        if (compress != null) {//是否压缩
            int uncompressed = page.readInt();
            byte[] buff = Utils.newBytes(remainingInBuffer);
            page.read(buff, 0, remainingInBuffer);
            page.reset();
            page.checkCapacity(uncompressed);
            CompressTool.expand(buff, page.getBytes(), 0);
            remainingInBuffer = uncompressed;
        }
        if (alwaysClose) {//是否自动关闭
            store.closeFile();
        }
    }

    @Override
    public void close() {
        if (store != null) {
            try {
                store.close();
                endOfFile = true;
            } finally {
                store = null;
            }
        }
    }

    @Override
    protected void finalize() {
        close();
    }

    @Override
    public int read() throws IOException {//读一个byte
        fillBuffer();
        if (endOfFile) {
            return -1;
        }
        int i = page.readByte() & 0xff;
        remainingInBuffer--;
        return i;
    }

}
