/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store;

import java.io.OutputStream;
import java.util.Arrays;

import org.h2.engine.Constants;
import org.h2.tools.CompressTool;

/**
 * An output stream that is backed by a file store.
 */
public class FileStoreOutputStream extends OutputStream {//tiger 输出流，和输入对称 ，相比这里没有remainingInBuffer，直接写
    private FileStore store;//文件读入处理
    private final Data page;//行和列的序列化操作
    private final String compressionAlgorithm;//压缩算法名称
    private final CompressTool compress;//压缩
    private final byte[] buffer = { 0 };

    public FileStoreOutputStream(FileStore store, DataHandler handler,
            String compressionAlgorithm) {
        this.store = store;
        if (compressionAlgorithm != null) {
            this.compress = CompressTool.getInstance();
            this.compressionAlgorithm = compressionAlgorithm;
        } else {
            this.compress = null;
            this.compressionAlgorithm = null;
        }
        page = Data.create(handler, Constants.FILE_BLOCK_SIZE, true);
    }

    @Override
    public void write(int b) {//写一个字节
        buffer[0] = (byte) b;
        write(buffer);
    }

    @Override
    public void write(byte[] buff) {
        write(buff, 0, buff.length);
    }

    @Override
    public void write(byte[] buff, int off, int len) {
        if (len > 0) {
            page.reset();

            if (compress != null) {//如果是压缩模式
                if (off != 0 || len != buff.length) {//如果不是在buff的合适位置
                    buff = Arrays.copyOfRange(buff, off, off + len);
                    off = 0;
                }
                int uncompressed = len;//为压缩的长度
                buff = compress.compress(buff, compressionAlgorithm);//in place 压缩
                len = buff.length;//压缩后的长度
                page.checkCapacity(2 * Data.LENGTH_INT + len);//容量
                page.writeInt(len);//写长度(序列化)
                page.writeInt(uncompressed);//写压缩后的长度(序列化)
                page.write(buff, off, len);//写压缩后的内容(序列化)
            } else {//如果不是压缩，
                page.checkCapacity(Data.LENGTH_INT + len);//检查长度
                page.writeInt(len);//写可变长度长度(序列化)
                page.write(buff, off, len);//写内容(序列化)
            }
            page.fillAligned();//对齐
            store.write(page.getBytes(), 0, page.length());//序列化内容写文件
        }
    }

    @Override
    public void close() {
        if (store != null) {
            try {
                store.close();
            } finally {
                store = null;
            }
        }
    }

}
