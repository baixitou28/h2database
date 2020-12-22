/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * A storage mechanism that "persists" data in the off-heap area of the main
 * memory.
 */
public class OffHeapStore extends FileStore {//tiger 实现java的off heap模式的存储

    private final TreeMap<Long, ByteBuffer> memory =
            new TreeMap<>();

    @Override
    public void open(String fileName, boolean readOnly, char[] encryptionKey) {
        memory.clear();
    }

    @Override
    public String toString() {
        return memory.toString();
    }

    @Override
    public ByteBuffer readFully(long pos, int len) {
        Entry<Long, ByteBuffer> memEntry = memory.floorEntry(pos);//算出是哪个页
        if (memEntry == null) {
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_READING_FAILED,
                    "Could not read from position {0}", pos);
        }
        readCount.incrementAndGet();//统计在前，因为内存几乎不会失败，但还是放后面更合理
        readBytes.addAndGet(len);
        ByteBuffer buff = memEntry.getValue();//获取页
        ByteBuffer read = buff.duplicate();//直接复制页
        int offset = (int) (pos - memEntry.getKey());//记录开始的地方
        read.position(offset);//起点
        read.limit(len + offset);//终点
        return read.slice();//没用过，截取
    }

    @Override
    public void free(long pos, int length) {
        freeSpace.free(pos, length);
        ByteBuffer buff = memory.remove(pos);
        if (buff == null) {
            // nothing was written (just allocated)
        } else if (buff.remaining() != length) {//长度不对
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_READING_FAILED,
                    "Partial remove is not supported at position {0}", pos);
        }
    }

    @Override
    public void writeFully(long pos, ByteBuffer src) {
        fileSize = Math.max(fileSize, pos + src.remaining());
        Entry<Long, ByteBuffer> mem = memory.floorEntry(pos);//获取对应页面
        if (mem == null) {
            // not found: create a new entry
            writeNewEntry(pos, src);
            return;
        }
        long prevPos = mem.getKey();
        ByteBuffer buff = mem.getValue();
        int prevLength = buff.capacity();
        int length = src.remaining();
        if (prevPos == pos) {//是否相等
            if (prevLength != length) {//如果不一致，说明不对
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_READING_FAILED,
                        "Could not write to position {0}; " +
                        "partial overwrite is not supported", pos);//只支持整个页面全部更新
            }
            writeCount.incrementAndGet();
            writeBytes.addAndGet(length);
            buff.rewind();//
            buff.put(src);//
            return;
        }
        if (prevPos + prevLength > pos) {//如果pos在页面内，就是有错误了。
            throw DataUtils.newMVStoreException(
                    DataUtils.ERROR_READING_FAILED,
                    "Could not write to position {0}; " +
                    "partial overwrite is not supported", pos);
        }
        writeNewEntry(pos, src);//分配off heap 内存
    }

    private void writeNewEntry(long pos, ByteBuffer src) {//tiger allocateDirect 就是off heap内存
        int length = src.remaining();
        writeCount.incrementAndGet();
        writeBytes.addAndGet(length);
        ByteBuffer buff = ByteBuffer.allocateDirect(length);//直接分配off heap内存
        buff.put(src);
        buff.rewind();
        memory.put(pos, buff);//放入列表
    }

    @Override
    public void truncate(long size) {
        writeCount.incrementAndGet();//统计
        if (size == 0) {//一般是全部删除
            fileSize = 0;
            memory.clear();
            return;
        }
        fileSize = size;
        for (Iterator<Long> it = memory.keySet().iterator(); it.hasNext();) {
            long pos = it.next();
            if (pos < size) {
                break;
            }
            ByteBuffer buff = memory.get(pos);
            if (buff.capacity() > size) {//不支持部分删除
                throw DataUtils.newMVStoreException(
                        DataUtils.ERROR_READING_FAILED,
                        "Could not truncate to {0}; " +
                        "partial truncate is not supported", pos);
            }
            it.remove();
        }
    }

    @Override
    public void close() {
        memory.clear();
    }

    @Override
    public void sync() {
        // nothing to do
    }

    @Override
    public int getDefaultRetentionTime() {
        return 0;
    }

}
