/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (https://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.cache;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.h2.store.fs.FileBase;
import org.h2.store.fs.FilePath;
import org.h2.store.fs.FilePathWrapper;

/**
 * A file with a read cache.
 */
public class FilePathCache extends FilePathWrapper {//带每页4k，最多256页的文件处理类

    /**
     * The instance.
     */
    public static final FilePathCache INSTANCE = new FilePathCache();//静态类

    /**
     * Register the file system.
     */
    static {
        FilePath.register(INSTANCE);//注册文件系统
    }

    public static FileChannel wrap(FileChannel f) {
        return new FileCache(f);
    }//每个文件对饮一个cache

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileCache(getBase().open(mode));
    }

    @Override
    public String getScheme() {
        return "cache";
    }

    /**
     * A file with a read cache.
     */
    public static class FileCache extends FileBase {//1M的cache，即256块内存，这个是静态类

        private static final int CACHE_BLOCK_SIZE = 4 * 1024;//按照4k长度来读取
        private final FileChannel base;

        private final CacheLongKeyLIRS<ByteBuffer> cache;

        {
            CacheLongKeyLIRS.Config cc = new CacheLongKeyLIRS.Config();
            // 1 MB cache size
            cc.maxMemory = 1024 * 1024;
            cache = new CacheLongKeyLIRS<>(cc);
        }

        FileCache(FileChannel base) {
            this.base = base;
        }

        @Override
        protected void implCloseChannel() throws IOException {
            base.close();
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            base.position(newPosition);
            return this;
        }

        @Override
        public long position() throws IOException {
            return base.position();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            return base.read(dst);
        }
//按照cache 4k长度来读，每次读不超过4k
        @Override
        public synchronized int read(ByteBuffer dst, long position) throws IOException {//对于某个位置的数据读取 ByteBuffer的长度，不超过4K
            long cachePos = getCachePos(position);//换算cache位置
            int off = (int) (position - cachePos);//从某个cache块开始的位置
            int len = CACHE_BLOCK_SIZE - off;//
            len = Math.min(len, dst.remaining());//每次读入不超过cache块大小，
            ByteBuffer buff = cache.get(cachePos);//先从cache 取 4K
            if (buff == null) {//如果cache 未载入
                buff = ByteBuffer.allocate(CACHE_BLOCK_SIZE);//分配4K
                long pos = cachePos;
                while (true) {//不停读，直到满
                    int read = base.read(buff, pos);//从pos开始顺序读入，知道buff满为止
                    if (read <= 0) {
                        break;
                    }
                    if (buff.remaining() == 0) {//4k buff满了
                        break;
                    }
                    pos += read;//累加
                }
                int read = buff.position();//查看buff的位置
                if (read == CACHE_BLOCK_SIZE) {
                    cache.put(cachePos, buff, CACHE_BLOCK_SIZE);//4k数据放入cache
                } else {
                    if (read <= 0) {
                        return -1;
                    }
                    len = Math.min(len, read - off);
                }
            }
            dst.put(buff.array(), off, len);//放入用户的dst
            return len == 0 ? -1 : len;
        }

        private static long getCachePos(long pos) {//折算
            return (pos / CACHE_BLOCK_SIZE) * CACHE_BLOCK_SIZE;
        }

        @Override
        public long size() throws IOException {
            return base.size();
        }

        @Override
        public synchronized FileChannel truncate(long newSize) throws IOException {
            cache.clear();//删除cache
            base.truncate(newSize);//调整长度
            return this;
        }

        @Override
        public synchronized int write(ByteBuffer src, long position) throws IOException {
            clearCache(src, position);//废弃position部分
            return base.write(src, position);//写
        }

        @Override
        public synchronized int write(ByteBuffer src) throws IOException {//写入是删除对应cache内容，可以n个4k
            clearCache(src, position());
            return base.write(src);//写
        }

        private void clearCache(ByteBuffer src, long position) {//删除对应的cache，可能有n个4k
            if (cache.size() > 0) {
                int len = src.remaining();
                long p = getCachePos(position);
                while (len > 0) {
                    cache.remove(p);
                    p += CACHE_BLOCK_SIZE;
                    len -= CACHE_BLOCK_SIZE;
                }
            }
        }

        @Override
        public void force(boolean metaData) throws IOException {
            base.force(metaData);
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared)
                throws IOException {
            return base.tryLock(position, size, shared);
        }

        @Override
        public String toString() {
            return "cache:" + base.toString();
        }

    }

}
