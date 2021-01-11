/*
 * Copyright 2004-2020 H2 Group. Multiple-Licensed under the MPL 2.0, and the
 * EPL 1.0 (https://h2database.com/html/license.html). Initial Developer: H2
 * Group
 */
package org.h2.store;

import java.io.IOException;
import java.io.InputStream;
import org.h2.engine.SessionRemote;
import org.h2.message.DbException;
import org.h2.mvstore.DataUtils;

/**
 * An input stream used by the client side of a tcp connection to fetch LOB data
 * on demand from the server.
 */
public class LobStorageRemoteInputStream extends InputStream {//TIGER 看注释lob

    private final SessionRemote sessionRemote;

    /**
     * The lob id.
     */
    private final long lobId;

    private final byte[] hmac;

    /**
     * The position.
     */
    private long pos;

    public LobStorageRemoteInputStream(SessionRemote handler, long lobId, byte[] hmac) {
        this.sessionRemote = handler;
        this.lobId = lobId;
        this.hmac = hmac;
    }

    @Override
    public int read() throws IOException {//读入随机值吗？
        byte[] buff = new byte[1];
        int len = read(buff, 0, 1);
        return len < 0 ? len : (buff[0] & 255);
    }

    @Override
    public int read(byte[] buff) throws IOException {
        return read(buff, 0, buff.length);
    }

    @Override
    public int read(byte[] buff, int off, int length) throws IOException {//主函数
        assert(length >= 0);
        if (length == 0) {
            return 0;
        }
        try {
            length = sessionRemote.readLob(lobId, hmac, pos, buff, off, length);//代理读入
        } catch (DbException e) {
            throw DataUtils.convertToIOException(e);
        }
        if (length == 0) {
            return -1;
        }
        pos += length;
        return length;
    }

}
