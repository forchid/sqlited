/*
 * Copyright (c) 2021 little-pan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sqlited.rmi.util;

import org.sqlited.util.IOUtils;
import org.sqlited.util.MDUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.io.*;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AuthSocket extends Socket {

    static final Logger log = LoggerFactory.getLogger(AuthSocket.class);
    private static final AtomicLong ID = new AtomicLong();

    protected final long id;
    protected final Properties props;

    public AuthSocket(Properties props) {
        this.props = props;
        this.id = nextId();
        log.fine(() -> String.format("%s: Create socket#%d",
                Thread.currentThread().getName(), this.id));
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        this.connect(endpoint, 0);
    }

    @Override
    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        String readTimeout;
        boolean failed = true;
        try {
            readTimeout = this.props.getProperty("readTimeout");
            int soTimeout = Integer.decode(readTimeout);
            setSoTimeout(soTimeout);
            super.connect(endpoint, timeout);
            login(this.props, this);
            failed = false;
        } finally {
            if (failed) IOUtils.close(this);
        }
    }

    protected static long nextId() {
        return ID.incrementAndGet();
    }

    @Override
    public void close() throws IOException {
        super.close();
        log.fine(() -> String.format("%s: Close socket#%d",
                Thread.currentThread().getName(), this.id));
    }

    @Override
    public int hashCode() {
        return (int)this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AuthSocket) {
            AuthSocket s = (AuthSocket)o;
            return s.id == this.id;
        } else {
            return false;
        }
    }

    static void login(Properties props, Socket socket) throws IOException {
        int soTimeout = socket.getSoTimeout();

        String loginTimeout = props.getProperty("loginTimeout");
        socket.setSoTimeout(Integer.decode(loginTimeout));
        InputStream is = socket.getInputStream();
        DataInputStream in = new DataInputStream(is);

        byte protocol = in.readByte();
        if (protocol != 0x01) {
            throw new IOException("Unknown server protocol version " + protocol);
        }
        String server = in.readUTF();
        log.fine(() -> String.format("%s handshake", server));
        byte mCode = in.readByte();
        boolean mFound = false;
        for (Map.Entry<String, Byte> i: SocketUtils.METHODS.entrySet()) {
            if (mCode == i.getValue()) {
                mFound = true;
                break;
            }
        }
        if (!mFound) {
            throw new IOException("Unknown server auth method " + mCode);
        }
        byte[] challenge = new byte[8];
        in.readFully(challenge);

        // Do login
        String client = props.getProperty("client", "SQLited-jdbc");
        OutputStream os = socket.getOutputStream();
        DataOutputStream out = new DataOutputStream(os);
        MessageDigest md5 = MDUtils.md5();
        md5.update(challenge);
        String password = props.getProperty("password");
        if (password != null) {
            byte[] data = password.getBytes(UTF_8);
            md5.update(data);
        }
        byte[] authData = md5.digest();
        out.writeUTF(client);
        out.writeUTF(props.getProperty("user"));
        out.write(authData);
        out.flush();

        socket.setSoTimeout(soTimeout);
    }
}
