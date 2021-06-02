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

package org.sqlited.net;

import org.sqlited.io.Transfer;
import org.sqlited.util.IOUtils;
import org.sqlited.util.MDUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.sqlited.net.SocketUtils.METHODS;

public class AuthSocket extends Socket {

    static final Logger log = LoggerFactory.getLogger(AuthSocket.class);
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final AtomicLong ID = new AtomicLong();

    protected final long id;
    protected final Properties props;
    protected final boolean client;
    private boolean handshaked;

    public AuthSocket(Properties props, boolean client) {
        this.props = props;
        this.client = client;
        this.id = nextId();
        log.fine(() -> String.format("%s: Create socket#%d",
                Thread.currentThread().getName(), this.id));
    }

    public long getId() {
        return this.id;
    }

    public Properties getProps() {
        return this.props;
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

    @Override
    public InputStream getInputStream() throws IOException {
        if (!this.client && !this.handshaked) handshake();
        return super.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (!this.client && !this.handshaked) handshake();
        return super.getOutputStream();
    }

    protected void handshake() throws IOException {
        this.handshaked = true;
        handshake(this.props, this);
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

    static void handshake(Properties props, Socket socket)
            throws IOException {
        final int soTimeout = socket.getSoTimeout();

        String info = props.getProperty("server", "SQLited");
        String method = props.getProperty("method");
        final byte mCode = METHODS.get(method);
        String maxBuffer = props.getProperty("maxBufferSize");
        Transfer ch = new Transfer(socket, Integer.decode(maxBuffer));
        final byte[] challenge = new byte[8];
        RANDOM.nextBytes(challenge);

        // Send handshake packet
        final int serverVersion = Transfer.VERSION; // protocol version
        ch.writeByte(serverVersion)
                .writeByte(mCode)
                .write(challenge)
                .writeString(info)
                .flush();

        // Receive login packet
        // -Format: version, auth-method, auth-data, user or null, client-info
        String loginTimeout = props.getProperty("loginTimeout");
        socket.setSoTimeout(Integer.decode(loginTimeout));
        final int clientVersion = ch.readByte(true); // protocol version
        if (serverVersion != clientVersion) {
            String s = "Protocol version error";
            ch.writeError(s);
            throw new IOException(s);
        }
        final int clientMethod = ch.readByte(true);
        if (clientMethod != mCode) {
            String s = "Unsupported client auth method: " + clientMethod;
            ch.writeError(s);
            throw new IOException(s);
        }
        final byte[] authData = ch.readFully(16);
        int maxLength = Integer.decode(props.getProperty("userMaxLength"));
        final String loginUser = ch.readString(maxLength);
        // Do auth
        InetSocketAddress remote = (InetSocketAddress)socket.getRemoteSocketAddress();
        MessageDigest md5 = MDUtils.md5();
        md5.update(challenge);
        String password = props.getProperty("password");
        if (password != null) {
            byte[] data = password.getBytes(UTF_8);
            md5.update(data);
        }
        byte[] digest = md5.digest();
        String user = props.getProperty("user");
        if (user.equals(loginUser) && Arrays.equals(digest, authData)) {
            String client = ch.readString();
            log.fine(() -> String.format("%s login", client));
            ch.writeOK(0);
        } else {
            String f = "Access denied for %s@%s";
            String s = String.format(f, loginUser, remote.getHostName());
            ch.writeError(s);
            throw new IOException(s);
        }
        log.fine(() -> String.format("%s@%s login OK", user, remote.getHostName()));

        socket.setSoTimeout(soTimeout);
    }

    static void login(Properties props, Socket socket) throws IOException {
        int soTimeout = socket.getSoTimeout();

        String s = props.getProperty("loginTimeout");
        socket.setSoTimeout(Integer.decode(s));
        s = props.getProperty("maxBufferSize");
        Transfer ch = new Transfer(socket, Integer.decode(s));

        // Handshake
        final int clientVersion = Transfer.VERSION;
        int serverVersion = ch.readByte(true);
        if (serverVersion != clientVersion) {
            s = "Unknown server protocol " + serverVersion;
            throw new IOException(s);
        }
        int mCode = ch.readByte(true);
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
        byte[] challenge = ch.readFully(8);
        String server = ch.readString();
        log.fine(() -> String.format("%s handshake", server));

        // Do login
        String client = props.getProperty("client", "SQLited-jdbc");
        MessageDigest md5 = MDUtils.md5();
        md5.update(challenge);
        String password = props.getProperty("password");
        if (password != null) {
            byte[] data = password.getBytes(UTF_8);
            md5.update(data);
        }
        byte[] authData = md5.digest();
        String user = props.getProperty("user");
        ch.writeByte(clientVersion)
                .writeByte(mCode)
                .write(authData)
                .writeString(user)
                .writeString(client)
                .flush();
        // - response
        int result = ch.readByte(true);
        if (Transfer.RESULT_ER == result) {
            s = ch.readString();
            ch.readString();
            ch.readInt();
            throw new IOException(s);
        }
        int status = ch.readInt();
        ch.readLong();
        ch.readLong();
        props.setProperty("status", status + "");

        socket.setSoTimeout(soTimeout);
    }

}
