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

import static org.sqlited.rmi.util.SocketUtils.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AuthServerSocket extends ServerSocket {

    static final Logger log = LoggerFactory.getLogger(AuthServerSocket.class);
    private static final AtomicInteger ID = new AtomicInteger();

    protected final int id;
    protected final SecureRandom secureRandom = new SecureRandom();
    protected final Properties props;

    public AuthServerSocket(Properties props) throws IOException {
        this.props = props;
        this.id = nextId();
    }

    protected static int nextId() {
        return ID.incrementAndGet();
    }

    @Override
    public Socket accept() throws IOException {
        Socket socket = super.accept();
        boolean failed = true;
        try {
            String readTimeout = this.props.getProperty("readTimeout");
            int soTimeout = Integer.decode(readTimeout);
            socket.setSoTimeout(soTimeout);
            handshake(this.props, socket, this.secureRandom);
            failed = false;
            return socket;
        } finally {
            if (failed) IOUtils.close(socket);
        }
    }

    @Override
    public int hashCode() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AuthServerSocket) {
            AuthServerSocket s = (AuthServerSocket)o;
            return s.id == this.id;
        } else {
            return false;
        }
    }

    static void handshake(Properties props, Socket socket, SecureRandom secureRandom)
            throws IOException {
        final int soTimeout = socket.getSoTimeout();

        String info = props.getProperty("server", "SQLited");
        String method = props.getProperty("method");
        final byte mCode = METHODS.get(method);
        OutputStream os = socket.getOutputStream();
        DataOutputStream out = new DataOutputStream(os);
        final byte[] challenge = new byte[8];
        secureRandom.nextBytes(challenge);

        // Send handshake packet
        out.writeByte(0x01); // protocol version
        out.writeUTF(info);
        out.writeByte(mCode);
        out.write(challenge);
        out.flush();

        // Receive login packet
        String loginTimeout = props.getProperty("loginTimeout");
        socket.setSoTimeout(Integer.decode(loginTimeout));
        InputStream is = socket.getInputStream();
        DataInputStream in = new DataInputStream(is);
        byte[] authData = new byte[16];
        String client = in.readUTF();
        log.fine(() -> String.format("%s login", client));
        String loginUser = in.readUTF();
        in.readFully(authData);

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
        if (!user.equals(loginUser) || !Arrays.equals(digest, authData)) {
            String f = "Auth failed from %s@%s";
            String s = String.format(f, loginUser, remote.getHostName());
            log.warning(s);
            throw new IOException(s);
        }
        log.fine(() -> String.format("%s@%s login OK", user, remote.getHostName()));

        socket.setSoTimeout(soTimeout);
    }

}
