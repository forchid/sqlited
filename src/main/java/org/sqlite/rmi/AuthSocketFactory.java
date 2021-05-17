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

package org.sqlite.rmi;

import org.sqlite.server.util.IoUtils;
import org.sqlite.util.MDUtils;
import org.sqlite.util.logging.LoggerFactory;

import java.io.*;
import java.net.*;

import static java.nio.charset.StandardCharsets.*;
import java.rmi.server.RMISocketFactory;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public class AuthSocketFactory extends RMISocketFactory {
    static final Logger log = LoggerFactory.getLogger(AuthSocketFactory.class);

    static final Map<String, Byte> METHODS = new HashMap<>();
    static {
        METHODS.put("md5", (byte)0x01);
    }

    private final Properties props;

    public AuthSocketFactory(Properties props) throws IllegalArgumentException {
        if (props.getProperty("user") == null) {
            throw new IllegalArgumentException("No user property");
        }
        // Current only support MD5
        String value = props.getProperty("method");
        if (value != null && !"md5".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("Unknown method '" + value + "'");
        }
        this.props = defaultConfig(props);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return new AuthSocket(this.props, host, port);
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        String host = this.props.getProperty("host");
        String backlog = this.props.getProperty("backlog", "150");
        SocketAddress endpoint = new InetSocketAddress(host, port);
        ServerSocket server = new AuthServerSocket(this.props);
        boolean failed = true;
        try {
            log.info(() -> String.format("Server bind %s:%d", host, port));
            server.bind(endpoint, Integer.decode(backlog));
            failed = false;
            return server;
        } finally {
            if (failed) IoUtils.close(server);
        }
    }

    static Properties defaultConfig(Properties props) {
        setIfAbsent(props, "host", "localhost");
        setIfAbsent(props, "method", "md5");
        setIfAbsent(props, "loginTimeout", "15000");
        return props;
    }

    static void setIfAbsent(Properties props, String name, String value) {
        if (props.getProperty(name) == null) {
            props.setProperty(name, value);
        }
    }

    static class AuthServerSocket extends ServerSocket {

        protected final SecureRandom secureRandom = new SecureRandom();
        protected final Properties props;

        public AuthServerSocket(Properties props) throws IOException {
            this.props = props;
        }

        @Override
        public Socket accept() throws IOException {
            Socket socket = super.accept();
            boolean failed = true;
            try {
                handshake(this.props, socket, this.secureRandom);
                failed = false;
                return socket;
            } finally {
                if (failed) IoUtils.close(socket);
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

    static class AuthSocket extends Socket {

        public AuthSocket(Properties props, String host, int port) throws IOException {
            super(host, port);

            boolean failed = true;
            try {
                login(props, this);
                failed = false;
            } finally {
                if (failed) IoUtils.close(this);
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
            for (Map.Entry<String, Byte> i: METHODS.entrySet()) {
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

}
