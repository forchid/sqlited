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

import org.sqlited.util.IOUtils;
import org.sqlited.util.PropsUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public final class SocketUtils {
    static final Logger log = LoggerFactory.getLogger(SocketUtils.class);

    static final Map<String, Byte> METHODS = new HashMap<String, Byte>() {
        { put("md5", (byte)0x01); }
    };

    private SocketUtils() {}

    public static Properties defaultConfig(Properties props)
            throws IllegalArgumentException {

        Properties copy = new Properties();
        copy.putAll(props);
        props = copy;

        if (props.getProperty("user") == null) {
            throw new IllegalArgumentException("No user property");
        }
        // Current only support MD5
        String value = props.getProperty("method");
        if (value != null && !"md5".equalsIgnoreCase(value)) {
            throw new IllegalArgumentException("Unknown auth method '" + value + "'");
        }
        PropsUtils.setIfAbsent(props, "host", "localhost");
        PropsUtils.setIfAbsent(props, "method", "md5");
        PropsUtils.setIfAbsent(props, "loginTimeout", "15000");
        PropsUtils.setIfAbsent(props, "connectTimeout", "5000");
        PropsUtils.setIfAbsent(props, "readTimeout", "60000");
        PropsUtils.setIfAbsent(props, "tcpNoDelay", "true");
        PropsUtils.setIfAbsent(props, "bufferSize", "4096");

        return props;
    }

    public static ServerSocket createServerSocket(Properties props, int port)
            throws IOException {
        String host = props.getProperty("host");
        SocketAddress endpoint = new InetSocketAddress(host, port);
        ServerSocket server = new AuthServerSocket(props);

        boolean failed = true;
        try {
            log.info(() -> String.format("Server bind %s:%d", host, port));
            String s = props.getProperty("backlog", "150");
            int backlog = Integer.decode(s);
            long start = System.currentTimeMillis();

            server.setReuseAddress(true);
            while (true) {
                try {
                    server.bind(endpoint, backlog);
                    break;
                } catch (BindException e) {
                    long cur = System.currentTimeMillis();
                    if ( cur - start > 10000) {
                        throw e;
                    }
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException cause) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                }
            }

            failed = false;
            return server;
        } finally {
            if (failed) IOUtils.close(server);
        }
    }

    public static Socket createSocket(Properties props, String host, int port)
            throws IOException {
        SocketAddress endpoint = new InetSocketAddress(host, port);
        return createSocket(props, endpoint, null);
    }

    public static Socket createSocket(Properties props, String host, int port,
                                      InetAddress localAddress, int localPort)
            throws IOException {
        SocketAddress endpoint = new InetSocketAddress(host, port);
        SocketAddress local = new InetSocketAddress(localAddress, localPort);
        return createSocket(props, endpoint, local);
    }

    public static Socket createSocket(Properties props, InetAddress host, int port)
            throws IOException {
        SocketAddress endpoint = new InetSocketAddress(host, port);
        return createSocket(props, endpoint, null);
    }

    public static Socket createSocket(Properties props, InetAddress host, int port,
                                      InetAddress localAddress, int localPort)
            throws IOException {
        SocketAddress endpoint = new InetSocketAddress(host, port);
        SocketAddress local = new InetSocketAddress(localAddress, localPort);
        return createSocket(props, endpoint, local);
    }

    public static Socket createSocket(Properties props, SocketAddress remote,
                                      SocketAddress local)
            throws IOException {
        log.fine(() -> String.format("%s: connection to %s",
                Thread.currentThread().getName(), remote));

        String prop = props.getProperty("connectTimeout");
        int timeout = Integer.decode(prop);
        prop = props.getProperty("tcpNoDelay");
        boolean tcpNoDelay = Boolean.parseBoolean(prop);
        Socket socket = new AuthSocket(props, true);

        boolean failed = true;
        try {
            socket.setTcpNoDelay(tcpNoDelay);
            if (local != null) socket.bind(local);
            socket.connect(remote, timeout);
            failed = false;
            return socket;
        } finally {
            if (failed) IOUtils.close(socket);
        }
    }

}
