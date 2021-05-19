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

package org.sqlite.rmi.util;

import org.sqlite.util.IOUtils;
import org.sqlite.util.logging.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

public final class SocketUtils {
    static final Logger log = LoggerFactory.getLogger(SocketUtils.class);

    static final ThreadLocal<Properties> PROPS = new ThreadLocal<>();
    static final Map<String, Byte> METHODS = new HashMap<String, Byte>() {
        { put("md5", (byte)0x01); }
    };

    private SocketUtils() {}

    public static void attachProperties(Properties props) {
        PROPS.set(props);
    }

    public static void detachProperties() {
        PROPS.remove();
    }

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

    public static ServerSocket createServerSocket(Properties props, int port) throws IOException {
        String host = props.getProperty("host");
        SocketAddress endpoint = new InetSocketAddress(host, port);
        ServerSocket server = new AuthServerSocket(props);

        boolean failed = true;
        try {
            log.info(() -> String.format("Server bind %s:%d", host, port));
            String backlog = props.getProperty("backlog", "150");
            server.bind(endpoint, Integer.decode(backlog));
            failed = false;
            return server;
        } finally {
            if (failed) IOUtils.close(server);
        }
    }

    public static Socket createSocket(String host, int port) throws IOException {
        final Properties props = PROPS.get();
        log.fine(() -> String.format("%s: connection to %s:%d",
                Thread.currentThread().getName(), host, port));

        return new AuthSocket(defaultConfig(props), host, port);
    }

}
