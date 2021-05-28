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

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class AuthServerSocketFactory extends ServerSocketFactory {

    private static final AtomicInteger ID = new AtomicInteger();

    protected final int id;
    protected final Properties props;

    public AuthServerSocketFactory(Properties props) {
        this.props = SocketUtils.defaultConfig(props);
        this.id = nextId();
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        return SocketUtils.createServerSocket(this.props, port);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog)
            throws IOException {
        Properties copy = new Properties();
        copy.putAll(this.props);
        copy.setProperty("backlog", backlog + "");
        return SocketUtils.createServerSocket(copy, port);
    }

    @Override
    public ServerSocket createServerSocket(int port, int backlog, InetAddress ifAddress)
            throws IOException {
        Properties copy = new Properties();
        copy.putAll(this.props);
        copy.setProperty("host", ifAddress.getHostAddress());
        return SocketUtils.createServerSocket(copy, port);
    }

    protected static int nextId() {
        return ID.incrementAndGet();
    }

}
