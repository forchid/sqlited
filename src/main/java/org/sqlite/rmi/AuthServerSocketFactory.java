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

import org.sqlite.rmi.util.SocketUtils;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RMI serverSocket factory with authentication.
 *
 * @threadsafe
 */
public class AuthServerSocketFactory implements RMIServerSocketFactory {

    private static final AtomicInteger ID = new AtomicInteger();

    protected final int id;
    private final Properties props;

    public AuthServerSocketFactory(Properties props) {
        this.props = SocketUtils.defaultConfig(props);
        this.id = nextId();
    }

    protected static int nextId() {
        return ID.incrementAndGet();
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        return SocketUtils.createServerSocket(this.props, port);
    }

    @Override
    public int hashCode() {
        return this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AuthServerSocketFactory) {
            AuthServerSocketFactory f = (AuthServerSocketFactory)o;
            return f.id == this.id;
        } else {
            return false;
        }
    }

}
