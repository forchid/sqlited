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
import org.sqlite.util.IOUtils;
import org.sqlite.util.logging.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * RMI serverSocket factory with authentication.
 *
 * @threadsafe
 */
public class AuthServerSocketFactory implements RMIServerSocketFactory, AutoCloseable {

    static final Logger log = LoggerFactory.getLogger(AuthServerSocketFactory.class);
    private static final AtomicInteger ID = new AtomicInteger();

    protected final int id;
    private final Properties props;
    private volatile ServerSocket serverSocket;

    public AuthServerSocketFactory(Properties props) {
        this.props = SocketUtils.defaultConfig(props);
        this.id = nextId();
    }

    protected static int nextId() {
        return ID.incrementAndGet();
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        Properties props = this.props;
        ServerSocket s = SocketUtils.createServerSocket(props, port);
        return (this.serverSocket = s);
    }

    public boolean isClosed() {
        ServerSocket s = this.serverSocket;
        return (s != null && s.isClosed());
    }

    @Override
    public void close() {
        ServerSocket s = this.serverSocket;
        log.info(() -> String.format("%s: close server socketFactory#%d",
                Thread.currentThread().getName(), this.id));
        while (s != null && !isClosed()) {
            IOUtils.close(s);
        }
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
