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

import java.io.*;
import java.net.*;

import java.rmi.server.RMIClientSocketFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RMI socket factory with authentication.
 *
 * @threadsafe
 */
public class AuthSocketFactory implements RMIClientSocketFactory, Serializable {

    private static final long serialVersionUID = 1L;
    private static final AtomicLong ID = new AtomicLong();

    protected final long id;

    public AuthSocketFactory() {
        this.id = nextId();
    }

    protected static long nextId() {
        return ID.incrementAndGet();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return SocketUtils.createSocket(host, port);
    }

    @Override
    public int hashCode() {
        return (int)this.id;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AuthSocketFactory) {
            AuthSocketFactory f = (AuthSocketFactory)o;
            return f.id == this.id;
        } else {
            return false;
        }
    }

}