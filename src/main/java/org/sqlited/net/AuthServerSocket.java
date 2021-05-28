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

import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class AuthServerSocket extends ServerSocket {

    private static final AtomicInteger ID = new AtomicInteger();

    protected final int id;
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
        Properties props = this.props;
        Socket socket = null;

        boolean failed = true;
        try {
            // Do accept
            if (this.isClosed()) {
                throw new SocketException("Socket is closed");
            } else if (!this.isBound()) {
                throw new SocketException("Socket is not bound yet");
            } else {
                socket = new AuthSocket(props, false);
                super.implAccept(socket);
            }

            String prop = props.getProperty("readTimeout");
            int soTimeout = Integer.decode(prop);
            prop = props.getProperty("tcpNoDelay");
            boolean tcpNoDelay = Boolean.parseBoolean(prop);
            socket.setSoTimeout(soTimeout);
            socket.setTcpNoDelay(tcpNoDelay);
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

}
