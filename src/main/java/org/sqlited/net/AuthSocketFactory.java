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

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class AuthSocketFactory extends SocketFactory {

    private static final AtomicLong ID = new AtomicLong();

    protected final Properties props;
    protected final long id;

    public AuthSocketFactory() {
        this(null);
    }

    public AuthSocketFactory(Properties props) {
        this.props = props;
        this.id = nextId();
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        return SocketUtils.createSocket(this.props, host, port);
    }

    @Override
    public Socket createSocket(String host, int port,
                               InetAddress localAddress, int localPort)
            throws IOException {
        return SocketUtils.createSocket(this.props, host, port, localAddress, localPort);
    }

    @Override
    public Socket createSocket(InetAddress host, int port)
            throws IOException {
        return SocketUtils.createSocket(this.props, host, port);
    }

    @Override
    public Socket createSocket(InetAddress host, int port,
                               InetAddress localAddress, int localPort)
            throws IOException {
        return SocketUtils.createSocket(this.props, host, port, localAddress, localPort);
    }

    @Override
    public int hashCode() {
        Properties props = this.props;

        if (props == null) {
            return (int)this.id;
        } else {
            String url = props.getProperty("url");
            String user = props.getProperty("user");
            String password = props.getProperty("password");
            return (url.hashCode() ^ user.hashCode()
                    ^ (password == null? 37: password.hashCode()));
        }
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AuthSocketFactory) {
            AuthSocketFactory f = (AuthSocketFactory) o;
            Properties props = this.props;
            if (props == null || f.props == null) {
                return f.id == this.id;
            } else {
                String url = props.getProperty("url");
                String user = props.getProperty("user");
                String password = props.getProperty("password");
                String oUrl = f.props.getProperty("url");
                String oUser = f.props.getProperty("user");
                String oPassword = f.props.getProperty("password");
                return (url.equals(oUrl) && user.equals(oUser)
                        && (Objects.equals(password, oPassword)));
            }
        } else {
            return false;
        }
    }

    protected static long nextId() {
        return ID.incrementAndGet();
    }

}
