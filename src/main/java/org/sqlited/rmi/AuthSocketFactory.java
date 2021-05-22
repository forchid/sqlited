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

package org.sqlited.rmi;

import org.sqlited.rmi.util.SocketUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.io.*;
import java.net.*;

import java.rmi.server.RMIClientSocketFactory;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * RMI socket factory with authentication.
 *
 * @threadsafe
 */
public class AuthSocketFactory implements RMIClientSocketFactory, Serializable {
    private static final long serialVersionUID = 1L;
    static final Logger log = LoggerFactory.getLogger(AuthSocketFactory.class);

    static final ThreadLocal<Properties> LOCAL_PROPS = new ThreadLocal<>();
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
        log.fine(() -> String.format("%s: use socketFactory#%d",
                Thread.currentThread().getName(), this.id));
        Properties local = LOCAL_PROPS.get();
        Properties props = SocketUtils.defaultConfig(local);
        return SocketUtils.createSocket(props, host, port);
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

    public static void attachProperties(Properties props) {
        LOCAL_PROPS.set(props);
    }

    public static void detachProperties() {
        LOCAL_PROPS.remove();
    }

}
