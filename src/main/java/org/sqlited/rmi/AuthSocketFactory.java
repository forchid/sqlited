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

import org.sqlited.net.SocketUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.io.*;
import java.net.*;

import java.rmi.server.RMIClientSocketFactory;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * RMI socket factory with authentication.
 *
 * @threadsafe
 */
public class AuthSocketFactory extends org.sqlited.net.AuthSocketFactory
        implements RMIClientSocketFactory, Serializable {

    private static final long serialVersionUID = 1L;

    static final Logger log = LoggerFactory.getLogger(AuthSocketFactory.class);
    static final ThreadLocal<Properties> LOCAL_PROPS = new ThreadLocal<>();

    public AuthSocketFactory() {
        super(null);
    }

    public AuthSocketFactory(Properties props) {
        super(props);
    }

    @Override
    public Socket createSocket(String host, int port) throws IOException {
        log.fine(() -> String.format("%s: use socketFactory#%d",
                Thread.currentThread().getName(), this.id));
        Properties local = LOCAL_PROPS.get();
        Properties props = SocketUtils.defaultConfig(local);
        return SocketUtils.createSocket(props, host, port);
    }

    public static void attachProperties(Properties props) {
        LOCAL_PROPS.set(props);
    }

    public static void detachProperties() {
        LOCAL_PROPS.remove();
    }

}
