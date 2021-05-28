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

package org.sqlited.jdbc.rmi;

import org.sqlited.jdbc.adapter.DriverAdapter;
import org.sqlited.jdbc.rmi.impl.JdbcRMIConnection;
import org.sqlited.jdbc.rmi.util.RMIUtils;
import org.sqlited.rmi.AuthSocketFactory;
import org.sqlited.rmi.RMIConnection;
import org.sqlited.rmi.RMIDriver;
import org.sqlited.util.IOUtils;
import org.sqlited.util.LruCache;
import org.sqlited.util.logging.LoggerFactory;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcRMIDriver extends DriverAdapter {

    static final Logger log = LoggerFactory.getLogger(JdbcRMIDriver.class);
    public static final String PREFIX = DriverAdapter.PREFIX + "rmi:";

    final LruCache<RMIClientSocketFactory, Registry> regs = new LruCache<>(250);

    @Override
    protected Connection connect(String url, Properties info, Properties connProps)
            throws SQLException {
        String host = connProps.getProperty("host");
        int port = Integer.decode(connProps.getProperty("port"));

        int retries = 0;
        while (true) {
            RMIClientSocketFactory socketFactory = new AuthSocketFactory(connProps);
            Registry registry = null;
            boolean regCached = true;
            try {
                AuthSocketFactory.attachProperties(connProps);
                log.fine(() -> String.format("%s: locate remote registry",
                        Thread.currentThread().getName()));
                synchronized (this.regs) {
                    registry = this.regs.get(socketFactory);
                }
                if (registry == null) {
                    registry = LocateRegistry.getRegistry(host, port, socketFactory);
                    regCached = false;
                }
                log.fine(() -> String.format("%s: lookup remote driver",
                        Thread.currentThread().getName()));
                RMIDriver rmiDriver = (RMIDriver) registry.lookup("SQLited");
                log.fine(() -> String.format("%s: get a remote connection",
                        Thread.currentThread().getName()));
                RMIConnection rmiConn = rmiDriver.connect(url, info);
                boolean failed = true;
                try {
                    Connection c = new JdbcRMIConnection(connProps, rmiConn);
                    failed = false;
                    return c;
                } finally {
                    if (failed) IOUtils.close(rmiConn);
                }
            } catch (NoSuchObjectException e) {
                if (++retries > 2) {
                    throw RMIUtils.wrap(e);
                }
                // Do retry for obsolete reference
                if (registry != null && regCached) {
                    synchronized (this.regs) {
                        this.regs.remove(socketFactory);
                    }
                }
            } catch (NotBoundException | RemoteException e) {
                throw RMIUtils.wrap(e);
            } finally {
                AuthSocketFactory.detachProperties();
            }
        }
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

    @Override
    protected int getPort() {
        return 3515;
    }

}
