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

package org.sqlited.server.rmi.impl;

import org.sqlite.JDBC;
import org.sqlited.rmi.RMIConnection;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.server.Config;
import org.sqlited.util.IOUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

public class RMIConnectionImpl extends UnicastRemoteObject implements RMIConnection {

    static final Logger log = LoggerFactory.getLogger(RMIConnectionImpl.class);

    protected final Config config;
    protected final Connection sqlConn;

    protected RMIConnectionImpl(Config config, String url, Properties info)
            throws RemoteException, SQLException {
        super(config.getPort(), config.getRMIClientSocketFactory(),
                config.getRMIServerSocketFactory());
        log.fine(() -> String.format("Open DB '%s'", url));
        this.sqlConn = JDBC.createConnection(url, info);
        this.config  = config;
    }

    @Override
    public RMIStatement createStatement() throws RemoteException, SQLException {
        Statement stmt = this.sqlConn.createStatement();
        boolean failed = true;
        try {
            RMIStatement s = new RMIStatementImpl(this, stmt);
            failed = false;
            return s;
        } finally {
            if (failed) IOUtils.close(stmt);
        }
    }

    @Override
    public void close() throws RemoteException {
        IOUtils.close(this.sqlConn);
    }

}
