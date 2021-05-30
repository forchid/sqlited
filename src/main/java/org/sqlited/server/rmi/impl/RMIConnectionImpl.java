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

import org.sqlited.jdbc.JdbcSavepoint;
import org.sqlited.rmi.RMIConnection;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.server.Config;
import static org.sqlited.server.util.SQLiteUtils.*;

import org.sqlited.server.util.SQLiteUtils;
import org.sqlited.util.IOUtils;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

public class RMIConnectionImpl extends UnicastRemoteObject implements RMIConnection {

    protected final RMIClientSocketFactory clientSocketFactory;
    protected final RMIServerSocketFactory serverSocketFactory;
    protected final Config config;
    protected final Connection sqlConn;
    private boolean readonly;
    private Statement auxStmt;

    protected RMIConnectionImpl(String url, Properties info, Config config,
                                RMIClientSocketFactory clientSocketFactory,
                                RMIServerSocketFactory serverSocketFactory)
            throws RemoteException, SQLException {
        super(config.getPort(), clientSocketFactory, serverSocketFactory);
        this.sqlConn = open(url, info);
        this.config  = config;
        this.clientSocketFactory = clientSocketFactory;
        this.serverSocketFactory = serverSocketFactory;
        init();
    }

    protected void init() throws SQLException {
        boolean failed = true;
        try {
            Statement stmt = getAuxStmt();
            this.readonly = queryOnly(this.sqlConn, stmt);
            failed = false;
        } finally {
            if (failed) IOUtils.close(this.sqlConn);
        }
    }

    @Override
    public int getStatus() throws RemoteException, SQLException {
        return SQLiteUtils.getStatus(this.sqlConn, this.readonly);
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
    public boolean getAutoCommit() throws RemoteException, SQLException {
        return this.sqlConn.getAutoCommit();
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws RemoteException, SQLException {
        this.sqlConn.setAutoCommit(autoCommit);
    }

    @Override
    public void commit() throws RemoteException, SQLException {
        this.sqlConn.commit();
    }

    @Override
    public void rollback() throws RemoteException, SQLException {
        this.sqlConn.rollback();
    }

    @Override
    public Savepoint setSavepoint(String name) throws RemoteException, SQLException {
        Savepoint sp;
        if (name == null) sp = this.sqlConn.setSavepoint();
        else sp = this.sqlConn.setSavepoint(name);
        return new JdbcSavepoint(sp);
    }

    @Override
    public void rollback(Savepoint savepoint) throws RemoteException, SQLException {
        this.sqlConn.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws RemoteException, SQLException {
        this.sqlConn.releaseSavepoint(savepoint);
    }

    @Override
    public boolean isReadonly() throws RemoteException, SQLException {
        return this.readonly;
    }

    @Override
    public void setReadOnly(boolean readonly) throws RemoteException, SQLException {
        if (this.readonly != readonly) {
            Statement stmt = getAuxStmt();
            setQueryOnly(stmt, readonly);
            if (readonly == queryOnly(this.sqlConn, stmt)) {
                this.readonly = readonly;
            } else {
                throw new SQLException("Set readonly failure");
            }
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws RemoteException, SQLException {
        this.sqlConn.setTransactionIsolation(level);
    }

    @Override
    public void setHoldability(int holdability) throws RemoteException, SQLException {
        this.sqlConn.setHoldability(holdability);
    }

    @Override
    public void close() throws RemoteException {
        IOUtils.close(this.sqlConn);
    }

    protected Statement getAuxStmt() throws SQLException {
        Statement stmt = this.auxStmt;
        if (stmt == null) {
            return (this.auxStmt = this.sqlConn.createStatement());
        } else {
            return stmt;
        }
    }

}
