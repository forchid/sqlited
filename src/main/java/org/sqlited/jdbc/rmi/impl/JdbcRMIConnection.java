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

package org.sqlited.jdbc.rmi.impl;

import org.sqlited.jdbc.JdbcSavepoint;
import org.sqlited.jdbc.adapter.ConnectionAdapter;
import static org.sqlited.jdbc.rmi.util.RMIUtils.*;

import org.sqlited.jdbc.rmi.util.VoidMethod;
import org.sqlited.rmi.RMIConnection;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.util.IOUtils;

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

public class JdbcRMIConnection extends ConnectionAdapter {

    protected final Properties props;
    protected final RMIConnection rmiConn;
    private boolean readonly;

    public JdbcRMIConnection(Properties props, RMIConnection rmiConn)
            throws SQLException {
        this.props = props;
        this.rmiConn = rmiConn;
        this.readonly = invoke(this.rmiConn::isReadonly, this.props);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return invoke(() -> {
            RMIStatement stmt = null;
            boolean failed = true;
            try {
                stmt = this.rmiConn.createStatement();
                Statement s = new JdbcRMIStatement(this, stmt);
                failed = false;
                return s;
            } finally {
                if (failed) IOUtils.close(stmt);
            }
        }, this.props);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return invoke(() -> this.rmiConn.setSavepoint(name), this.props);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        Savepoint sp = wrapSavepoint(savepoint);
        invoke(() -> this.rmiConn.rollback(sp), this.props);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        Savepoint sp = wrapSavepoint(savepoint);
        invoke(() -> this.rmiConn.releaseSavepoint(sp), this.props);
    }

    protected Savepoint wrapSavepoint(Savepoint savepoint) throws SQLException {
        Savepoint sp;
        if (savepoint instanceof Serializable) sp = savepoint;
        else sp = new JdbcSavepoint(savepoint);
        return sp;
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return this.readonly;
    }

    @Override
    public void setReadOnly(boolean readonly) throws SQLException {
        invoke(() -> this.rmiConn.setReadOnly(readonly), this.props);
        this.readonly = readonly;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return invoke(this.rmiConn::getAutoCommit, this.props);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        invoke(() -> this.rmiConn.setAutoCommit(autoCommit), this.props);
    }

    @Override
    public void commit() throws SQLException {
        invoke(this.rmiConn::commit, this.props);
    }

    @Override
    public void rollback() throws SQLException {
        invoke((VoidMethod) this.rmiConn::rollback, this.props);
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiConn);
    }

}
