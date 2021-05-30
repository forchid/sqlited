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
import java.sql.*;
import java.util.Properties;

public class JdbcRMIConnection extends ConnectionAdapter {

    protected final Properties props;
    protected final RMIConnection rmiConn;
    protected int status;

    public JdbcRMIConnection(Properties props, RMIConnection rmiConn)
            throws SQLException {
        this.props = props;
        this.rmiConn = rmiConn;
        this.status = invoke(this.rmiConn::getStatus, this.props);
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
    public int getTransactionIsolation() throws SQLException {
        return (this.status & 0x3C) >>> 2;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkIsolation(level);
        invoke(() -> this.rmiConn.setTransactionIsolation(level), this.props);
        this.status = (this.status & ~0x3C) | (level << 2);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return (this.status & 0x1) != 0x0;
    }

    @Override
    public void setReadOnly(boolean readonly) throws SQLException {
        invoke(() -> this.rmiConn.setReadOnly(readonly), this.props);
        this.status = (this.status & ~0x1) | (readonly? 0x1: 0x0);
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
    public void setHoldability(int holdability) throws SQLException {
        checkHoldability(holdability);
        invoke(() -> this.rmiConn.setHoldability(holdability), this.props);
        this.status = (this.status & ~0xC0) | (holdability << 6);
    }

    @Override
    public int getHoldability() throws SQLException {
        int status = this.status;

        if ((status & 0x40) != 0x0) {
            return ResultSet.HOLD_CURSORS_OVER_COMMIT;
        } else if ((status & 0x80) != 0x0) {
            return ResultSet.CLOSE_CURSORS_AT_COMMIT;
        } else {
            throw new SQLException("Unknown holdability");
        }
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiConn);
    }

}
