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

import org.sqlited.jdbc.adapter.ConnectionAdapter;
import static org.sqlited.jdbc.rmi.util.RMIUtils.*;
import org.sqlited.rmi.RMIConnection;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.util.IOUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcRMIConnection extends ConnectionAdapter {

    protected final Properties props;
    protected final RMIConnection rmiConn;

    public JdbcRMIConnection(Properties props, RMIConnection rmiConn) {
        this.props = props;
        this.rmiConn = rmiConn;
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
        invoke(this.rmiConn::rollback, this.props);
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiConn);
    }

}
