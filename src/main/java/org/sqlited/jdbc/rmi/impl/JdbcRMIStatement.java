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

import org.sqlited.jdbc.adapter.StatementAdapter;
import org.sqlited.rmi.RMIResultSet;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.util.IOUtils;

import static org.sqlited.jdbc.rmi.util.RMIUtils.*;

import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcRMIStatement extends StatementAdapter {

    protected final JdbcRMIConnection conn;
    protected final RMIStatement rmiStmt;

    public JdbcRMIStatement(JdbcRMIConnection conn, RMIStatement rmiStmt) {
        this.conn = conn;
        this.rmiStmt = rmiStmt;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        return invoke(() -> {
            RMIResultSet rs = null;
            boolean failed = true;
            try {
                rs = this.rmiStmt.executeQuery(sql);
                ResultSet r = new JdbcRMIResultSet(this.conn, rs);
                failed = false;
                return r;
            } finally {
                if (failed) IOUtils.close(rs);
            }
        }, this.conn.props);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return invoke(() -> this.rmiStmt.executeUpdate(sql), this.conn.props);
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiStmt);
    }

}
