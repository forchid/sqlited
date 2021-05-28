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

import org.sqlited.jdbc.JdbcResultSet;
import org.sqlited.result.RowIterator;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.jdbc.rmi.util.RMIUtils;

import java.sql.SQLException;

public class JdbcRMIResultSet extends JdbcResultSet {

    public JdbcRMIResultSet(JdbcRMIConnection conn, JdbcRMIStatement stmt,
                            RowIterator rowItr) {
        super(conn, stmt, rowItr);
    }

    protected RMIStatement rmiStmt() throws SQLException {
        return getStatement().rmiStmt;
    }

    @Override
    protected JdbcRMIConnection getConnection() {
        return (JdbcRMIConnection)this.conn;
    }

    @Override
    public JdbcRMIStatement getStatement() throws SQLException {
        return (JdbcRMIStatement)this.stmt;
    }

    @Override
    protected RowIterator fetchRows(boolean meta) throws SQLException {
        JdbcRMIConnection conn = getConnection();
        return RMIUtils.invoke(() -> rmiStmt().next(meta), conn.props);
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        JdbcRMIConnection conn = getConnection();
        return RMIUtils.invoke(rmiStmt()::getFetchSize, conn.props);
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        checkOpen();
        JdbcRMIConnection conn = getConnection();
        RMIUtils.invoke(() -> rmiStmt().setFetchSize(fetchSize), conn.props);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        JdbcRMIConnection conn = getConnection();
        return RMIUtils.invoke(rmiStmt()::getFetchDirection, conn.props);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        JdbcRMIConnection conn = getConnection();
        RMIUtils.invoke(() -> rmiStmt().setFetchDirection(direction), conn.props);
    }

}
