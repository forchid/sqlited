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

package org.sqlited.jdbc.tcp.impl;

import org.sqlited.io.Transfer;
import org.sqlited.jdbc.JdbcResultSet;
import org.sqlited.result.ResultSetMetaData;
import org.sqlited.result.RowIterator;

import java.io.IOException;
import java.sql.SQLException;

public class JdbcTcpResultSet extends JdbcResultSet {

    protected JdbcTcpResultSet(JdbcTcpConnection conn, JdbcTcpStatement stmt,
                               RowIterator rowItr) {
        super(conn, stmt, rowItr);
    }

    @Override
    protected JdbcTcpConnection getConnection() {
        return (JdbcTcpConnection)this.conn;
    }

    @Override
    public JdbcTcpStatement getStatement() {
        return (JdbcTcpStatement)this.stmt;
    }

    @Override
    protected RowIterator fetchRows(boolean meta) throws SQLException {
        JdbcTcpConnection conn = getConnection();
        Transfer ch = conn.ch;
        JdbcTcpStatement stmt = getStatement();
        ResultSetMetaData metaData = this.rowItr.getMetaData();
        try {
            int fetch = Transfer.CMD_FETCH_ROWS;
            int size = stmt.getFetchSize();
            ch.writeByte(fetch)
                    .writeInt(stmt.id)
                    .writeInt(size)
                    .flush();

            return stmt.readRows(metaData);
        } catch (IOException e) {
            String s = "Fetch rows error";
            throw conn.handle(s, e);
        }
    }

}
