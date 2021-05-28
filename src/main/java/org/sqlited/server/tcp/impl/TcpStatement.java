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

package org.sqlited.server.tcp.impl;

import org.sqlited.io.Transfer;
import org.sqlited.util.IOUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static java.sql.ResultSetMetaData.columnNullable;

public class TcpStatement implements AutoCloseable {

    protected final TcpConnection conn;
    protected final Statement stmt;

    private ResultSet rs;

    public TcpStatement(TcpConnection conn, Statement stmt) {
        this.conn = conn;
        this.stmt = stmt;
    }

    public void setResultSet(ResultSet rs) {
        if (this.rs != rs) IOUtils.close(this.rs);
        this.rs = rs;
    }

    public void sendResultSet(boolean meta) throws IOException, SQLException {
        // Format: ResultSet flag, status-1, resultSetMeta, rows, status-2
        TcpConnection conn = this.conn;
        Transfer ch = conn.ch;
        int status = meta? 0x1: 0x0;
        ch.writeByte(Transfer.RESULT_SET).writeInt(status);
        boolean next = false;
        if (meta) next = writeResultSetMeta();
        else ch.writeArray(null);
        writeRows(next)
        .flush();
    }

    protected Transfer writeRows(boolean next) throws SQLException, IOException {
        // Format: [row, ..., ] null(row end), status
        Transfer ch = this.conn.ch;
        ResultSet rs = this.rs;
        if (next || (next = rs.next())) {
            ResultSetMetaData metaData = rs.getMetaData();
            int m = metaData.getColumnCount();
            int n = Math.min(Math.max(rs.getFetchSize(), 50), 500);
            int i = 0;
            do {
                Object[] row = new Object[m];
                for (int j = 0; j < m; ++j) {
                    row[j] = rs.getObject(j + 1);
                }
                ch.writeArray(row);
            } while (++i < n && (next = rs.next()));
        }
        // Row end
        ch.writeArray(null);

        int status = next? 0x01:0x00;
        return ch.writeByte(status);
    }

    protected boolean writeResultSetMeta() throws SQLException, IOException {
        // Format: [names, metas, typeNames, types, scales, ] null(meta end)
        Transfer ch = this.conn.ch;
        ResultSet rs = this.rs;
        boolean next = rs.next();

        if (next) {
            ResultSetMetaData metaData = this.rs.getMetaData();
            int n = metaData.getColumnCount();

            String[] names = new String[n];
            for (int i = 0; i < n; ++i) {
                names[i] = metaData.getColumnName(i + 1);
            }
            ch.writeArray(names);

            int[] metas = new int[n];
            for (int i = 0; i < n; ++i) {
                int meta = metaData.isNullable(i + 1) == columnNullable? 0x01: 0x00;
                //meta |= 0x00; // Reserved: primary key flag
                meta |= metaData.isAutoIncrement(i + 1)? 0x04: 0x00;
                metas[i] = meta;
            }
            ch.writeArray(metas);

            String[] typeNames = new String[n];
            for (int i = 0; i < n; ++i) {
                typeNames[i] = metaData.getColumnTypeName(i + 1);
            }
            ch.writeArray(typeNames);
            int[] types = new int[n];
            for (int i = 0; i < n; ++i) {
                types[i] = metaData.getColumnType(i + 1);
            }
            ch.writeArray(types);

            int[] scales = new int[n];
            for (int i = 0; i < n; ++i) {
                scales[i] = metaData.getScale(i + 1);
            }
            ch.writeArray(scales);
        }
        ch.writeArray(null);

        return next;
    }

    public void fetchRows() throws IOException, SQLException {
        // Format: [row, ..., ] null(row end)
        writeRows(false)
        .flush();
    }

    @Override
    public void close() {
        IOUtils.close(this.rs);
        IOUtils.close(this.stmt);
    }

}
