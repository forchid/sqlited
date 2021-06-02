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
import org.sqlited.result.RowIterator;
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

    protected ResultSet initResultSet() throws SQLException {
        IOUtils.close(this.rs);
        return (this.rs = this.stmt.getResultSet());
    }

    public void sendResultSet(boolean meta) throws IOException, SQLException {
        // Format: ResultSet flag, status-1, resultSetMeta, rows, status-2
        TcpConnection conn = this.conn;
        ResultSet rs = initResultSet();
        Transfer ch = conn.ch;
        int status = meta? 0x1: 0x0;
        ch.write(Transfer.RESULT_SET)
                .writeInt(status);
        boolean next = false;
        if (meta) next = writeResultSetMeta(rs);
        else ch.writeArray(null);
        writeRows(next)
        .flush();
    }

    protected Transfer writeRows(boolean next) throws SQLException, IOException {
        // Format: [row, ..., ] null(row end), status
        ResultSet rs = this.rs;
        Transfer ch = this.conn.ch;
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
        return ch.write(status);
    }

    protected boolean writeResultSetMeta(ResultSet rs) throws SQLException, IOException {
        boolean next = rs.next();
        org.sqlited.result.ResultSetMetaData rsMeta = null;

        if (next) {
            ResultSetMetaData metaData = rs.getMetaData();
            int n = metaData.getColumnCount();

            String[] names = new String[n];
            for (int i = 0; i < n; ++i) {
                names[i] = metaData.getColumnName(i + 1);
            }

            int[] metas = new int[n];
            for (int i = 0; i < n; ++i) {
                int j = i + 1;
                int meta = metaData.isNullable(j) == columnNullable? 0x01: 0x00;
                //meta |= 0x00; // Reserved: primary key flag
                meta |= metaData.isAutoIncrement(j)? 0x04: 0x00;
                metas[i] = meta;
            }

            String[] typeNames = new String[n];
            for (int i = 0; i < n; ++i) {
                typeNames[i] = metaData.getColumnTypeName(i + 1);
            }
            int[] types = new int[n];
            for (int i = 0; i < n; ++i) {
                types[i] = metaData.getColumnType(i + 1);
            }

            int[] scales = new int[n];
            for (int i = 0; i < n; ++i) {
                scales[i] = metaData.getScale(i + 1);
            }
            rsMeta = new org.sqlited.result.ResultSetMetaData(names,
                    metas, typeNames, types, scales);
        }
        writeResultSetMeta(rsMeta);

        return next;
    }

    public void sendResultSet(RowIterator rowItr) throws IOException {
        // Format: ResultSet flag, status-1, resultSetMeta, rows, status-2
        org.sqlited.result.ResultSetMetaData meta = rowItr.getMetaData();
        Transfer ch = this.conn.ch;
        int status = 0x1;

        ch.write(Transfer.RESULT_SET)
                .writeInt(status);
        writeResultSetMeta(meta)
                .writeRows(rowItr)
                .flush();
    }

    public void sendResultSet(org.sqlited.result.ResultSetMetaData meta)
            throws IOException, SQLException {
        // Format: ResultSet flag, status-1, resultSetMeta, rows, status-2
        Transfer ch = this.conn.ch;
        int status = 0x1;

        initResultSet();
        ch.write(Transfer.RESULT_SET)
                .writeInt(status);
        writeResultSetMeta(meta)
                .writeRows(false)
                .flush();
    }

    protected Transfer writeRows(RowIterator rowItr) throws IOException {
        // Format: [row, ..., ] null(row end), status
        Transfer ch = this.conn.ch;

        while (rowItr.hasNext()) {
            Object[] row = rowItr.next();
            ch.writeArray(row);
        }
        // Row end
        ch.writeArray(null);

        int status = 0x00;
        return ch.write(status);
    }

    protected TcpStatement writeResultSetMeta(org.sqlited.result.ResultSetMetaData meta)
            throws IOException {
        // Format: names, metas, typeNames, types, scales, null(meta end)
        Transfer ch = this.conn.ch;

        if (meta != null) {
            ch.writeArray(meta.getNames())
                    .writeArray(meta.getColumnIntMetas())
                    .writeArray(meta.getColumnTypeNames())
                    .writeArray(meta.getColumnTypes())
                    .writeArray(meta.getScales());
        }
        ch.writeArray(null);
        return this;
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
