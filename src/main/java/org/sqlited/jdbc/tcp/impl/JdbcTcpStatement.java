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
import org.sqlited.jdbc.adapter.StatementAdapter;
import org.sqlited.result.ResultSetMetaData;
import org.sqlited.result.RowIterator;
import org.sqlited.util.IOUtils;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class JdbcTcpStatement extends StatementAdapter {

    protected final JdbcTcpConnection conn;
    protected final int id;
    protected int fetchSize;

    protected JdbcTcpResultSet resultSet;
    protected long lastId;
    protected long affectedRows;

    public JdbcTcpStatement(JdbcTcpConnection conn, int id) {
        this.conn = conn;
        this.id = id;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        execute(sql, Statement.NO_GENERATED_KEYS);
        return getResultSet();
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return this.resultSet;
    }

    @Override
    public boolean execute(String sql, String[] columnNames)
            throws SQLException {
        if (columnNames == null) throw new NullPointerException();
        return execute(sql, Statement.RETURN_GENERATED_KEYS, null, columnNames);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes)
            throws SQLException {
        if (columnIndexes == null) throw new NullPointerException();
        return execute(sql, Statement.RETURN_GENERATED_KEYS, columnIndexes, null);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys)
            throws SQLException {
        return execute(sql, autoGeneratedKeys, null, null);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        execute(sql);
        return getUpdateCount();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        execute(sql, autoGeneratedKeys);
        return getUpdateCount();
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        execute(sql, columnIndexes);
        return getUpdateCount();
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        execute(sql, columnNames);
        return getUpdateCount();
    }

    protected boolean execute(String sql, int autoGeneratedKeys,
                              int[] columnIndexes, String[] columnNames)
            throws SQLException {
        Transfer ch = this.conn.ch;
        try {
            Object genColumns = columnIndexes == null?
                    columnNames: columnIndexes;
            // Send
            ch.writeByte(Transfer.CMD_EXECUTE)
                    .writeInt(this.id)
                    .writeString(sql)
                    .writeInt(autoGeneratedKeys)
                    .writeArray(genColumns)
                    .flush();
            // Reply
            final int result = ch.readByte(true);
            if (Transfer.RESULT_SET == result) {
                // - ResultSet status flag
                ch.readInt();
                ResultSetMetaData metaData = readMetaData();
                RowIterator rowItr = readRows(metaData);
                IOUtils.close(this.resultSet);
                this.resultSet = new JdbcTcpResultSet(this.conn, this, rowItr);
                this.lastId = 0;
                this.affectedRows = 0;
                return true;
            } else {
                IOUtils.close(this.resultSet);
                this.resultSet = null;
                long[] a = this.conn.readOK(result);
                this.lastId = a[0];
                this.affectedRows = a[1];
                return false;
            }
        } catch (IOException e) {
            String s = "Execute statement error";
            throw this.conn.handle(s, e);
        }
    }

    protected RowIterator readRows(ResultSetMetaData metaData)
            throws IOException {
        // Format: [row ..., ] null(row end), status
        List<Object[]> rows = new ArrayList<>();
        Transfer ch = this.conn.ch;
        Object[] row = (Object[])ch.readArray();

        while (row != null) {
            rows.add(row);
            row = (Object[])ch.readArray();
        }
        int status = ch.readByte(true);
        boolean last = (status & 0x01) == 0x00;

        return new RowIterator(rows, last, metaData);
    }

    protected ResultSetMetaData readMetaData() throws IOException {
        Transfer ch = this.conn.ch;
        Object a = ch.readArray();

        if (a == null) {
            return null;
        } else {
            String[] names = (String[])a;
            int[] metas = (int[])ch.readArray();
            String[] typeNames = (String[]) ch.readArray();
            int[] types = (int[]) ch.readArray();
            int[] scales = (int[]) ch.readArray();
            while (ch.readArray() != null);
            int n = metas.length;
            boolean[][] colMetas = new boolean[n][];
            for (int i = 0; i < n; ++i) {
                boolean[] meta = new boolean[3];
                meta[0] = (metas[i] & 0x01) != 0x0;
                meta[1] = (metas[i] & 0x02) != 0x0;
                meta[2] = (metas[i] & 0x04) != 0x0;
                colMetas[i] = meta;
            }
            return new ResultSetMetaData(names, colMetas, typeNames, types, scales);
        }
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) throw new SQLException("Fetch size negative: " + rows);
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return this.fetchSize;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return (int)this.affectedRows;
    }

    @Override
    public void close() throws SQLException {
        try {
            Transfer ch = this.conn.ch;
            ch.writeByte(Transfer.CMD_CLOSE_STMT)
                    .writeInt(this.id)
                    .flush();
        } catch (IOException e) {
            String s = "Close statement error";
            throw this.conn.handle(s, e);
        }
    }

}
