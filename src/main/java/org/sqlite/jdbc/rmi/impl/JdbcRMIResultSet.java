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

package org.sqlite.jdbc.rmi.impl;

import org.sqlite.jdbc.adapter.ResultSetAdapter;
import static org.sqlite.jdbc.rmi.util.RMIUtils.*;
import org.sqlite.rmi.RMIResultSet;
import org.sqlite.rmi.RMIResultSetMetaData;
import org.sqlite.rmi.RowIterator;
import org.sqlite.util.IOUtils;

import java.sql.SQLException;

public class JdbcRMIResultSet extends ResultSetAdapter {

    protected final JdbcRMIConnection conn;
    protected final RMIResultSet rmiRs;

    private JdbcRMIResultSetMetaData metaData;
    protected RowIterator rowItr;

    public JdbcRMIResultSet(JdbcRMIConnection conn, RMIResultSet rmiRs) {
        this.conn = conn;
        this.rmiRs = rmiRs;
    }

    @Override
    public boolean next() throws SQLException {
        RowIterator i = this.rowItr;
        if (i != null && i.isLast() && !i.hasNext()) {
            IOUtils.close(this);
            return false;
        }

        if (i == null || !i.hasNext()) {
            boolean meta = this.metaData == null;
            i = invoke(() -> this.rmiRs.next(meta), this.conn.props);
            RMIResultSetMetaData d = i.getMetaData();
            if (meta && d != null) {
                this.metaData = new JdbcRMIResultSetMetaData(d);
            }
            this.rowItr = i.reset();
        }
        boolean next = i.hasNext();
        if (next) i.next();

        return next;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        JdbcRMIResultSetMetaData meta = getMetaData();
        int column = meta.findColumn(columnLabel);
        Object[] row = this.rowItr.get();
        return row[column - 1];
    }

    @Override
    public Object getObject(int column) throws SQLException {
        Object[] row = this.rowItr.get();
        return row[column - 1];
    }

    protected static SQLException castException(String type) {
        return new SQLException("Column can't cast to " + type);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Object value = getObject(columnLabel);
        return castToInt(value);
    }

    @Override
    public int getInt(int column) throws SQLException {
        Object value = getObject(column);
        return castToInt(value);
    }

    protected int castToInt(Object value) throws SQLException {
        if (value == null) {
            return 0;
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return n.intValue();
        } else if (value instanceof String) {
            String s = (String) value;
            return Long.decode(s).intValue();
        } else {
            throw castException("int");
        }
    }

    @Override
    public String getString(int column) throws SQLException {
        Object value = getObject(column);
        return castToString(value);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object value = getObject(columnLabel);
        return castToString(value);
    }

    protected String castToString(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof String) {
            return (String) value;
        } else {
            throw castException("String");
        }
    }

    protected JdbcRMIResultSetMetaData initMetaData() throws SQLException {
        if (this.metaData == null) {
            this.metaData = invoke(() -> {
                RMIResultSetMetaData metaData = this.rmiRs.getMetaData();
                return new JdbcRMIResultSetMetaData(metaData);
            }, this.conn.props);
        }

        return this.metaData;
    }

    @Override
    public JdbcRMIResultSetMetaData getMetaData() throws SQLException {
        JdbcRMIResultSetMetaData metaData = this.metaData;
        if (metaData == null) {
            return initMetaData();
        } else {
            return metaData;
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return invoke(this.rmiRs::getFetchSize, this.conn.props);
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        invoke(() -> this.rmiRs.setFetchSize(fetchSize), this.conn.props);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return invoke(this.rmiRs::getFetchDirection, this.conn.props);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        invoke(() -> this.rmiRs.setFetchDirection(direction), this.conn.props);
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiRs);
    }

}
