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
import org.sqlite.util.IOUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class JdbcRMIResultSet extends ResultSetAdapter {

    protected final JdbcRMIConnection conn;
    protected final RMIResultSet rmiRs;
    private JdbcRMIResultSetMetaData metaData;
    protected List<Object[]> rows = Collections.emptyList();
    private int index = -1;
    private boolean hasNext = true;

    public JdbcRMIResultSet(JdbcRMIConnection conn, RMIResultSet rmiRs) {
        this.conn = conn;
        this.rmiRs = rmiRs;
    }

    @Override
    public boolean next() throws SQLException {
        if (!this.hasNext) {
            return false;
        }

        if (++this.index >= this.rows.size()) {
            initMetaData();
            this.rows = invoke(this.rmiRs::next, this.conn.props);
            this.index = 0;
            this.hasNext = this.index < this.rows.size();
            if (!this.hasNext) {
                IOUtils.close(this);
                this.index = -1;
            }
            return this.hasNext;
        } else {
            return true;
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        JdbcRMIResultSetMetaData meta = getMetaData();
        int column = meta.findColumn(columnLabel);
        Object[] row = this.rows.get(this.index);
        return row[column - 1];
    }

    @Override
    public Object getObject(int column) throws SQLException {
        Object[] row = this.rows.get(this.index);
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
            return ((Number)value).intValue();
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
