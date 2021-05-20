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

package org.sqlite.server.rmi.impl;

import org.sqlite.rmi.RMIResultSetMetaData;
import org.sqlite.rmi.RMIResultSet;
import org.sqlite.util.IOUtils;

import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import static java.sql.ResultSetMetaData.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RMIResultSetImpl extends ConnRemoteObject implements RMIResultSet {

    protected static final int FETCH_SIZE = 50;

    protected final ResultSet rs;
    protected int fetchSize;

    protected RMIResultSetImpl(RMIConnectionImpl conn, ResultSet rs)
            throws RemoteException {
        super(conn);
        this.rs = rs;
    }

    @Override
    public List<Object[]> next() throws RemoteException, SQLException {
        int n = this.fetchSize;

        if (n == 0) {
            n = FETCH_SIZE;
        }
        boolean next = this.rs.next();
        if (next) {
            ResultSetMetaData meta = this.rs.getMetaData();
            int m = meta.getColumnCount();
            List<Object[]> rows = new ArrayList<>(n);
            for (int i = 0; next && i < n; ++i) {
                Object[] row = new Object[m];
                for (int j = 0; j < m; ++j) {
                    row[j] = this.rs.getObject(j + 1);
                }
                rows.add(row);
                next = this.rs.next();
            }
            return rows;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public RMIResultSetMetaData getMetaData() throws RemoteException, SQLException {
        ResultSetMetaData metaData = this.rs.getMetaData();
        int n = metaData.getColumnCount();

        String[] names = new String[n];
        for (int i = 0; i < n; ++i) {
            names[i] = metaData.getColumnName(i + 1);
        }

        boolean[][] metas = new boolean[n][3];
        for (int i = 0; i < n; ++i) {
            boolean[] meta = metas[i];
            meta[0] = metaData.isNullable(i + 1) == columnNullable;
            meta[1] = false; // Reserved: primary key flag
            meta[2] = metaData.isAutoIncrement(i + 1);
        }

        String[] typeNames = new String[n];
        for (int i = 0; i < n; ++i) {
            typeNames[i] = metaData.getColumnTypeName(i + 1);
        }
        int[] types = new int[n];
        for (int i = 0; i < n; ++i) {
            types[i] = metaData.getColumnType(i + 1);
        }

        return new RMIResultSetMetaData(names, metas, typeNames, types);
    }

    @Override
    public int getFetchSize() throws RemoteException, SQLException {
        return this.fetchSize;
    }

    @Override
    public void setFetchSize(int fetchSize) throws RemoteException, SQLException {
        if (fetchSize < 0) {
            throw new SQLException("fetch size " + fetchSize + " negative");
        }
        this.fetchSize = fetchSize;
    }

    @Override
    public void close() throws RemoteException {
        IOUtils.close(this.rs);
    }

    @Override
    public int getFetchDirection() throws RemoteException, SQLException {
        return this.rs.getFetchDirection();
    }

    @Override
    public void setFetchDirection(int direction) throws RemoteException, SQLException {
        this.rs.setFetchDirection(direction);
    }

}
