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

package org.sqlited.server.rmi.impl;

import org.sqlited.rmi.RMIResultSetMetaData;
import org.sqlited.rmi.RMIResultSet;
import org.sqlited.rmi.RowIterator;
import org.sqlited.util.IOUtils;

import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import static java.sql.ResultSetMetaData.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RMIResultSetImpl implements RMIResultSet {

    protected static final int FETCH_SIZE_DEFAULT = 50;
    protected static final int FETCH_SIZE_MAXIMUM = 500;

    protected final ResultSet rs;

    protected RMIResultSetImpl(ResultSet rs) {
        this.rs = rs;
    }

    @Override
    public RowIterator next(boolean meta) throws RemoteException, SQLException {
        final int n, size = this.rs.getFetchSize();
        if (size == 0) {
            n = FETCH_SIZE_DEFAULT;
        } else {
            n = Math.min(size, FETCH_SIZE_MAXIMUM);
        }

        final RMIResultSetMetaData metaData;
        if (meta && !this.rs.isClosed()) metaData = getMetaData();
        else metaData = null;

        boolean next = this.rs.next();
        if (next) {
            ResultSetMetaData rsMeta = this.rs.getMetaData();
            int m = rsMeta.getColumnCount();
            List<Object[]> rows = new ArrayList<>(n);
            for (int i = 0; next && i < n; ++i) {
                Object[] row = new Object[m];
                for (int j = 0; j < m; ++j) {
                    row[j] = this.rs.getObject(j + 1);
                }
                rows.add(row);
                next = this.rs.next();
            }
            return new RowIterator(rows, !next, metaData);
        } else {
            List<Object[]> rows = Collections.emptyList();
            return new RowIterator(rows, true, metaData);
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

        int[] scales = new int[n];
        for (int i = 0; i < n; ++i) {
            scales[i] = metaData.getScale(i + 1);
        }

        return new RMIResultSetMetaData(names, metas, typeNames, types, scales);
    }

    @Override
    public int getFetchSize() throws RemoteException, SQLException {
        return this.rs.getFetchSize();
    }

    @Override
    public void setFetchSize(int rows) throws RemoteException, SQLException {
        this.rs.setFetchSize(rows);
    }

    @Override
    public int getFetchDirection() throws RemoteException, SQLException {
        return this.rs.getFetchDirection();
    }

    @Override
    public void setFetchDirection(int direction) throws RemoteException, SQLException {
        this.rs.setFetchDirection(direction);
    }

    @Override
    public void close() throws RemoteException {
        IOUtils.close(this.rs);
    }

}
