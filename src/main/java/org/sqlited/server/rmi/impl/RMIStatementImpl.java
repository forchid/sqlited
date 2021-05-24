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

import org.sqlited.rmi.RMIResultSet;
import org.sqlited.rmi.RMIResultSetMetaData;
import org.sqlited.rmi.RMIStatement;
import org.sqlited.rmi.RowIterator;
import org.sqlited.util.IOUtils;

import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RMIStatementImpl extends ConnRemoteObject implements RMIStatement {

    protected final Statement stmt;
    protected RMIResultSet rs;

    protected RMIStatementImpl(RMIConnectionImpl conn, Statement stmt)
            throws RemoteException {
        super(conn);
        this.stmt = stmt;
    }

    @Override
    public RowIterator executeQuery(String s)
            throws RemoteException, SQLException {
        ResultSet rs = this.stmt.executeQuery(s);
        boolean failed = true;
        try {
            this.rs = new RMIResultSetImpl(rs);
            RowIterator itr = next(true);
            failed = false;
            return itr;
        } finally {
            if (failed) IOUtils.close(rs);
        }
    }

    @Override
    public int executeUpdate(String s)
            throws RemoteException, SQLException {
        return this.stmt.executeUpdate(s);
    }

    @Override
    public RowIterator next(boolean meta) throws RemoteException, SQLException {
        return this.rs.next(meta);
    }

    @Override
    public RMIResultSetMetaData getMetaData() throws RemoteException, SQLException {
        return this.rs.getMetaData();
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
        IOUtils.close(this.stmt);
    }

}
