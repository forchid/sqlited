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
import org.sqlited.rmi.RMIStatement;
import org.sqlited.util.IOUtils;

import java.rmi.RemoteException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RMIStatementImpl extends ConnRemoteObject implements RMIStatement {

    protected final Statement stmt;

    protected RMIStatementImpl(RMIConnectionImpl conn, Statement stmt)
            throws RemoteException {
        super(conn);
        this.stmt = stmt;
    }

    @Override
    public RMIResultSet executeQuery(String s)
            throws RemoteException, SQLException {
        ResultSet rs = this.stmt.executeQuery(s);
        boolean failed = true;
        try {
            RMIResultSet r = new RMIResultSetImpl(this.conn, rs);
            failed = false;
            return r;
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
    public void close() throws RemoteException {
        IOUtils.close(this.stmt);
    }

}
