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

import org.sqlite.server.rmi.RMIResultSetMetaData;
import org.sqlite.server.rmi.RMIResultSet;
import org.sqlite.server.util.IoUtils;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RMIResultSetImpl extends UnicastRemoteObject implements RMIResultSet {

    protected final ResultSet rs;

    protected RMIResultSetImpl(ResultSet rs) throws RemoteException {
        this.rs = rs;
    }

    @Override
    public boolean next() throws RemoteException, SQLException {
        return this.rs.next();
    }

    @Override
    public int getInt(String column) throws RemoteException, SQLException {
        return this.rs.getInt(column);
    }

    @Override
    public String getString(String column) throws RemoteException, SQLException {
        return this.rs.getString(column);
    }

    @Override
    public RMIResultSetMetaData getMetaData() throws RemoteException, SQLException {
        ResultSetMetaData metaData = this.rs.getMetaData();
        return new RMIResultSetMetaDataImpl(metaData);
    }

    @Override
    public void close() throws RemoteException {
        IoUtils.close(this.rs);
    }

}
