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

import java.rmi.RemoteException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class RMIResultSetMetaDataImpl extends ConnRemoteObject implements RMIResultSetMetaData {

    protected final ResultSetMetaData metaData;


    protected RMIResultSetMetaDataImpl(RMIConnectionImpl conn, ResultSetMetaData metaData)
            throws RemoteException {
        super(conn);
        this.metaData = metaData;
    }

    @Override
    public int getColumnCount() throws RemoteException, SQLException {
        return this.metaData.getColumnCount();
    }

    @Override
    public String getColumnName(int column) throws RemoteException, SQLException {
        return this.metaData.getColumnName(column);
    }

    @Override
    public int getColumnType(int column) throws RemoteException, SQLException {
        return this.metaData.getColumnType(column);
    }

    @Override
    public int getColumnDisplaySize(int column) throws RemoteException, SQLException {
        return this.metaData.getColumnDisplaySize(column);
    }

}
