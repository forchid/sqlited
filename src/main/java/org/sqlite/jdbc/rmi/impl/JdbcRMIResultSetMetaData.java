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

import org.sqlite.jdbc.adapter.ResultSetMetaDataAdapter;
import org.sqlite.rmi.RMIResultSetMetaData;

import java.sql.SQLException;

public class JdbcRMIResultSetMetaData extends ResultSetMetaDataAdapter {

    protected final RMIResultSetMetaData rmiMetaData;

    public JdbcRMIResultSetMetaData(RMIResultSetMetaData rmiMetaData) {
        this.rmiMetaData = rmiMetaData;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.rmiMetaData.getColumnCount();
    }

    public int findColumn(String name) throws SQLException {
        return this.rmiMetaData.findColumn(name);
    }

    @Override
    public String getColumnLabel(int columnIndex) throws SQLException {
        return this.rmiMetaData.getColumnLabel(columnIndex);
    }

    @Override
    public String getColumnName(int columnIndex) throws SQLException {
        return this.rmiMetaData.getColumnName(columnIndex);
    }

    @Override
    public int getColumnType(int columnIndex) throws SQLException {
        return this.rmiMetaData.getColumnType(columnIndex);
    }

    @Override
    public int getColumnDisplaySize(int columnIndex) throws SQLException {
        return this.rmiMetaData.getColumnDisplaySize(columnIndex);
    }

}
