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
import org.sqlite.server.util.IoUtils;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcRMIResultSet extends ResultSetAdapter {

    protected final RMIResultSet rmiRs;

    public JdbcRMIResultSet(RMIResultSet rmiRs) {
        this.rmiRs = rmiRs;
    }

    @Override
    public boolean next() throws SQLException {
        return invoke(this.rmiRs::next);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return invoke(() -> this.rmiRs.getInt(columnLabel));
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return invoke(() -> this.rmiRs.getString(columnLabel));
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return invoke(() -> {
            RMIResultSetMetaData metaData = this.rmiRs.getMetaData();
            return new JdbcRMIResultSetMetaData(metaData);
        });
    }

    @Override
    public void close() {
        IoUtils.close(this.rmiRs);
    }

}
