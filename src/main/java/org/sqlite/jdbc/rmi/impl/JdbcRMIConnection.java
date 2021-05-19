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

import org.sqlite.jdbc.adapter.ConnectionAdapter;
import static org.sqlite.jdbc.rmi.util.RMIUtils.*;
import org.sqlite.rmi.RMIConnection;
import org.sqlite.rmi.RMIStatement;
import org.sqlite.util.IOUtils;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcRMIConnection extends ConnectionAdapter {

    protected final Properties props;
    protected final RMIConnection rmiConn;

    public JdbcRMIConnection(Properties props, RMIConnection rmiConn) {
        this.props = props;
        this.rmiConn = rmiConn;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return invoke(() -> {
            RMIStatement stmt = null;
            boolean failed = true;
            try {
                stmt = this.rmiConn.createStatement();
                Statement s = new JdbcRMIStatement(this, stmt);
                failed = false;
                return s;
            } finally {
                if (failed) IOUtils.close(stmt);
            }
        }, this.props);
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiConn);
    }

}
