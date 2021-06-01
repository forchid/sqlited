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

package org.sqlited.server.tcp.impl;

import org.sqlite.SQLiteUpdateListener;
import org.sqlited.result.ResultSetMetaData;
import org.sqlited.result.RowIterator;
import org.sqlited.util.logging.LoggerFactory;

import static java.lang.String.*;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Logger;

public class AutoGenKeysListener implements SQLiteUpdateListener {
    static final Logger log = LoggerFactory.getLogger(AutoGenKeysListener.class);

    protected final TcpStatement ts;

    protected String database;
    protected String table;
    protected long min = 0;
    protected long max = 0;
    protected boolean generated;

    public AutoGenKeysListener(TcpStatement ts) {
        this.ts = ts;
    }

    @Override
    public void onUpdate(Type type, String database, String table, long rowId) {
        if (type == Type.INSERT) {
            this.database = database;
            this.table = table;
            if (this.min == 0) this.min = rowId;
            if (this.max < rowId) this.max = rowId;
            this.generated = true;
        }
    }

    public void sendResultSet() throws IOException, SQLException {
        TcpStatement ts = this.ts;
        ResultSetMetaData meta = ResultSetMetaData.AUTO_GEN_KEYS_META;

        if (this.generated) {
            // Query auto-generated column
            String sql = format("pragma '%s'.table_info('%s')",
                    this.database, this.table);
            Statement stmt = ts.stmt;
            String key = null;

            try (ResultSet rs = stmt.executeQuery(sql)) {
                while (rs.next()) {
                    if (rs.getInt("pk") == 1) {
                        key = rs.getString("name");
                        break;
                    }
                }
            }
            final String column = key;
            log.fine(() -> format("auto-generated column '%s' by %s", column, sql));
            if (column != null) {
                String f = "select %s from '%s'.'%s' where %s between %d and %d";
                String s = format(f, column, this.database, this.table, column,
                        this.min, this.max);
                log.fine(() -> "auto-generated keys by " + s);
                stmt.execute(s);
                ts.sendResultSet(meta);
                return;
            }
        }
        RowIterator rowItr = RowIterator.empty(meta);
        ts.sendResultSet(rowItr);
    }

}
