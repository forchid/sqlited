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
import org.sqlited.result.AutoGenKeysIterator;
import org.sqlited.result.ResultSetMetaData;
import org.sqlited.result.RowIterator;
import static org.sqlited.server.util.SQLiteUtils.*;
import org.sqlited.util.logging.LoggerFactory;

import static java.lang.String.*;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class AutoGenKeysListener implements SQLiteUpdateListener {
    static final Logger log = LoggerFactory.getLogger(AutoGenKeysListener.class);

    protected final TcpStatement ts;
    protected final boolean autoCommit;

    protected String database;
    protected String table;
    protected long min;
    protected long max;
    protected boolean hasGap; // Optimize

    protected final List<Long[]> keyRanges;
    protected long rangeMin;
    protected long rangeMax;

    protected boolean generated;

    public AutoGenKeysListener(TcpStatement ts, boolean autoCommit) {
        this.ts = ts;
        this.autoCommit = autoCommit;
        if (autoCommit) this.keyRanges = new ArrayList<>();
        else this.keyRanges = null;
    }

    @Override
    public void onUpdate(Type type, String database, String table, long rowId) {
        if (type == Type.INSERT) {
            this.generated = true;
            this.database = database;
            this.table = table;
            // Whole range
            if (this.min == 0) {
                this.min = this.max = rowId;
            }
            long max = this.max;
            if (max < rowId) {
                if (max + 1 < rowId) this.hasGap = true;
                this.max = rowId;
            }

            // A segment range for autocommit tx environment.
            // Note: in autocommit tx, we shouldn't query auto-generated keys
            // by the range [min, max].
            List<Long[]> keyRanges = this.keyRanges;
            if (keyRanges != null) {
                if (this.rangeMin == 0) {
                    this.rangeMin = this.rangeMax = rowId;
                }
                long raMax = this.rangeMax;
                if (raMax + 1 == rowId) {
                    // Increment without gap
                    this.rangeMax = rowId;
                } else if (raMax < rowId) {
                    // Increment with gap, so generate a new segment
                    keyRanges.add(new Long[]{ this.rangeMin, raMax });
                    this.rangeMin = this.rangeMax = rowId;
                }
            }
        }
    }

    public void sendResultSet() throws IOException, SQLException {
        ResultSetMetaData meta = ResultSetMetaData.AUTO_GEN_KEYS_META;
        TcpStatement ts = this.ts;
        Statement stmt = ts.stmt;
        String aiColumn;

        if (!this.generated || (aiColumn = autoIncrementColumn(stmt,
                this.database, this.table)) == null) {
            sendEmptySet(meta);
            return;
        }
        log.fine(() -> format("auto-generated column '%s'", aiColumn));

        List<Long[]> keyRanges = this.keyRanges;
        if (this.hasGap) {
            if (keyRanges == null) {
                sendResultSetByQuery(meta, aiColumn);
                return;
            }
        } else {
            log.fine(() -> format("auto-generated keys by key range [%d, %d]",
                    this.min, this.max));
            keyRanges = new ArrayList<>(1);
            keyRanges.add(new Long[]{ this.min, this.max });
        }
        RowIterator rowItr = new AutoGenKeysIterator(keyRanges, meta);
        ts.sendResultSet(rowItr);
    }

    protected void sendEmptySet(ResultSetMetaData meta) throws IOException {
        log.fine("no auto-generated keys");
        RowIterator rowItr = RowIterator.empty(meta);
        this.ts.sendResultSet(rowItr);
    }

    protected void sendResultSetByQuery(ResultSetMetaData meta, String aiColumn)
            throws SQLException, IOException {
        TcpStatement ts = this.ts;
        Statement stmt = ts.stmt;
        String f = "select %s from '%s'.'%s' where %s between %d and %d";
        String s = format(f, aiColumn, this.database, this.table, aiColumn,
                this.min, this.max);
        log.fine(() -> "auto-generated keys by " + s);

        stmt.execute(s);
        ts.sendResultSet(meta);
    }

}
