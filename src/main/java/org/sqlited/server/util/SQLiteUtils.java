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

package org.sqlited.server.util;

import org.sqlite.JDBC;
import org.sqlite.SQLiteConnection;
import org.sqlited.util.logging.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public final class SQLiteUtils {

    static final Logger log = LoggerFactory.getLogger(SQLiteUtils.class);

    private SQLiteUtils() {}

    public static SQLiteConnection open(String url, Properties info)
            throws SQLException {
        log.fine(() -> String.format("Open DB '%s'", url));
        return JDBC.createConnection(url, info);
    }

    public static String wrapURL(String dataDir, String url) {
        String db = url;
        int i = url.indexOf('?');

        if (i != -1) {
            db = url.substring(0, i);
        }
        if (!"".equals(db) && !":memory:".equals(db)) {
            url = dataDir + File.separator + url;
        }
        if (!url.startsWith(JDBC.PREFIX)) {
            url = JDBC.PREFIX + url;
        }

        return url;
    }

}
