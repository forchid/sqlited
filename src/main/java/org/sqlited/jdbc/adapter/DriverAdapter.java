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

package org.sqlited.jdbc.adapter;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class DriverAdapter implements Driver {

    protected static final String PREFIX = "jdbc:sqlited:";
    protected static final String DEFAULT_HOST = "localhost";
    protected static final int DEFAULT_PORT = 3515;
    protected static final String DEFAULT_USER = "root";

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return isValidURL(url);
    }

    protected boolean isValidURL(String url) {
        return url != null && url.toLowerCase().startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties)
            throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return null;
    }

}
