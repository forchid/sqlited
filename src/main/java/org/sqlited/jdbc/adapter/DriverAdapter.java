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

import org.sqlited.net.SocketUtils;
import org.sqlited.util.PropsUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public abstract class DriverAdapter implements Driver {

    protected static final String PREFIX = "jdbc:sqlited:";
    protected static final String DEFAULT_HOST = "localhost";
    protected static final int DEFAULT_PORT = 3525;
    protected static final String DEFAULT_USER = "root";

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!isValidURL(url)) {
            return null;
        }
        // Make a copy
        Properties copy = new Properties();
        copy.putAll(info);
        info = copy;

        // Parse url
        // jdbc:sqlited:[rmi|tcp:][//[HOST][:PORT]/][DB][?a=b&...]
        int i = DriverAdapter.PREFIX.length();
        String prefix = getPrefix();
        if (url.toLowerCase().startsWith(prefix)) {
            i = prefix.length();
        }

        final int j;
        if (i < url.length() && url.indexOf('/', i) != -1) {
            if (url.charAt(i++) != '/' || i >= url.length() || url.charAt(i++) != '/') {
                throw new SQLException("Malformed url '" + url + "'");
            }
            int k = url.indexOf('/', i);
            if (k == -1) {
                throw new SQLException("Malformed url '" + url + "'");
            }
            j = ++k;
        } else {
            j = i;
        }

        String host = DEFAULT_HOST;
        int port = getPort();
        if (i < j) {
            String s = url.substring(i, j - 1);
            int k = s.indexOf(':');
            if (k != -1) {
                if (k > 0) {
                    host = s.substring(0, k);
                }
                if (k + 1 < s.length()) {
                    s = s.substring(k + 1);
                    port = Integer.decode(s);
                }
            }
        }

        final String path;
        String user = PropsUtils.remove(info,"user", DEFAULT_USER);
        String password = PropsUtils.remove(info, "password");
        String loginTimeout = PropsUtils.remove(info, "loginTimeout");
        String connectTimeout = PropsUtils.remove(info, "connectTimeout");
        String readTimeout = PropsUtils.remove(info, "readTimeout");
        i = url.indexOf('?', j);
        if (i != -1) {
            path = url.substring(0, i);
            String p = url.substring(i + 1);
            String[] a = p.split("&");
            List<String> np = new ArrayList<>();
            for (String s: a) {
                String[] item = s.split("=", 2);
                if (item.length == 2) {
                    String name = item[0];
                    switch (name) {
                        case "user":
                            user = item[1];
                            break;
                        case "password":
                            password = item[1];
                            break;
                        case "loginTimeout":
                            loginTimeout = item[1];
                            break;
                        case "connectTimeout":
                            connectTimeout = item[1];
                            break;
                        case "readTimeout":
                            readTimeout = item[1];
                            break;
                        default:
                            np.add(s);
                            break;
                    }
                } else {
                    throw new SQLException("Malformed url '" + url + "'");
                }
            }
            url = url.substring(j, i);
            if (np.size() > 0) {
                url += "?" + String.join("&", np);
            }
        } else {
            path = url;
            url = url.substring(j);
        }

        // Do connect
        final Properties connProps = new Properties();
        connProps.setProperty("host", host);
        connProps.setProperty("port", port + "");
        connProps.setProperty("url", path);
        connProps.setProperty("user", user);
        PropsUtils.setNullSafe(connProps, "password", password);
        PropsUtils.setNullSafe(connProps, "loginTimeout", loginTimeout);
        PropsUtils.setNullSafe(connProps, "connectTimeout", connectTimeout);
        PropsUtils.setNullSafe(connProps, "readTimeout", readTimeout);

        return connect(url, info, SocketUtils.defaultConfig(connProps));
    }

    protected abstract Connection connect(String url, Properties info,
                                          Properties connProps)
            throws SQLException;

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return isValidURL(url);
    }

    protected String getPrefix() {
        return PREFIX;
    }

    protected int getPort() {
        return DEFAULT_PORT;
    }

    protected boolean isValidURL(String url) {
        String prefix = getPrefix();
        return url != null && url.toLowerCase().startsWith(prefix);
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
        String root = getClass().getPackage().getName();
        return LoggerFactory.getLogger(root);
    }

}
