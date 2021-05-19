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

package org.sqlite.jdbc.rmi;

import org.sqlite.jdbc.adapter.DriverAdapter;
import org.sqlite.jdbc.rmi.impl.JdbcRMIConnection;
import org.sqlite.jdbc.rmi.util.RMIUtils;
import org.sqlite.rmi.AuthSocketFactory;
import org.sqlite.rmi.RMIConnection;
import org.sqlite.rmi.RMIDriver;
import org.sqlite.rmi.util.SocketUtils;
import org.sqlite.util.IOUtils;
import org.sqlite.util.PropsUtils;
import org.sqlite.util.logging.LoggerFactory;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIClientSocketFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;

public class JdbcRMIDriver extends DriverAdapter {
    static final Logger log = LoggerFactory.getLogger(JdbcRMIDriver.class);

    static final RMIClientSocketFactory SO_FACTORY = new AuthSocketFactory();
    public static final String PREFIX = DriverAdapter.PREFIX + "rmi:";

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException {
        if (!isValidURL(url)) {
            return null;
        }
        // Make a copy
        Properties copy = new Properties();
        copy.putAll(info);
        info = copy;

        // Parse url
        // jdbc:sqlited:[rmi:][//[HOST][:PORT]/][DB][?a=b&...]
        int i = DriverAdapter.PREFIX.length();
        if (url.toLowerCase().startsWith(PREFIX)) {
            i = PREFIX.length();
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
        int port = DEFAULT_PORT;
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

        String user = PropsUtils.remove(info,"user", DEFAULT_USER);
        String password = PropsUtils.remove(info, "password");
        String loginTimeout = PropsUtils.remove(info, "loginTimeout");
        String connectTimeout = PropsUtils.remove(info, "connectTimeout");
        String readTimeout = PropsUtils.remove(info, "readTimeout");
        i = url.indexOf('?', j);
        if (i != -1) {
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
            url = url.substring(j);
        }

        // Do connect
        final Properties props = new Properties();
        props.put("user", user);
        PropsUtils.setNullSafe(props, "password", password);
        PropsUtils.setNullSafe(props, "loginTimeout", loginTimeout);
        PropsUtils.setNullSafe(props, "connectTimeout", connectTimeout);
        PropsUtils.setNullSafe(props, "readTimeout", readTimeout);
        try {
            SocketUtils.attachProperties(props);
            log.fine(() -> String.format("%s: locate remote registry",
                    Thread.currentThread().getName()));
            Registry registry = LocateRegistry.getRegistry(host, port, SO_FACTORY);
            log.fine(() -> String.format("%s: lookup remote driver",
                    Thread.currentThread().getName()));
            RMIDriver rmiDriver = (RMIDriver) registry.lookup("SQLited");
            log.fine(() -> String.format("%s: get a remote connection",
                    Thread.currentThread().getName()));
            RMIConnection rmiConn = rmiDriver.connect(url, info);
            boolean failed = true;
            try {
                Connection c = new JdbcRMIConnection(props, rmiConn);
                failed = false;
                return c;
            } finally {
                if (failed) IOUtils.close(rmiConn);
            }
        } catch (NotBoundException | RemoteException e) {
            throw RMIUtils.wrap(e);
        } finally {
            SocketUtils.detachProperties();
        }
    }

    @Override
    protected boolean isValidURL(String url) {
        if (super.isValidURL(url)) {
            return true;
        } else {
            return url != null && url.toLowerCase().startsWith(PREFIX);
        }
    }

}
