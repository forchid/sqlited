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
import org.sqlite.rmi.RMIConnection;
import org.sqlite.rmi.RMIDriver;
import org.sqlite.server.util.IoUtils;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class JdbcRMIDriver extends DriverAdapter {

    public static final String PREFIX = DriverAdapter.PREFIX + "rmi:";

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException {
        if (!isValidURL(url)) {
            return null;
        }

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

        String user = info.getProperty("user");
        String password = info.getProperty("password");
        info.remove("user");
        info.remove("password");
        i = url.indexOf('?', j);
        if (i != -1) {
            String p = url.substring(i + 1);
            String[] a = p.split("&");
            List<String> np = new ArrayList<>();
            for (String s: a) {
                String[] item = s.split("=", 2);
                if (item.length == 2) {
                    String name = item[0];
                    if (name.equals("user")) {
                        user = item[1];
                    } else if (name.equals("password")) {
                        password = item[1];
                    } else {
                        np.add(s);
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
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            RMIDriver rmiDriver = (RMIDriver) registry.lookup("SQLited");
            RMIConnection rmiConn = rmiDriver.connect(url, info);
            boolean failed = true;
            try {
                Connection c = new JdbcRMIConnection(rmiConn);
                failed = false;
                return c;
            } finally {
                if (failed) IoUtils.close(rmiConn);
            }
        } catch (NotBoundException | RemoteException e) {
            throw RMIUtils.wrap(e);
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
