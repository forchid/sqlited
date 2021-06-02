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

package org.sqlited.jdbc.tcp;

import org.sqlited.jdbc.adapter.DriverAdapter;
import org.sqlited.jdbc.tcp.impl.JdbcTcpConnection;
import org.sqlited.net.AuthSocketFactory;
import org.sqlited.util.IOUtils;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Locale;
import java.util.Properties;

public class JdbcTcpDriver extends DriverAdapter {

    public static final String PREFIX = DriverAdapter.PREFIX + "tcp:";

    @Override
    protected Connection connect(String url, Properties info, Properties connProps)
            throws SQLException {
        SocketFactory socketFactory = new AuthSocketFactory(connProps);
        String host = connProps.getProperty("host");
        int port = Integer.decode(connProps.getProperty("port"));

        try {
            Socket socket = socketFactory.createSocket(host, port);
            boolean failed = true;
            try {
                JdbcTcpConnection conn = new JdbcTcpConnection(connProps, socket);
                conn.openDB(url, info);
                failed = false;
                return conn;
            } finally {
                if (failed) IOUtils.close(socket);
            }
        } catch (IOException e) {
            String s = "Connection failure";
            throw new SQLNonTransientConnectionException(s, "08001", e);
        }
    }

    @Override
    protected boolean isValidURL(String url) {
        if (super.isValidURL(url)) {
            return true;
        } else {
            if (url == null) {
                return false;
            } else {
                // Default driver
                String s = url.toLowerCase(Locale.ENGLISH);
                return s.startsWith(DriverAdapter.PREFIX);
            }
        }
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

}
