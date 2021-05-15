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

package org.sqlite.server.rmi.impl;

import org.sqlite.JDBC;
import org.sqlite.rmi.RMIConnection;
import org.sqlite.rmi.RMIDriver;
import org.sqlite.server.Config;

import java.io.File;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Properties;

public class RMIDriverImpl extends UnicastRemoteObject implements RMIDriver {

    protected final Config config;

    public RMIDriverImpl(Config config) throws RemoteException {
        this.config = config;
    }

    @Override
    public RMIConnection connect(String url, Properties info)
            throws RemoteException, SQLException {
        String db = url;
        int i = url.indexOf('?');

        if (i != -1) {
            db = url.substring(0, i);
        }
        if (!"".equals(db) && !":memory:".equals(db)) {
            url = config.getDataDir() + File.separator + url;
        }
        if (!url.startsWith(JDBC.PREFIX)) {
            url = JDBC.PREFIX + url;
        }

        return new RMIConnectionImpl(url, info);
    }

}