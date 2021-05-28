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

package org.sqlited.server.rmi.impl;

import org.sqlited.rmi.RMIConnection;
import org.sqlited.rmi.RMIDriver;
import org.sqlited.server.Config;
import org.sqlited.server.util.SQLiteUtils;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.sql.SQLException;
import java.util.Properties;

public class RMIDriverImpl extends UnicastRemoteObject implements RMIDriver {

    protected final Config config;

    public RMIDriverImpl(Config config) throws RemoteException {
        super(config.getPort(), config.getRMIClientSocketFactory(),
                config.getRMIServerSocketFactory());
        this.config = config;
    }

    @Override
    public RMIConnection connect(String url, Properties info)
            throws RemoteException, SQLException {
        Config config = this.config;
        url = SQLiteUtils.wrapURL(config.getDataDir(), url);
        return new RMIConnectionImpl(config, url, info);
    }

}
