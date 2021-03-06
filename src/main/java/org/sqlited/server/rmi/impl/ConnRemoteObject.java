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

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

class ConnRemoteObject extends UnicastRemoteObject {

    protected final RMIConnectionImpl conn;

    protected ConnRemoteObject(RMIConnectionImpl conn) throws RemoteException {
        super(conn.config.getPort(), conn.clientSocketFactory,
                conn.serverSocketFactory);
        this.conn = conn;
    }

}
