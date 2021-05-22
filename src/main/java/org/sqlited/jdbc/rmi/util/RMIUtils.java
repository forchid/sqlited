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

package org.sqlited.jdbc.rmi.util;

import org.sqlited.rmi.AuthSocketFactory;

import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Properties;

public final class RMIUtils {

    private RMIUtils() {}

    public static SQLException wrap(Exception e) {
        if (e instanceof ConnectException
                || e instanceof NotBoundException) {
            return new SQLException(e.getMessage(), "08001", e);
        } else {
            return new SQLException(e);
        }
    }

    public static void invoke(VoidMethod m, Properties props) throws SQLException {
        AuthSocketFactory.attachProperties(props);
        try {
            m.invoke();
        } catch (RemoteException | NotBoundException e) {
            throw RMIUtils.wrap(e);
        } finally {
            AuthSocketFactory.detachProperties();
        }
    }

    public static <R> R invoke(RMIMethod<R> m, Properties props) throws SQLException {
        AuthSocketFactory.attachProperties(props);
        try {
            return m.invoke();
        } catch (RemoteException | NotBoundException e) {
            throw RMIUtils.wrap(e);
        } finally {
            AuthSocketFactory.detachProperties();
        }
    }

}
