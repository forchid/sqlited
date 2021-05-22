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

package org.sqlited.server.rmi.util;

import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;

import static java.rmi.server.UnicastRemoteObject.unexportObject;

/**
 * Remote Object utils.
 */
public final class ROUtils {

    private ROUtils() {}

    public static boolean unexport(Remote obj) {
        return unexport(obj, true);
    }

    public static boolean unexport(Remote obj, boolean force) {
        if (obj == null) {
            return false;
        }

        try {
            unexportObject(obj, force);
            return true;
        } catch (NoSuchObjectException ignore) {
            return false;
        }
    }

    public static boolean unbind(Registry registry, String name) {
        if (registry == null) {
            return false;
        }
        try {
            registry.unbind(name);
            return true;
        } catch (RemoteException | NotBoundException e) {
            return false;
        }
    }

}
