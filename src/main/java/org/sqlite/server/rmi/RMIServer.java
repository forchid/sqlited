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

package org.sqlite.server.rmi;

import org.sqlite.rmi.AuthServerSocketFactory;
import org.sqlite.rmi.AuthSocketFactory;
import org.sqlite.server.Config;
import org.sqlite.server.Server;
import org.sqlite.server.rmi.impl.RMIDriverImpl;
import org.sqlite.util.logging.LoggerFactory;

import static java.lang.Thread.*;
import java.io.File;
import java.rmi.NotBoundException;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.*;
import java.util.Properties;
import java.util.logging.Logger;

public class RMIServer implements Server {
    static Logger log = LoggerFactory.getLogger(RMIServer.class);

    protected final Config config;
    protected volatile Registry registry;
    private volatile boolean stopped;

    public RMIServer(Config config) {
        this.config = config;
    }

    @Override
    public void start()  throws IllegalStateException {
        if (this.stopped) {
            throw new IllegalStateException("Server stopped");
        }

        Config config = this.config;
        File baseDir = new File(config.getBaseDir());
        if (!baseDir.isDirectory() && !baseDir.mkdirs()) {
            String s = "Can't make base dir '" + baseDir + "'";
            throw new IllegalStateException(s);
        }
        File dataDir = new File(config.getDataDir());
        if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
            String s = "Can't make data dir '" + dataDir + "'";
            throw new IllegalStateException(s);
        }

        int port = config.getPort();
        Properties props = config.getConnProperties();
        RMIServerSocketFactory serverFactory = new AuthServerSocketFactory(props);
        RMIClientSocketFactory clientFactory = new AuthSocketFactory();
        config.setRMIServerSocketFactory(serverFactory);
        config.setRMIClientSocketFactory(clientFactory);
        try {
            log.fine(() -> String.format("%s: create a registry", currentThread().getName()));
            this.registry = LocateRegistry.createRegistry(port, clientFactory, serverFactory);
            Remote driver = new RMIDriverImpl(config);
            log.fine(() -> String.format("%s: rebind the driver", currentThread().getName()));
            this.registry.rebind(NAME, driver);
            String f = "%s: %s v%s listen on %d";
            log.info(() -> String.format(f, currentThread().getName(), NAME, VERSION, port));
        } catch (RemoteException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void stop() throws IllegalStateException {
        Registry registry = this.registry;
        if (registry != null) {
            try {
                registry.unbind(NAME);
            } catch (RemoteException e) {
                throw new IllegalStateException(e);
            } catch (NotBoundException ignore) {
                // Ignore
            }
        }
        this.stopped = true;
    }

    @Override
    public boolean isStopped() {
        return this.stopped;
    }

}
