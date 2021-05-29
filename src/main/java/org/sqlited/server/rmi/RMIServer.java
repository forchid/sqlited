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

package org.sqlited.server.rmi;

import org.sqlited.rmi.AuthServerSocketFactory;
import org.sqlited.rmi.AuthSocketFactory;
import org.sqlited.server.Config;
import org.sqlited.server.Server;
import org.sqlited.server.rmi.impl.RMIDriverImpl;
import org.sqlited.server.rmi.util.ROUtils;
import org.sqlited.util.IOUtils;
import org.sqlited.util.logging.LoggerFactory;

import static java.lang.Thread.*;
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
    protected final String name;
    protected volatile Registry registry;
    private volatile AuthServerSocketFactory serverSocketFactory;
    private volatile Remote driver;
    private volatile boolean stopped;
    private volatile boolean inited;

    public RMIServer(Config config) {
        this.config = config;
        this.name = getName();
    }

    @Override
    public void init() throws IllegalStateException {
        if (this.stopped) {
            throw new IllegalStateException("Server stopped");
        }
        if (this.inited) {
            return;
        }

        Config config = this.config.init();
        int port = config.getPort();
        Properties props = config.getConnProperties();
        this.serverSocketFactory = new AuthServerSocketFactory(props);
        RMIClientSocketFactory clientFactory = new AuthSocketFactory();
        try {
            log.fine(() -> String.format("%s: create a registry", currentThread().getName()));
            this.registry = LocateRegistry.createRegistry(port, clientFactory, this.serverSocketFactory);
            this.driver = new RMIDriverImpl(config, clientFactory, this.serverSocketFactory);
            log.fine(() -> String.format("%s: rebind the driver", currentThread().getName()));
            this.registry.rebind(NAME, this.driver);
            String f = "%s: %s v%s listen on %d";
            log.info(() -> String.format(f, currentThread().getName(), this, VERSION, port));
            this.inited = true;
        } catch (RemoteException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public RMIServer start() throws IllegalStateException {
        init();
        return this;
    }

    @Override
    public void stop() throws IllegalStateException {
        Registry registry = this.registry;
        if (registry == null) {
            return;
        }

        try {
            IOUtils.close(this.serverSocketFactory);
            ROUtils.unbind(registry, NAME);
            ROUtils.unexport(this.driver);
            ROUtils.unexport(registry);
            this.registry = null;
        } finally {
            this.stopped = true;
        }
    }

    @Override
    public boolean isStopped() {
        return this.stopped;
    }

    @Override
    public Config getConfig() {
        return this.config;
    }

    @Override
    public String toString() {
        return this.name;
    }

    @Override
    public void run() {
        start();
    }

}
