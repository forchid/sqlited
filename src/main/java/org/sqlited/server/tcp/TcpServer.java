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

package org.sqlited.server.tcp;

import org.sqlited.io.Transfer;
import org.sqlited.net.AuthServerSocketFactory;
import org.sqlited.server.Config;
import org.sqlited.server.Server;
import org.sqlited.server.tcp.impl.TcpConnection;
import org.sqlited.util.IOUtils;
import org.sqlited.util.logging.LoggerFactory;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.Thread.currentThread;

public class TcpServer implements Server {
    static final Logger log = LoggerFactory.getLogger(TcpServer.class);
    static final AtomicLong WORKER_ID = new AtomicLong();

    protected final String name;
    protected final Config config;
    private int nextConnId;
    private final Map<Integer, TcpConnection> connMap = new HashMap<>();

    protected volatile ServerSocket server;
    private volatile ThreadPoolExecutor workPool;
    private volatile boolean inited;
    private volatile boolean stopped;

    public TcpServer(Config config) {
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
        ServerSocketFactory socketFactory = new AuthServerSocketFactory(props);

        boolean failed = true;
        try {
            int poolSize = config.getTcpWorkPool();
            int corePool = Math.min(2 * (Config.PROCESSORS + 1), poolSize);
            this.server = socketFactory.createServerSocket(port);
            this.workPool = new ThreadPoolExecutor(corePool, poolSize,
                    120, TimeUnit.SECONDS, new SynchronousQueue<>(),
                    task -> {
                        long id = WORKER_ID.getAndIncrement();
                        String name = String.format("%s-worker-%s", NAME, id);
                        Thread worker = new Thread(task, name);
                        worker.setDaemon(true);
                        return worker;
                    });
            String f = "%s: %s v%s listen on %d";
            log.info(() -> String.format(f, currentThread().getName(), this, VERSION, port));
            this.inited = true;
            failed = false;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } finally {
            if (failed) IOUtils.close(this.server);
        }
    }

    @Override
    public TcpServer start() throws IllegalStateException {
        init();
        new Thread(this, this.name).start();
        return this;
    }

    @Override
    public void stop() throws IllegalStateException {
        this.stopped = true;
        ExecutorService workPool = this.workPool;
        if (workPool != null) workPool.shutdown();
        IOUtils.close(this.server);
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
    public void run() {
        init();
        try {
            ServerSocket server = this.server;
            while (!this.stopped) {
                Socket conn = server.accept();
                boolean failed = true;
                try {
                    handle(conn);
                    failed = false;
                } finally {
                    if (failed) IOUtils.close(conn);
                }
            }
            log.info(this + " stopped then exit");
        } catch (IOException e) {
            if (this.stopped) {
                log.info(this + " closed then exit");
            } else {
                String s = this + " crash";
                log.log(Level.WARNING, s, e);
            }
        } finally {
            stop();
        }
    }

    @Override
    public String toString() {
        return this.name;
    }

    protected void handle(Socket conn) {
        try {
            removeClosed();

            int id;
            do {
                id = this.nextConnId++;
                if (this.nextConnId < 0) this.nextConnId = 0;
                TcpConnection old = this.connMap.get(id);
                if (old == null) break;
            } while (true);

            Config config = getConfig();
            TcpConnection tc = new TcpConnection(id, conn, config);
            this.workPool.execute(tc);
            this.connMap.put(id, tc);
        } catch (RejectedExecutionException e) {
            try {
                Transfer ch = new Transfer(conn);
                String s = "Too many connections";
                ch.writeError(s, "08001");
            } catch (IOException ignore) {
                // Ignore
            } finally {
                IOUtils.close(conn);
            }
        }
    }

    protected void removeClosed() {
        Iterator<Map.Entry<Integer, TcpConnection>> i =
                this.connMap.entrySet().iterator();

        while (i.hasNext()) {
            TcpConnection tc = i.next().getValue();
            if (!tc.isOpen()) i.remove();
        }
    }

}
