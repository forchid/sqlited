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

package org.sqlited.server;

import org.sqlited.server.rmi.RMIServer;
import org.sqlited.server.tcp.TcpServer;

import java.io.File;

/**
 * The SQLite daemon.
 * @since 2020-05-15
 */
public class SQLited implements Server {

    final Config config = new Config();
    private volatile Server server;
    private volatile boolean stopped;

    public static void main(String[] args) {
        SQLited server = new SQLited();
        try {
            server.parse(args);
            server.start();
        } catch (Exception e) {
            System.err.println("[ERROR] " + e.getMessage());
            usage(1);
        }
    }

    private static void usage(int exitCode) {
        Config def = new Config();
        String usage = "Usage: java SQLited [OPTIONS]%n" +
                "OPTIONS: %n" +
                "  --help|-?      Show this help message%n" +
                "  --protocol     <protocol>  Client/server communication protocol, default '%s'%n" +
                "  --host|-h      <host>      Which host the server listen on, default '%s'%n" +
                "  --port|-P      <port>      Which port the server listen on, default %d%n" +
                "  --user|-u      <username>  Which user login the server, default '%s'%n" +
                "  --password|-p  [password]  The user password%n" +
                "  --base-dir     [base-dir]  The server base directory, default '%s'%n" +
                "  --data-dir     [data-dir]  The server data directory, default '%s'%n";
        System.out.printf(usage, def.protocol, def.host, def.port, def.user, def.baseDir, def.dataDir);
        System.exit(exitCode);
    }

    public void start() throws IllegalStateException {
        Config config = this.config;
        String protocol = config.protocol;

        switch (protocol) {
            case "tcp":
                this.server = new TcpServer(config);
                break;
            case "rmi":
                this.server = new RMIServer(config);
                break;
            default:
                throw new IllegalStateException("Unknown protocol: " + protocol);
        }
        this.server.start();
    }

    public SQLited parse(String[] args) throws IllegalArgumentException {
        int n = args.length;
        Config config = this.config;

        for (int i = 0; i < n; ++i) {
            String arg = args[i];
            if ("--protocol".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No protocol argv");
                }
                config.protocol = args[i];
            } else if ("--host".equals(arg) || "-h".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No host argv");
                }
                config.host = args[i];
            } else if ("--port".equals(arg) || "-P".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No port argv");
                }
                config.port = Integer.decode(args[i]);
            } else if ("--user".equals(arg) || "-u".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No user argv");
                }
                config.user = args[i];
            } else if ("--password".equals(arg) || "-p".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No password argv");
                }
                config.password = args[i];
            } else if ("--base-dir".equals(arg) || "-B".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No base-dir argv");
                }
                config.baseDir = args[i];
            } else if ("--data-dir".equals(arg) || "-D".equals(arg)) {
                if (++i >= n) {
                    throw new IllegalArgumentException("No data-dir argv");
                }
                config.dataDir = args[i];
            } else if ("--help".equals(arg) || "-?".equals(arg)) {
                usage(0);
            } else {
                throw new IllegalArgumentException("Unknown arg: " + arg);
            }
        }
        String dataDir = config.dataDir;
        if (!dataDir.startsWith(File.separator)) {
            config.dataDir = config.baseDir + File.separator + dataDir;
        }

        return this;
    }

    public boolean isStopped() {
        return this.stopped;
    }

    public void stop() throws IllegalStateException {
        Server server = this.server;
        if (server != null) {
            server.stop();
            this.stopped = true;
        }
    }

    @Override
    public Config getConfig() {
        return this.config.clone();
    }

    @Override
    public String toString() {
        return getName();
    }

}
