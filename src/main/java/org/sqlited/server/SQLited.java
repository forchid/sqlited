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

/**
 * The SQLite daemon.
 * @since 2020-05-15
 */
public class SQLited {

    public static void main(String[] args) {
        try {
            Config.run(args, a -> usage(0));
        } catch (Exception e) {
            System.err.println("[ERROR] " + e);
            usage(1);
        }
    }

    private static void usage(int exitCode) {
        Config def = new Config();
        String usage = "Usage: java SQLited [OPTIONS]%n" +
                "OPTIONS: %n" +
                "  --help|-?      Show this help message%n" +
                "  --protocol|-x  <protocol>  Client/server protocol, default '%s'%n" +
                "  --host|-h      <host>      Which host the server listen on, default '%s'%n" +
                "  --port|-P      <port>      Which port the server listen on, default %d%n" +
                "  --user|-u      <username>  Which user login the server, default '%s'%n" +
                "  --password|-p  [password]  The user password%n" +
                "  --base-dir|-B  [base-dir]  The server base directory, default '%s'%n" +
                "  --data-dir|-D  [data-dir]  The server data directory, default '%s'%n";
        System.out.printf(usage, def.protocol, def.host, def.port, def.user, def.baseDir, def.dataDir);
        System.exit(exitCode);
    }

}
