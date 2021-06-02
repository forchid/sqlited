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
import org.sqlited.util.PropsUtils;
import org.sqlited.util.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class Config implements Cloneable {
    static final Logger log = LoggerFactory.getLogger(Config.class);

    public static final int PROCESSORS = Runtime.getRuntime().availableProcessors();
    private static final Properties DEFAULT = new Properties();
    private static final String CONFIG_FILE = "config.properties";

    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream in = loader.getResourceAsStream(CONFIG_FILE)) {
            String f = "Load config file '%s' from classpath";
            log.info(() -> String.format(f, CONFIG_FILE));
            if (in != null) {
                DEFAULT.load(in);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    String protocol = DEFAULT.getProperty("protocol", "tcp");
    String host = DEFAULT.getProperty("host", "localhost");
    int port = Integer.decode(DEFAULT.getProperty("port", "3525"));
    String user = DEFAULT.getProperty("user", "root");
    String password = DEFAULT.getProperty("password");

    String baseDir = DEFAULT.getProperty("baseDir", System.getProperty("user.dir"));
    String dataDir = DEFAULT.getProperty("dataDir", this.baseDir + File.separator + "data");

    int loginTimeout = Integer.decode(DEFAULT.getProperty("loginTimeout", "5000"));
    int readTimeout = Integer.decode(DEFAULT.getProperty("readTimeout", "1800000"));
    int tcpWorkPool = Integer.decode(DEFAULT.getProperty("tcp.workPool", "520"));
    int userMaxLength = Integer.decode(DEFAULT.getProperty("userMaxLength", "64"));
    int maxBufferSize = Integer.decode(DEFAULT.getProperty("maxBufferSize", "16777216"));

    protected Config() {

    }

    public static Server start(String[] args)
            throws IllegalArgumentException, IllegalStateException {
        return parse(args).start();
    }

    public static Server start(String[] args, Usage usage)
            throws IllegalArgumentException, IllegalStateException {
        return parse(args, usage).start();
    }

    public static void run(String[] args)
            throws IllegalArgumentException, IllegalStateException {
        parse(args).run();
    }

    public static void run(String[] args, Usage usage)
            throws IllegalArgumentException, IllegalStateException {
        parse(args, usage).run();
    }

    public static Server parse(String[] args)
            throws IllegalArgumentException {
        return parse(args, null);
    }

    public static Server parse(String[] args, Usage usage)
            throws IllegalArgumentException {
        int n = args.length;
        Config config = new Config();

        for (int i = 0; i < n; ++i) {
            String arg = args[i];
            if ("--protocol".equals(arg) || "-x".equals(arg)) {
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
                if (usage != null) usage.help(args);
            } else {
                throw new IllegalArgumentException("Unknown arg: " + arg);
            }
        }
        String dataDir = config.dataDir;
        if (!dataDir.startsWith(File.separator)) {
            config.dataDir = config.baseDir + File.separator + dataDir;
        }

        String protocol = config.protocol;
        Server server;
        switch (protocol) {
            case "tcp":
                server = new TcpServer(config);
                break;
            case "rmi":
                server = new RMIServer(config);
                break;
            default:
                String s = "Unknown protocol: " + protocol;
                throw new IllegalArgumentException(s);
        }

        return server;
    }

    public Config init() throws IllegalStateException {
        File baseDir = new File(getBaseDir());
        if (!baseDir.isDirectory() && !baseDir.mkdirs()) {
            String s = "Can't make base dir '" + baseDir + "'";
            throw new IllegalStateException(s);
        }
        File dataDir = new File(getDataDir());
        if (!dataDir.isDirectory() && !dataDir.mkdirs()) {
            String s = "Can't make data dir '" + dataDir + "'";
            throw new IllegalStateException(s);
        }

        return this;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public String getDataDir() {
        return dataDir;
    }

    public int getLoginTimeout() {
        return this.loginTimeout;
    }

    public int getReadTimeout() {
        return this.readTimeout;
    }

    public int getTcpWorkPool() {
        return this.tcpWorkPool;
    }

    public int getMaxBufferSize() {
        return this.maxBufferSize;
    }

    public Properties getConnProperties() {
        final Properties props = new Properties();

        props.setProperty("host", this.host);
        props.setProperty("port", this.port + "");
        props.setProperty("user", this.user);
        PropsUtils.setNullSafe(props, "password", this.password);
        props.setProperty("loginTimeout", this.loginTimeout + "");
        props.setProperty("readTimeout", this.readTimeout + "");
        props.setProperty("userMaxLength", this.userMaxLength + "");
        props.setProperty("maxBufferSize", this.maxBufferSize + "");

        return props;
    }

}
