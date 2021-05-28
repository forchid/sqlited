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

package org.sqlited.jdbc;

import org.h2.tools.Server;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.sqlited.server.SQLited;
import org.sqlited.util.logging.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseTest {
    static final Logger log = LoggerFactory.getLogger(BaseTest.class);
    protected static final String workDir = System.getProperty("user.dir");
    protected static final String baseDir = workDir + File.separator + "temp";
    protected static final String password = "123456";

    protected static final String TBL_ACCOUNT_DDL =
            "create table if not exists account(" +
                    "id int not null primary key, " +
                    "name varchar(20) not null, " +
                    "balance decimal(12,1) not null default 0," +
                    "create_at datetime)";

    private static SQLited intruder;
    protected SQLited server;
    protected SQLited rmiServer;

    @BeforeClass
    public static void setup() {
        log.info(() -> String.format("baseDir '%s'", baseDir));
        intruder = new SQLited();
        intruder.parse(new String[]{
                "-D", "temp", "-P", "3516"
        }).start();
    }

    @AfterClass
    public static void teardown() {
        intruder.stop();
    }

    @Before
    public void init() {
        this.server = new SQLited();
        this.server.parse(new String[]{
                "-D", "temp", "-p", password
        }).start();

        this.rmiServer = new SQLited();
        this.rmiServer.parse(new String[] {
                "-D", "temp", "-p", password,
                "--protocol", "rmi", "-P", "3515"
        });
        this.rmiServer.start();
    }

    @After
    public void destroy() {
        this.server.stop();
        this.rmiServer.stop();
    }

    public static void testMySQL(VoidCallable<?> test) {
        try {
            test.call();
        } catch (Exception e) {
            log.log(Level.INFO, "Skip MySQL test", e);
        }
    }

    public String getSQLiteUrl() {
        String dataDir = this.server.getConfig().getDataDir();
        return "jdbc:sqlite:" + dataDir +"/test";
    }

    public static String getMySQLUrl() {
        return "jdbc:mysql://localhost:3306/test?user=root&password=123456";
    }

    public static String getTestUrl() {
        return getUrl("jdbc:sqlited:///test", "password", password);
    }

    public static String getRMIUrl() {
        return getUrl("jdbc:sqlited:rmi:///test", "password", password);
    }

    public static String getH2Url() {
        return "jdbc:h2:tcp://localhost:9093/test;user=sa";
    }

    public static Connection getH2Conn() throws SQLException {
        String url = getH2Url();
        return DriverManager.getConnection(url);
    }

    public static Server startH2Server() throws Exception {
        Server server = Server.createTcpServer(
                "-tcpPort", "9093",
                "-ifNotExists",
                "-baseDir", getBaseDir());
        return server.start();
    }

    public static String getBaseDir() {
        return baseDir;
    }

    public static Connection getTestConn() throws SQLException {
        String url = getTestUrl();
        return DriverManager.getConnection(url);
    }

    public static String getUrl(String base, Object ... args) {
        StringBuilder sb = new StringBuilder(base);
        int n = args.length;

        if (n > 0) {
            if (base.indexOf('?') == -1) sb.append('?');
            else sb.append('&');
            int i = 0;
            while (i < n) {
                sb.append(args[i++]);
                if (i < n) {
                    sb.append('=').append(args[i++]);
                }
            }
        }

        return sb.toString();
    }

    public static ExecutorService getExecutors(String name, int threads) {
        AtomicInteger id = new AtomicInteger();
        return Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r);
            t.setName(name + "-" + id.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
    }

    public static void execute(Callable<?> callable, int threads)
            throws ExecutionException, InterruptedException {
        execute(callable, threads, "test");
    }

    public static void execute(Callable<?> callable, int threads, String poolName)
        throws ExecutionException, InterruptedException {
        ExecutorService executors = getExecutors(poolName, threads);
        try {
            List<Future<?>> futures = new ArrayList<>(threads);
            for (int i = 0; i < threads; ++i) {
                Future<?> f = executors.submit(callable);
                futures.add(f);
            }
            for (Future<?> f: futures) {
                f.get();
            }
        } finally {
            executors.shutdown();
        }
    }

}
