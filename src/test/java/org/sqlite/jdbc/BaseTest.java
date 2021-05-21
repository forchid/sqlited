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

package org.sqlite.jdbc;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.sqlite.server.SQLited;
import org.sqlite.util.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class BaseTest {
    static final Logger log = LoggerFactory.getLogger(BaseTest.class);
    protected static final String password = "123456";

    protected static final String TBL_ACCOUNT_DDL =
            "create table if not exists account(" +
                    "id int not null primary key, " +
                    "name varchar(20) not null, " +
                    "balance decimal(12,1) not null default 0," +
                    "create_at datetime)";

    private static SQLited intruder;
    protected SQLited server;

    @BeforeClass
    public static void setup() {
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
    }

    @After
    public void destroy() {
        this.server.stop();
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
