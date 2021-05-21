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

import org.junit.Test;
import org.sqlite.server.SQLited;
import org.sqlite.util.logging.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import static junit.framework.TestCase.*;

import static java.sql.DriverManager.*;

public class ConnectTest extends BaseTest {
    static final Logger log = LoggerFactory.getLogger(ConnectTest.class);

    @Test
    public void testRestart() throws Exception {
        int n = 100;
        int p = 3525;

        for (int i = 0; i < n; ++i) {
            String u = "test";
            final SQLited d = new SQLited();
            d.parse(new String[]{ "-P", p + "", "-D", "temp", "-u", u });
            d.start();
            String url = getUrl("jdbc:sqlited://:" + p + "/test");
            Properties info = new Properties();
            info.put("user", u);
            try (Connection c = DriverManager.getConnection(url, info);
                 Statement s = c.createStatement()) {
                String sql = "select current_timestamp as cts";
                ResultSet rs = s.executeQuery(sql);
                assertTrue(rs.next());
                assertNotNull(rs.getString(1));
                assertFalse(rs.next());
                rs.close();
            }
            d.stop();
        }
    }

    @Test
    public void testPerf() throws Exception {
        doTestPerf(10);
        doTestPerf(50);
        doTestPerf(100);
        doTestPerf(10, 2);
        doTestPerf(50, 4);
        doTestPerf(1, 10);
        doTestPerf(1, 250);
        doTestPerf(100, 100);
    }

    @Test
    public void testPerfMySQL() {
        testMySQL(() -> {
            String url = getMySQLUrl();
            doTestPerf(10, url);
            doTestPerf(50, url);
            doTestPerf(100, url);
            doTestPerf(10, 2, url);
            doTestPerf(50, 4, url);
            doTestPerf(1, 10, url);
            doTestPerf(1, 250, url);
            doTestPerf(100, 100, url);
        });
    }

    private void doTestPerf(int conns) throws Exception {
        doTestPerf(conns, 1, null);
    }

    private void doTestPerf(int conns, String url) throws Exception {
        doTestPerf(conns, 1, url);
    }

    private void doTestPerf(int conns, int threads) throws Exception {
        doTestPerf(conns, threads, null);
    }

    private void doTestPerf(int conns, int threads, String url) throws Exception {
        Callable<?> callable = () -> {
            String cStr = url == null? "jdbc:sqlited:test": url;
            try (Connection c = DriverManager.getConnection(cStr, "root", password);
                Statement s = c.createStatement()){
                ResultSet rs = s.executeQuery("select current_timestamp cts");
                assertTrue(rs.next());
                assertNotNull(rs.getString("cts"));
                rs.close();
            }
            return null;
        };

        log.info(() -> String.format("conns %d, threads %s", conns, threads));
        if (conns <= 1) {
            for (int i = 0; i < conns; ++i) {
                callable.call();
            }
        } else {
            ExecutorService executors = getExecutors("testPerf", threads);
            try {
                List<Future<?>> futures = new ArrayList<>(threads);
                Callable<?> mulCall = () -> {
                    for (int i = 0; i < conns; ++i) {
                        callable.call();
                    }
                    return null;
                };
                for (int i = 0; i < threads; ++i) {
                    Future<?> f = executors.submit(mulCall);
                    futures.add(f);
                }
                for (Future<?> f: futures) {
                    f.get();
                }
            } finally {
                executors.shutdown();
            }
        }
        log.info("OK");
    }

    @Test
    public void testConnect() throws Exception {
        // Incorrect user or password test-1
        try {
            doTestConnect("jdbc:sqlited:?user=test&password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:?password=" + password + "x");
            fail();
        } catch (SQLException e) {
            // OK
        }

        // temp database
        doTestConnect("jdbc:sqlited:?password=" + password);
        doTestConnect("jdbc:sqlited:?journal_mode=wal&password=" + password);
        // memory database
        doTestConnect("jdbc:sqlited::memory:?password=" + password);
        doTestConnect("jdbc:sqlited::memory:?journal_mode=wal&password=" + password);
        // file database
        doTestConnect("jdbc:sqlited:test?password=" + password);
        doTestConnect("jdbc:sqlited:test?journal_mode=wal&password=" + password);
        doTestConnect("jdbc:sqlited:///test?readTimeout=3000&password=" + password);
        doTestConnect("jdbc:sqlited:///test?journal_mode=wal&password=" + password);
        doTestConnect("jdbc:sqlited:///test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited://localhost/test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited://localhost:3515/test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited:rmi:?password=" + password);
        doTestConnect("jdbc:sqlited:rmi:?journal_mode=wal&password=" + password);
        doTestConnect("jdbc:sqlited:rmi:test?password=" + password);
        doTestConnect("jdbc:sqlited:rmi:test?journal_mode=wal&password=" + password);
        doTestConnect("jdbc:sqlited:rmi:///test?loginTimeout=5000&password=" + password);
        doTestConnect("jdbc:sqlited:rmi:///test?connectTimeout=10000&journal_mode=wal&password=" + password);
        doTestConnect("jdbc:sqlited:rmi:///test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited:rmi://localhost/test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited:rmi://localhost:3515/test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited:rmi://:3515/test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited:rmi://localhost:/test?journal_mode=wal&foreign_keys=true&password=" + password);
        doTestConnect("jdbc:sqlited:rmi://:/test?journal_mode=wal&foreign_keys=true&password=" + password);

        // Multi-thread test
        doTestConnect("jdbc:sqlited:///test?password=" + password, 2);
        doTestConnect("jdbc:sqlited:///test?password=" + password, 5);
        doTestConnect("jdbc:sqlited:///test?password=" + password, 10);
        doTestConnect("jdbc:sqlited:///test?password=" + password, 50);
        doTestConnect("jdbc:sqlited:///test?password=" + password, 150);

        // Exception test
        try {
            doTestConnect("jdbc:sqlited:///test?a&password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited://test?password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:/test?password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:test?a&password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:?a&password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }

        // Incorrect port test
        try {
            doTestConnect("jdbc:sqlited://:3516/?user=test&password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited://localhost:3516/?password=" + password + "x");
            fail();
        } catch (SQLException e) {
            // OK
        }

        // Incorrect user or password test-2
        try {
            doTestConnect("jdbc:sqlited:?user=sa&password=" + password);
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:?password=" + password + "x");
            fail();
        } catch (SQLException e) {
            // OK
        }
    }

    void doTestConnect(String url) throws Exception {
        doTestConnect(url, 1);
    }

    void doTestConnect(String url, int threads) throws Exception {
        Callable<Void> callable = () -> {
            try (Connection c = getConnection(url);
                 Statement s = c.createStatement()) {
                ResultSet rs = s.executeQuery("select 1");
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("1"));
                rs.close();
                s.executeUpdate(TBL_ACCOUNT_DDL);
            }
            return null;
        };

        if (threads <= 1) {
            callable.call();
        } else {
            execute(callable, threads, "testConnect");
        }
    }

}
