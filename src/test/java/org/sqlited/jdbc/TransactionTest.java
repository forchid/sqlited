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

import org.junit.Before;
import org.junit.Test;
import static junit.framework.TestCase.*;

import java.sql.*;

public class TransactionTest extends BaseTest {

    @Before
    public void prepare() throws Exception {
        try (Connection c = getTestConn();
             Statement s = c.createStatement()) {
            s.executeUpdate("drop table if exists account");
            s.executeUpdate("drop table if exists test_readonly_a");
            s.executeUpdate("drop table if exists test_readonly_b");
            s.executeUpdate(TBL_ACCOUNT_DDL);
        }
    }

    @Test
    public void testMultiConns() throws Exception {
        doTestMultiConns(getTestUrl());
    }

    @Test
    public void testMultiConnsRMI() throws Exception {
        doTestMultiConns(getRMIUrl());
    }

    @Test
    public void testMultiConnsSavepoint() throws Exception {
        doTestMultiConns(getTestUrl(), true);
    }

    @Test
    public void testMultiConnsSavepointRMI() throws Exception {
        doTestMultiConns(getRMIUrl(), true);
    }

    @Test
    public void testReadOnly() throws Exception {
        String url = getTestUrl();
        doTestReadOnly(url);
    }

    @Test
    public void testReadOnlyRMI() throws Exception {
        String url = getRMIUrl();
        doTestReadOnly(url);
    }

    @Test
    public void testIsolation() throws Exception {
        String url = getTestUrl();
        doTestIsolation(url);
    }

    @Test
    public void testIsolationRMI() throws Exception {
        String url = getRMIUrl();
        doTestIsolation(url);
    }

    @Test
    public void testHoldability() throws Exception {
        String url = getTestUrl();
        doTestHoldability(url);
    }

    @Test
    public void testHoldabilityRMI() throws Exception {
        String url = getRMIUrl();
        doTestHoldability(url);
    }

    void doTestHoldability(String url) throws Exception {
        int conns = 10, times = 10;

        for (int c = 0; c < conns; c++) {
            try (Connection conn = getConn(url)) {
                for (int t = 0; t < times; ++t) {
                    int hold = conn.getHoldability();
                    conn.setHoldability(hold);
                    assertEquals(hold, conn.getHoldability());

                    int[] holds = {ResultSet.HOLD_CURSORS_OVER_COMMIT,
                            ResultSet.CLOSE_CURSORS_AT_COMMIT};
                    for (int i = 0; i < holds.length; ++i) {
                        hold = holds[i];
                        try {
                            conn.setHoldability(hold);
                            assertEquals(hold, conn.getHoldability());
                            if (i != 1) {
                                fail();
                            }
                        } catch (SQLException e) {
                            if (i == 1) {
                                throw e;
                            }
                        }
                    }
                }
            }
        }
    }

    void doTestIsolation(String url) throws Exception {
        int conns = 10, times = 10;

        for (int c = 0; c < conns; c++) {
            try (Connection conn = getConn(url)) {
                for (int t = 0; t < times; ++t) {
                    int level = conn.getTransactionIsolation();
                    conn.setTransactionIsolation(level);
                    assertEquals(level, conn.getTransactionIsolation());

                    int[] levels = {Connection.TRANSACTION_NONE,
                            Connection.TRANSACTION_READ_UNCOMMITTED,
                            Connection.TRANSACTION_READ_COMMITTED,
                            Connection.TRANSACTION_REPEATABLE_READ,
                            Connection.TRANSACTION_SERIALIZABLE};
                    for (int i = 0; i < levels.length; ++i) {
                        level = levels[i];
                        try {
                            conn.setTransactionIsolation(level);
                            assertEquals(level, conn.getTransactionIsolation());
                            if (i != 1 && i != 4) {
                                fail();
                            }
                        } catch (SQLException e) {
                            if (i == 1 || i == 4) {
                                throw e;
                            }
                        }
                    }
                }
            }
        }
    }

    void doTestReadOnly(String url) throws Exception {
        doTestReadOnly(url, 10, false);
        doTestReadOnly(url, 10, true);
    }

    void doTestReadOnly(String url, int n, boolean testTx) throws Exception {
        String selectSql = "select count(*) from account";
        String createSql = "create table test_readonly_a(id integer primary key)";
        String dropSql   = "drop table test_readonly_b";
        String insertSql = "insert into account(id, name, balance)values(1, 'Jim', 1000)";
        String updateSql = "update account set balance = balance + 1000 where id = 1";
        String deleteSql = "delete from account where id = 1";
        String[] sqlList = {selectSql, createSql, dropSql, insertSql, updateSql, deleteSql};

        try (Connection conn = getConn(url);
             Statement stmt = conn.createStatement()) {
            for (int i = 0; i < n; ++i) {
                conn.setAutoCommit(true);
                boolean ro = conn.isReadOnly();
                conn.setReadOnly(ro);
                assertEquals(ro, conn.isReadOnly());
                stmt.executeUpdate("drop table if exists test_readonly_a");
                stmt.executeUpdate("create table test_readonly_b(id integer primary key)");

                // test readonly
                conn.setReadOnly(true);
                assertTrue(conn.isReadOnly());
                if (testTx) conn.setAutoCommit(false);
                for (int j = 0; j < sqlList.length; ++j) {
                    String sql = sqlList[j];
                    if (j == 0) {
                        try (ResultSet rs = stmt.executeQuery(sql)) {
                            assertTrue(rs.next());
                            assertEquals(0, rs.getInt(1));
                        }
                    } else {
                        try {
                            stmt.executeUpdate(sql);
                            fail();
                        } catch (SQLException e) {
                            if (8/*vendorCode: readonly*/ != e.getErrorCode()) {
                                throw e;
                            }
                            // OK
                        }
                    }
                }
                if (testTx) conn.rollback();

                // test read/write
                conn.setReadOnly(false);
                assertFalse(conn.isReadOnly());
                if (testTx) conn.setAutoCommit(false);
                for (int j = 0; j < sqlList.length; ++j) {
                    String sql = sqlList[j];
                    if (j == 0) {
                        try (ResultSet rs = stmt.executeQuery(sql)) {
                            assertTrue(rs.next());
                            assertEquals(0, rs.getInt(1));
                        }
                    } else {
                        stmt.executeUpdate(sql);
                    }
                }
                if (testTx) conn.commit();
            }
        }
    }

    void doTestMultiConns(String url) throws Exception {
        doTestMultiConns(url, false);
    }

    void doTestMultiConns(String url, boolean testSp) throws Exception {
        int n = 1000;
        for (int i = 0; i < n; ++i) {
            try (Connection c = getConn(url);
                 Statement s = c.createStatement()) {
                doTest(c, s, testSp);
            }
        }
    }

    @Test
    public void testSingleConn() throws Exception {
        doTestSingleConn(getTestConn());
    }

    @Test
    public void testSingleConnRMI() throws Exception {
        doTestSingleConn(getRMIConn());
    }

    @Test
    public void testSingleConnSavepoint() throws Exception {
        doTestSingleConn(getTestConn(), true);
    }

    @Test
    public void testSingleConnSavepointRMI() throws Exception {
        doTestSingleConn(getRMIConn(), true);
    }

    void doTestSingleConn(Connection conn) throws Exception {
        doTestSingleConn(conn, false);
    }

    void doTestSingleConn(Connection conn, boolean testSp) throws Exception {
        int n = 1000;
        try (Connection c = conn;
             Statement s = c.createStatement()) {
            for (int i = 0; i < n; ++i) {
                doTest(c, s, testSp);
            }
        }
    }

    void doTest(Connection c, Statement s, boolean testSp) throws SQLException {
        c.setAutoCommit(false);
        count(s, 0);
        int n = s.executeUpdate("insert into account(id, name, balance) values(1, 'Tom', 100000)");
        assertEquals(1, n);
        count(s, 1);
        n = s.executeUpdate("insert into account(id, name, balance) values(2, 'Ken', 150000)");
        assertEquals(1, n);
        count(s, 2);
        n = s.executeUpdate("update account set balance = balance + 1000 where id = 1");
        assertEquals(1, n);
        count(s, 2);
        if (testSp) {
            Savepoint sp = c.setSavepoint("'sp-1'");
            n = s.executeUpdate("insert into account(id, name, balance) values(3, 'John', 250000)");
            assertEquals(1, n);
            count(s, 3);
            c.rollback(sp);
            count(s, 2);
        }
        c.commit();

        count(s, 2);
        n = s.executeUpdate("insert into account(id, name, balance) values(3, 'John', 250000)");
        assertEquals(1, n);
        count(s, 3);
        n = s.executeUpdate("update account set balance = balance - 1000 where id = 1");
        assertEquals(1, n);
        count(s, 3);
        n = s.executeUpdate("update account set balance = balance - 1000 where id = 4");
        assertEquals(0, n);
        count(s, 3);
        n = s.executeUpdate("delete from account where id = 1");
        assertEquals(1, n);
        count(s, 2);
        if (testSp) {
            Savepoint sp = c.setSavepoint("sp1");
            n = s.executeUpdate("delete from account where id = 2");
            assertEquals(1, n);
            count(s, 1);
            c.releaseSavepoint(sp);
            count(s, 1);
        }
        c.rollback();
        count(s, 2);

        n = s.executeUpdate("delete from account");
        assertEquals(2, n);
        count(s, 0);
        if (testSp) {
            Savepoint sp = c.setSavepoint();
            n = s.executeUpdate("delete from account where id = 1");
            assertEquals(0, n);
            count(s, 0);
            c.rollback(sp);
            count(s, 0);
        }
        c.commit();
        count(s, 0);
    }

    static void count(Statement s, int expected) throws SQLException {
        ResultSet rs = s.executeQuery("select count(*) from account");
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

}
