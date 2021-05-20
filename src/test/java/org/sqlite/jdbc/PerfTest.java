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

import static junit.framework.TestCase.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;

public class PerfTest extends BaseTest {

    protected void prepare() throws Exception {
        String url = getTestUrl();

        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.executeUpdate(TBL_ACCOUNT_DDL);
            s.executeUpdate("replace into account(id, name, balance) " +
                    "values(1, 'Tom', 5000000)");
        }
    }

    @Test
    public void test() throws Exception {
        prepare();
        doTest(1000);
        doTest(100,10);
    }

    void doTest(int times) throws Exception {
        doTest(times, 1);
    }

    void doTest(int times, int threads) throws Exception {
        Callable<?> callable = () -> {
            String url = getTestUrl();

            try (Connection c = DriverManager.getConnection(url);
                 Statement s = c.createStatement()) {
                for (int i = 0; i < times; ++i) {
                    ResultSet rs = s.executeQuery("select id, name, balance " +
                            "from account where id = 1");
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertEquals("Tom", rs.getString("name"));
                    assertEquals(5000000, rs.getInt("balance"));
                    assertFalse(rs.next());
                    rs.close();
                }
            }

            return null;
        };

        if (threads <= 1) {
            callable.call();
        } else {
            execute(callable, threads, "testConcur");
        }
    }

}
