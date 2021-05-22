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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TransactionTest extends BaseTest {

    @Before
    public void prepare() throws Exception {
        try (Connection c = getTestConn();
             Statement s = c.createStatement()) {
            s.executeUpdate("drop table if exists account");
            s.executeUpdate(TBL_ACCOUNT_DDL);
        }
    }

    @Test
    public void test() throws Exception {
        for (int i = 0; i < 100; ++i) {
            try (Connection c = getTestConn(); Statement s = c.createStatement()) {
                c.setAutoCommit(false);
                count(s, 0);
                s.executeUpdate("insert into account(id, name, balance) values(1, 'Tom', 100000)");
                count(s, 1);
                s.executeUpdate("insert into account(id, name, balance) values(2, 'Ken', 150000)");
                count(s, 2);
                s.executeUpdate("update account set balance = balance + 1000 where id = 1");
                count(s, 2);
                c.commit();

                count(s, 2);
                s.executeUpdate("insert into account(id, name, balance) values(3, 'John', 250000)");
                count(s, 3);
                s.executeUpdate("update account set balance = balance - 1000 where id = 1");
                count(s, 3);
                s.executeUpdate("delete from account where id = 1");
                count(s, 2);
                c.rollback();
                count(s, 2);

                s.executeUpdate("delete from account");
                count(s, 0);
                c.commit();
                count(s, 0);
            }
        }
    }

    static void count(Statement s, int expected) throws SQLException {
        ResultSet rs = s.executeQuery("select count(*) from account");
        assertTrue(rs.next());
        assertEquals(expected, rs.getInt(1));
        assertFalse(rs.next());
        rs.close();
    }

}
