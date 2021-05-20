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

public class PerfTest extends BaseTest {

    @Test
    public void test() throws Exception {
        String url = getUrl("jdbc:sqlited:///test",
                "password", password);
        try (Connection c = DriverManager.getConnection(url);
             Statement s = c.createStatement()) {
            s.executeUpdate(TBL_ACCOUNT_DDL);
            s.executeUpdate("replace into account(id, name, balance) " +
                    "values(1, 'Tom', 5000000)");
            for (int i = 0; i < 1000; ++i) {
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
    }

}
