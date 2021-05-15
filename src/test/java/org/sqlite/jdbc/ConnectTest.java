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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import static junit.framework.TestCase.*;

import static java.sql.DriverManager.*;

public class ConnectTest {

    final SQLited server = new SQLited();
    {
        this.server.parse(new String[]{
                "-D", "temp"
        }).start();
    }

    @Test
    public void testConnect() throws Exception {
        doTestConnect("jdbc:sqlited:");
        doTestConnect("jdbc:sqlited:?journal_mode=wal");
        doTestConnect("jdbc:sqlited:test");
        doTestConnect("jdbc:sqlited:test?journal_mode=wal");
        doTestConnect("jdbc:sqlited:///test");
        doTestConnect("jdbc:sqlited:///test?journal_mode=wal");
        doTestConnect("jdbc:sqlited:///test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited://localhost/test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited://localhost:3515/test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited:rmi:");
        doTestConnect("jdbc:sqlited:rmi:?journal_mode=wal");
        doTestConnect("jdbc:sqlited:rmi:test");
        doTestConnect("jdbc:sqlited:rmi:test?journal_mode=wal");
        doTestConnect("jdbc:sqlited:rmi:///test");
        doTestConnect("jdbc:sqlited:rmi:///test?journal_mode=wal");
        doTestConnect("jdbc:sqlited:rmi:///test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited:rmi://localhost/test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited:rmi://localhost:3515/test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited:rmi://:3515/test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited:rmi://localhost:/test?journal_mode=wal&foreign_keys=true");
        doTestConnect("jdbc:sqlited:rmi://:/test?journal_mode=wal&foreign_keys=true");

        try {
            doTestConnect("jdbc:sqlited:///test?a");
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited://test");
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:/test");
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:test?a");
            fail();
        } catch (SQLException e) {
            // OK
        }
        try {
            doTestConnect("jdbc:sqlited:?a");
            fail();
        } catch (SQLException e) {
            // OK
        }
    }

    void doTestConnect(String url) throws Exception {
        try (Connection c = getConnection(url);
             Statement s = c.createStatement()) {
            ResultSet rs = s.executeQuery("select 1");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("1"));
            rs.close();
            s.executeUpdate("create table if not exists account(" +
                    "id int not null primary key, " +
                    "name varchar(20) not null, " +
                    "balance decimal(12,1) not null default 0)");
        }
    }

}
