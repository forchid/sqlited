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

public abstract class BaseTest {

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
        this.server = null;
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

}
