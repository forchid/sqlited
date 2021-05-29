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

import java.io.Serializable;
import java.sql.SQLException;
import java.sql.Savepoint;

public class JdbcSavepoint implements Savepoint, Serializable {
    private static final long serialVersionUID = 1L;

    protected final int id;
    protected final String name;

    public JdbcSavepoint(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public JdbcSavepoint(Savepoint source) throws SQLException {
        this.id = source.getSavepointId();
        this.name = source.getSavepointName();
    }

    @Override
    public int getSavepointId() throws SQLException {
        return this.id;
    }

    @Override
    public String getSavepointName() throws SQLException {
        return this.name;
    }

    @Override
    public String toString() {
        return this.name;
    }

}
