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

package org.sqlited.result;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class ResultSetMetaData implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String[] names;
    protected final boolean[][] columnMetas;
    protected final String[] columnTypeNames;
    protected final int[] columnTypes;
    protected final int[] scales;

    protected transient Map<String, Integer> nameToIndex;

    public ResultSetMetaData(String[] names, boolean[][] columnMetas,
                                String[] columnTypeNames, int[] columnTypes,
                                int[] scales) {
        this.names = names;
        this.columnMetas = columnMetas;
        this.columnTypeNames = columnTypeNames;
        this.columnTypes = columnTypes;
        this.scales = scales;
    }

    public int findColumn(String name) throws SQLException {
        Integer column = findColumnInCache(name);
        if (column != null) {
            return column;
        }

        String[] names = this.names;
        for (int i = 0; i < names.length; i++) {
            if (name.equalsIgnoreCase(names[i])) {
                return addColumnToCache(name, i + 1);
            }
        }

        throw new SQLException("No such column '"+name+"'");
    }

    private int addColumnToCache(String name, int column) {
        Map<String, Integer> cache = this.nameToIndex;

        if (cache == null) {
            cache = this.nameToIndex = new HashMap<>();
        }
        cache.put(name, column);

        return column;
    }

    private Integer findColumnInCache(String name) {
        Map<String, Integer> cache = this.nameToIndex;

        if (cache == null) {
            return null;
        } else {
            return cache.get(name);
        }
    }

    public int getColumnCount() throws SQLException {
        return this.columnMetas.length;
    }

    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    public String getColumnName(int column) throws SQLException {
        return this.names[column - 1];
    }

    public int getColumnType(int column) throws SQLException {
        return this.columnTypes[column - 1];
    }

    public String getColumnTypeName(int column) throws SQLException {
        return this.columnTypeNames[column - 1];
    }

    public int getColumnDisplaySize(int column) throws SQLException {
        return Integer.MAX_VALUE;
    }

    public int getScale(int column) throws SQLException {
        return this.scales[column - 1];
    }

}
