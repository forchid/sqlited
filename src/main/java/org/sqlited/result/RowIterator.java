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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class RowIterator implements Iterator<Object[]>, Serializable {
    private static final long serialVersionUID = 1L;

    protected final List<Object[]> rows;
    protected final boolean last;
    protected final ResultSetMetaData metaData;
    // Row cursor
    protected transient int index = -1;

    public RowIterator(List<Object[]> rows, boolean last,
                       ResultSetMetaData metaData) {
        this.rows = rows;
        this.last = last;
        this.metaData = metaData;
    }

    public static RowIterator empty(ResultSetMetaData metaData) {
        List<Object[]> rows = Collections.emptyList();
        return new RowIterator(rows, true, metaData);
    }

    @Override
    public boolean hasNext() {
        return this.index + 1 < this.rows.size();
    }

    @Override
    public Object[] next() {
        return this.rows.get(++this.index);
    }

    @Override
    public void remove() {
        this.rows.remove(this.index);
    }

    @Override
    public void forEachRemaining(Consumer<? super Object[]> consumer) {
        while (hasNext()) {
            Object[] row = next();
            consumer.accept(row);
        }
    }

    public RowIterator reset() {
        this.index = -1;
        return this;
    }

    public Object[] get() {
        return this.rows.get(this.index);
    }

    public boolean isLast() {
        return this.last;
    }

    public ResultSetMetaData getMetaData() {
        return this.metaData;
    }

}
