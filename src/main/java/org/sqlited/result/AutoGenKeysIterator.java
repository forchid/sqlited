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

import java.util.List;

public class AutoGenKeysIterator extends RowIterator {
    private static final long serialVersionUID = 1L;

    protected final List<Long[]> keyRanges;
    protected transient long key;

    public AutoGenKeysIterator(List<Long[]> keyRanges, ResultSetMetaData metaData) {
        super(null, true, metaData);
        this.keyRanges = keyRanges;
    }

    public AutoGenKeysIterator(List<Long[]> keyRanges) {
        this(keyRanges, ResultSetMetaData.AUTO_GEN_KEYS_META);
    }

    @Override
    public boolean hasNext() {
        List<Long[]> keyRanges = this.keyRanges;
        long key = this.key;
        int index = this.index;

        if (key != 0) {
            long raMax = keyRanges.get(index)[1];
            if (key < raMax) {
                return true;
            }
        }
        return (index + 1 < keyRanges.size());
    }

    @Override
    public Long[] next() {
        List<Long[]> keyRanges = this.keyRanges;
        long key = this.key;
        Long[] range;

        if (key == 0) ++this.index;
        range = keyRanges.get(this.index);
        if (key < range[0]) {
            key = range[0];
        } else if (key < range[1]) {
            key += 1;
        } else {
            range = keyRanges.get(++this.index);
            key = range[0];
        }

        return new Long[]{ this.key = key };
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("remove()");
    }

    @Override
    public Long[] get() {
        long key = this.key;
        if (key == 0) {
            String s = "next() not called";
            throw new IllegalStateException(s);
        } else {
            return new Long[] { key };
        }
    }

    @Override
    public AutoGenKeysIterator reset() {
        super.reset();
        this.key = 0;
        return this;
    }

}
