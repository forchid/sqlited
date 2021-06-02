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

import org.junit.Test;
import static junit.framework.TestCase.*;

import java.util.ArrayList;
import java.util.List;

public class AutoGenKeysIteratorTest {

    @Test
    public void test() {
        List<Long[]> ranges = new ArrayList<>();
        AutoGenKeysIterator itr;
        Long[] row;

        itr = new AutoGenKeysIterator(ranges);
        assertFalse(itr.hasNext());

        ranges.add(new Long[]{1L, 1L});
        itr = new AutoGenKeysIterator(ranges);
        assertTrue(itr.hasNext());
        row = itr.next();
        assertEquals(1, row.length);
        assertEquals(1L, (long)row[0]);
        assertEquals(1L, (long)itr.get()[0]);
        assertFalse(itr.hasNext());

        ranges.add(new Long[]{3L, 4L});
        itr = new AutoGenKeysIterator(ranges);
        // 1
        assertTrue(itr.hasNext());
        row = itr.next();
        assertEquals(1, row.length);
        assertEquals(1L, (long)row[0]);
        assertEquals(1L, (long)itr.get()[0]);
        assertTrue(itr.hasNext());
        // 3
        row = itr.next();
        assertEquals(1, row.length);
        assertEquals(3L, (long)row[0]);
        assertEquals(3L, (long)itr.get()[0]);
        assertTrue(itr.hasNext());
        // 4
        row = itr.next();
        assertEquals(1, row.length);
        assertEquals(4L, (long)row[0]);
        assertEquals(4L, (long)itr.get()[0]);
        assertFalse(itr.hasNext());
    }

}
