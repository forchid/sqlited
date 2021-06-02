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

package org.sqlited.io;

import org.junit.Test;
import static junit.framework.TestCase.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class TransferTest {

    @Test
    public void testInt() throws Exception {
        for (int i = -2560; i < 2570; ++i) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = new InputStream() {
                byte[] buf;
                int i = 0;

                @Override
                public int read() throws IOException {
                    if (this.buf == null) {
                        this.buf = out.toByteArray();
                    }
                    return this.buf[i++] & 0xff;
                }
            };
            Transfer ch = new Transfer(in, out, 8);
            try {
                int j = ch.writeInt(i).flush().readInt();
                assertEquals(i + " != " + j, i, j);
            } catch (IOException e) {
                System.err.println("i = " + i);
                throw e;
            }
        }
    }

}
