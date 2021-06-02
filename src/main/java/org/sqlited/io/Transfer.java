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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;

public class Transfer implements Protocol {

    static final Charset CHARSET = StandardCharsets.UTF_8;
    static final int IO_SIZE = 4096;

    protected final InputStream in;
    protected final OutputStream out;
    protected final int maxBufferSize;
    // It is important for performance and protocol integrity to
    // read/write first a complete packet into buffer
    private final ByteBuffer inBuffer;
    private ByteBuffer outBuffer;

    public Transfer(InputStream in, OutputStream out, int maxBufferSize)
        throws IllegalArgumentException {
        if (maxBufferSize <= 0) {
            String s = "maxBufferSize " + maxBufferSize;
            throw new IllegalArgumentException(s);
        }
        int initSize = Math.min(IO_SIZE, maxBufferSize);
        this.in = in;
        this.out = out;
        this.maxBufferSize = maxBufferSize;
        this.inBuffer = allocate(initSize);
        this.inBuffer.flip();
        this.outBuffer = allocate(initSize);
    }

    public Transfer(Socket socket, int maxBufferSize) throws IOException {
        this(socket.getInputStream(), socket.getOutputStream(), maxBufferSize);
    }

    public boolean readBoolean() throws IOException {
        int i = readByte(true);
        return i == 0x01;
    }

    public Transfer writeBoolean(boolean b) throws IOException {
        return writeByte(b ? 0x01: 0x00);
    }

    public int readByte() throws IOException {
        ByteBuffer buf = this.inBuffer;

        if (buf.hasRemaining()) {
            return (buf.get() & 0xff);
        } else {
            byte[] a = buf.array();
            int i = this.in.read(a);
            if (i == -1) {
                buf.position(0).limit(0);
                return i;
            } else {
                buf.position(0).limit(i);
                return (buf.get() & 0xff);
            }
        }
    }

    public int readByte(boolean check) throws IOException {
        int i = readByte();
        if (check && i == -1) throw new EOFException();
        return i;
    }

    public Transfer writeByte(int i) throws IOException {
        ByteBuffer buf = ensureOutBuffer(1);
        buf.put((byte)i);
        return this;
    }

    protected ByteBuffer ensureOutBuffer(int n) throws IOException {
        ByteBuffer buf = this.outBuffer;

        if (buf.remaining() < n) {
            int cap = buf.capacity() + Math.max(IO_SIZE, n);
            int max = this.maxBufferSize;
            if (cap > max) {
                cap = buf.position() + n;
                if (cap > max) {
                    throw new IOException("Output buffer overflow");
                }
            }
            ByteBuffer newBuf = allocate(cap);
            buf.flip();
            newBuf.put(buf);
            buf = this.outBuffer = newBuf;
        }

        return buf;
    }

    protected Transfer resetOutBuffer() {
        ByteBuffer buf = this.outBuffer;
        // Dirty data?
        if (buf.position() > 0) buf.clear();
        return this;
    }

    public byte[] readFully(int n) throws IOException {
        InputStream in = this.in;
        byte[] data = new byte[n];
        ByteBuffer buf = this.inBuffer;
        int i = 0;

        if (buf.hasRemaining()) {
            int rem = buf.remaining();
            i = Math.min(n, rem);
            buf.get(data, 0, i);
        }
        while (i < n) {
            int x = in.read(data, i, n - i);
            if (x < 0) throw new EOFException();
            else i += x;
        }

        return data;
    }

    public Transfer write(byte[] data) throws IOException {
        return write(data, 0, data.length);
    }

    public Transfer write(byte[] data, int i, int n) throws IOException {
        ByteBuffer buf = ensureOutBuffer(n);
        buf.put(data, i, n);
        return this;
    }

    public byte[] readBytes() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            return readFully(n);
        }
    }

    public Transfer writeBytes(byte[] data) throws IOException {
        if (data == null) {
            return writeInt(-1);
        } else {
            return writeBytes(data, 0, data.length);
        }
    }

    public Transfer writeBytes(byte[] data, int i, int n) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(n);
            ByteBuffer buf = ensureOutBuffer(n);
            buf.put(data, i, n);
        }
        return this;
    }

    public Transfer flush() throws IOException {
        ByteBuffer buf = this.outBuffer;
        byte[] data = buf.array();
        int n = buf.position();
        OutputStream out = this.out;

        out.write(data, 0, n);
        out.flush();
        if (buf.capacity() > IO_SIZE)
            this.outBuffer = allocate(IO_SIZE);
        else buf.clear();

        return this;
    }

    public String readString() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            byte[] data = readFully(n);
            return new String(data, CHARSET);
        }
    }

    public String readString(int max) throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            if (n > max) throw new IOException("String too big");
            byte[] data = readFully(n);
            return new String(data, CHARSET);
        }
    }

    public Transfer writeString(String s) throws IOException {
        if (s == null) {
            writeInt(-1);
        } else {
            byte[] data = s.getBytes(CHARSET);
            writeBytes(data, 0, data.length);
        }
        return this;
    }

    public int readInt() throws IOException {
        int b = readByte(true) & 0xff;
        int i = b & 0x7f;

        if (b > 0x7f) {
            b = readByte(true) & 0xff;
            i ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = readByte(true) & 0xff;
                i ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = readByte(true) & 0xff;
                    i ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        b = readByte(true) & 0xff;
                        i ^= (b & 0x7f) << 28;
                        if (b > 0x7f) {
                            throw new IOException("Invalid int");
                        }
                    }
                }
            }
        }

        return (i >>> 1) ^ -(i & 1);
    }

    public Transfer writeInt(int i) throws IOException {
        i = (i << 1) ^ (i >> 31);

        if ((i & ~0x7F) != 0) {
            writeByte((i | 0x80) & 0xFF);
            i >>>= 7;
            if (i > 0x7F) {
                writeByte((i | 0x80) & 0xFF);
                i >>>= 7;
                if (i > 0x7F) {
                    writeByte((i | 0x80) & 0xFF);
                    i >>>= 7;
                    if (i > 0x7F) {
                        writeByte((i | 0x80) & 0xFF);
                        i >>>= 7;
                    }
                }
            }
        }

        writeByte(i);
        return this;
    }

    public long readLong() throws IOException {
        int b = readByte(true) & 0xff;
        int n = b & 0x7f;
        long l;
        if (b > 0x7f) {
            b = readByte(true) & 0xff;
            n ^= (b & 0x7f) << 7;
            if (b > 0x7f) {
                b = readByte(true) & 0xff;
                n ^= (b & 0x7f) << 14;
                if (b > 0x7f) {
                    b = readByte(true) & 0xff;
                    n ^= (b & 0x7f) << 21;
                    if (b > 0x7f) {
                        l = decodeLong(n);
                    } else {
                        l = n;
                    }
                } else {
                    l = n;
                }
            } else {
                l = n;
            }
        } else {
            l = n;
        }

        return (l >>> 1) ^ -(l & 1);
    }

    private long decodeLong(long l) throws IOException {
        int b = readByte(true) & 0xff;
        l ^= (b & 0x7fL) << 28;

        if (b > 0x7f) {
            b = readByte(true) & 0xff;
            l ^= (b & 0x7fL) << 35;
            if (b > 0x7f) {
                b = readByte(true) & 0xff;
                l ^= (b & 0x7fL) << 42;
                if (b > 0x7f) {
                    b = readByte(true) & 0xff;
                    l ^= (b & 0x7fL) << 49;
                    if (b > 0x7f) {
                        b = readByte(true) & 0xff;
                        l ^= (b & 0x7fL) << 56;
                        if (b > 0x7f) {
                            b = readByte(true) & 0xff;
                            l ^= (b & 0x7fL) << 63;
                            if (b > 0x7f) {
                                throw new IOException("Invalid long");
                            }
                        }
                    }
                }
            }
        }

        return l;
    }

    public Transfer writeLong(long n) throws IOException {
        n = (n << 1) ^ (n >> 63);
        if ((n & ~0x7FL) != 0) {
            writeByte((byte) ((n | 0x80) & 0xFF));
            n >>>= 7;
            if (n > 0x7F) {
                writeByte((byte) ((n | 0x80) & 0xFF));
                n >>>= 7;
                if (n > 0x7F) {
                    writeByte((byte) ((n | 0x80) & 0xFF));
                    n >>>= 7;
                    if (n > 0x7F) {
                        writeByte((byte) ((n | 0x80) & 0xFF));
                        n >>>= 7;
                        if (n > 0x7F) {
                            writeByte((byte) ((n | 0x80) & 0xFF));
                            n >>>= 7;
                            if (n > 0x7F) {
                                writeByte((byte) ((n | 0x80) & 0xFF));
                                n >>>= 7;
                                if (n > 0x7F) {
                                    writeByte((byte) ((n | 0x80) & 0xFF));
                                    n >>>= 7;
                                    if (n > 0x7F) {
                                        writeByte((byte) ((n | 0x80) & 0xFF));
                                        n >>>= 7;
                                        if (n > 0x7F) {
                                            writeByte((byte) ((n | 0x80) & 0xFF));
                                            n >>>= 7;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return writeByte((byte)n);
    }

    public Transfer writeOK(int status) throws IOException {
        return writeOK(status, 0, 0);
    }

    public Transfer writeOK(int status, long lastId, long affectedRows)
            throws IOException {
        // Format: OK, status, lastId, affectedRows
        return writeByte(RESULT_OK)
                .writeInt(status)
                .writeLong(lastId)
                .writeLong(affectedRows)
                .flush();
    }

    public Transfer writeError(String message) throws IOException {
        return writeError(new SQLException(message));
    }

    public Transfer writeError(String message, String sqlState)
            throws IOException {
        return writeError(new SQLException(message, sqlState, 0));
    }

    public Transfer writeError(String message, String sqlState, int vendorCode)
            throws IOException {
        return writeError(new SQLException(message, sqlState, vendorCode));
    }

    public Transfer writeError(Throwable cause) throws IOException {
        return writeError(new SQLException(cause));
    }

    public Transfer writeError(SQLException e) throws IOException {
        String message = e.getMessage();
        String sqlState = e.getSQLState();
        int vendorCode = e.getErrorCode();
        // format: ER,message,sqlState,vendorCode
        return resetOutBuffer()
                .writeByte(RESULT_ER)
                .writeString(message)
                .writeString(sqlState)
                .writeInt(vendorCode)
                .flush();
    }

    public Object readArray() throws IOException {
        int type = readByte(true);
        switch (type) {
            case TYPE_ARR_int:
                return readIntArray();
            case TYPE_ARR_double:
                return readDoubleArray();
            case TYPE_ARR_String:
                return readStringArray();
            case TYPE_ARR_long:
                return readLongArray();
            case TYPE_ARR_Object:
                return readObjectArray();
            default:
                String s = "Unknown array type: " + type;
                throw new IOException(s);
        }
    }

    public Transfer writeArray(Object a) throws IOException {
        if (a == null) {
            return writeByte(TYPE_ARR_Object).writeObjectArray(null);
        } else if (a instanceof String[]) {
            return writeByte(TYPE_ARR_String).writeStringArray((String[])a);
        } else if (a instanceof Object[]) {
            return writeByte(TYPE_ARR_Object).writeObjectArray((Object[])a);
        } else if (a instanceof int[]) {
            return writeByte(TYPE_ARR_int).writeIntArray((int[])a);
        } else if (a instanceof double[]) {
            return writeByte(TYPE_ARR_double).writeDoubleArray((double[])a);
        } else if (a instanceof long[]) {
            return writeByte(TYPE_ARR_long).writeLongArray((long[])a);
        } else {
            throw new IOException("Unknown array type: " + a);
        }
    }

    protected Transfer writeStringArray(String[] a) throws IOException {
        if (a == null) {
            return writeInt(-1);
        } else {
            int n = a.length;
            writeInt(n);
            for (String i : a) {
                writeString(i);
            }
            return this;
        }
    }

    protected Transfer writeLongArray(long[] a) throws IOException {
        if (a == null) {
            return writeInt(-1);
        } else {
            int n = a.length;
            writeInt(n);
            for (long i : a) {
                writeLong(i);
            }
            return this;
        }
    }

    protected Transfer writeObjectArray(Object[] a) throws IOException {
        if (a == null) {
            return writeInt(-1);
        } else {
            int n = a.length;
            writeInt(n);
            for (Object i : a) {
                writeObject(i);
            }
            return this;
        }
    }

    public Transfer writeObject(Object o) throws IOException {
        if (o == null) {
            return writeByte(TYPE_OBJ_NULL);
        } else if (o instanceof Integer || o instanceof Long) {
            Number n = (Number) o;
            return writeByte(TYPE_OBJ_INT).writeLong(n.longValue());
        } else if (o instanceof Double) {
            return writeByte(TYPE_OBJ_REAL).writeDouble((double) o);
        } else if (o instanceof String) {
            return writeByte(TYPE_OBJ_TEXT).writeString((String) o);
        } else if (o instanceof byte[]) {
            return writeByte(TYPE_OBJ_BLOB).writeBytes((byte[]) o);
        } else {
            throw new IOException("Unknown object type: " + o);
        }
    }

    public Transfer writeDouble(double d) throws IOException {
        long n = Double.doubleToLongBits(d);
        return writeLong(n);
    }

    public Transfer writeIntArray(int[] a) throws IOException {
        if (a == null) {
            return writeInt(-1);
        } else {
            int n = a.length;
            writeInt(n);
            for (int i : a) {
                writeInt(i);
            }
            return this;
        }
    }

    public Transfer writeDoubleArray(double[] a) throws IOException {
        if (a == null) {
            return writeInt(-1);
        } else {
            int n = a.length;
            writeInt(n);
            for (double i : a) {
                long l = Double.doubleToLongBits(i);
                writeLong(l);
            }
            return this;
        }
    }

    public Object readObject() throws IOException {
        int type = readByte(true);
        switch (type) {
            case TYPE_OBJ_INT:
                return readLong();
            case TYPE_OBJ_REAL:
                return readDouble();
            case TYPE_OBJ_TEXT:
                return readString();
            case TYPE_OBJ_BLOB:
                return readBytes();
            case TYPE_OBJ_NULL:
                return null;
            default:
                String s = "Unknown object type: " + type;
                throw new IOException(s);
        }
    }

    protected Object[] readObjectArray() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            Object[] a = new Object[n];
            for (int i = 0; i < n; ++i) {
                a[i] = readObject();
            }
            return a;
        }
    }

    protected long[] readLongArray() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            long[] a = new long[n];
            for (int i = 0; i < n; ++i) {
                a[i] = readLong();
            }
            return a;
        }
    }

    protected String[] readStringArray() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            String[] a = new String[n];
            for (int i = 0; i < n; ++i) {
                a[i] = readString();
            }
            return a;
        }
    }

    protected int[] readIntArray() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            int[] a = new int[n];
            for (int i = 0; i < n; ++i) {
                a[i] = readInt();
            }
            return a;
        }
    }

    protected double[] readDoubleArray() throws IOException {
        int n = readInt();
        if (n == -1) {
            return null;
        } else {
            double[] a = new double[n];
            for (int i = 0; i < n; ++i) {
                a[i] = readDouble();
            }
            return a;
        }
    }

    protected double readDouble() throws IOException {
        long l = readLong();
        return Double.longBitsToDouble(l);
    }

}
