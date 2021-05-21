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

package org.sqlite.jdbc.rmi.impl;

import org.sqlite.jdbc.adapter.ResultSetAdapter;
import static org.sqlite.jdbc.rmi.util.RMIUtils.*;
import org.sqlite.rmi.RMIResultSet;
import org.sqlite.rmi.RMIResultSetMetaData;
import org.sqlite.rmi.RowIterator;
import org.sqlite.util.IOUtils;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

public class JdbcRMIResultSet extends ResultSetAdapter {

    static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    protected final JdbcRMIConnection conn;
    protected final RMIResultSet rmiRs;
    private volatile boolean closed;

    private JdbcRMIResultSetMetaData metaData;
    protected RowIterator rowItr;
    private int column;

    public JdbcRMIResultSet(JdbcRMIConnection conn, RMIResultSet rmiRs) {
        this.conn = conn;
        this.rmiRs = rmiRs;
    }

    @Override
    public boolean next() throws SQLException {
        RowIterator i = this.rowItr;
        if (i != null && i.isLast() && !i.hasNext()) {
            IOUtils.close(this);
            return false;
        }

        if (i == null || !i.hasNext()) {
            boolean meta = this.metaData == null;
            i = invoke(() -> this.rmiRs.next(meta), this.conn.props);
            RMIResultSetMetaData d = i.getMetaData();
            if (meta && d != null) {
                this.metaData = new JdbcRMIResultSetMetaData(d);
            }
            this.rowItr = i.reset();
        }
        boolean next = i.hasNext();
        if (next) i.next();
        else IOUtils.close(this);

        return next;
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getObject(column);
    }

    @Override
    public Object getObject(int column) throws SQLException {
        int i = checkColumn(column);
        Object[] row = this.rowItr.get();
        Object value = row[i];
        this.column = column;
        return value;
    }

    protected static SQLException castException(String type) {
        return new SQLException("Column can't cast to " + type);
    }

    @Override
    public byte getByte(int column) throws SQLException {
        return (byte) getInt(column);
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getByte(column);
    }

    @Override
    public short getShort(int column) throws SQLException {
        return (short) getInt(column);
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getShort(column);
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Object value = getObject(columnLabel);
        return castToInt(value);
    }

    @Override
    public int getInt(int column) throws SQLException {
        Object value = getObject(column);
        return castToInt(value);
    }

    @Override
    public long getLong(int column) throws SQLException {
        Object value = getObject(column);
        return castToLong(value);
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getLong(column);
    }

    @Override
    public boolean getBoolean(int column) throws SQLException {
        int i = getInt(column);
        return (i != 0);
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getBoolean(column);
    }

    @Override
    public float getFloat(int column) throws SQLException {
        return (float)getDouble(column);
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getFloat(column);
    }

    @Override
    public double getDouble(int column) throws SQLException {
        Object value = getObject(column);
        return castToDouble(value);
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getDouble(column);
    }

    @Override
    public BigDecimal getBigDecimal(int column) throws SQLException {
        Object value = getObject(column);
        return castToBig(value);
    }

    protected BigDecimal castToBig(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            Double n = (Double) value;
            return new BigDecimal(n);
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return new BigDecimal(n.longValue());
        } else if (value instanceof String) {
            String s = (String) value;
            return new BigDecimal(s);
        } else {
            throw castException("BigDecimal");
        }
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        int column = findColumn(columnLabel);
        return getBytes(column);
    }

    @Override
    public byte[] getBytes(int column) throws SQLException {
        Object value = getObject(column);
        return castToBytes(value);
    }

    protected byte[] castToBytes(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof String) {
            String s = (String) value;
            return s.getBytes(StandardCharsets.UTF_8);
        } else if (value instanceof byte[]) {
            return (byte[]) value;
        } else {
            throw castException("byte[]");
        }
    }

    protected double castToDouble(Object value) throws SQLException {
        if (value == null) {
            return 0.0;
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return n.doubleValue();
        } else if (value instanceof String) {
            String s = (String) value;
            return Double.parseDouble(s);
        } else {
            throw castException("double");
        }
    }

    protected long castToLong(Object value) throws SQLException {
        if (value == null) {
            return 0;
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return n.longValue();
        } else if (value instanceof String) {
            String s = (String) value;
            return Long.decode(s);
        } else {
            throw castException("long");
        }
    }

    protected int castToInt(Object value) throws SQLException {
        if (value == null) {
            return 0;
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return n.intValue();
        } else if (value instanceof String) {
            String s = (String) value;
            return Long.decode(s).intValue();
        } else {
            throw castException("int");
        }
    }

    @Override
    public String getString(int column) throws SQLException {
        Object value = getObject(column);
        return castToString(value);
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object value = getObject(columnLabel);
        return castToString(value);
    }

    protected String castToString(Object value) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Number) {
            return value.toString();
        } else if (value instanceof String) {
            return (String) value;
        } else {
            throw castException("String");
        }
    }

    @Override
    public Date getDate(int column) throws SQLException {
        return getDate(column, Calendar.getInstance());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(columnLabel, Calendar.getInstance());
    }

    @Override
    public Date getDate(int column, Calendar cal) throws SQLException {
        Object value = getObject(column);
        return castToDate(value, cal);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        int column = findColumn(columnLabel);
        return getDate(column, cal);
    }

    protected Date castToDate(Object value, Calendar cal) throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            Double d = (Double) value;
            Calendar c = julianDateToCalendar(d, cal);
            return new Date(c.getTimeInMillis());
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return new Date(n.longValue() * 1000L);
        } else if (value instanceof String) {
            String s = (String) value;
            DateFormat f = new SimpleDateFormat(DATE_FORMAT);
            f.setTimeZone(cal.getTimeZone());
            try {
                long millis = f.parse(s).getTime();
                return new Date(millis);
            } catch (ParseException e) {
                throw new SQLException("Parsing date error", e);
            }
        } else {
            throw castException("Date");
        }
    }

    @Override
    public Time getTime(int column) throws SQLException {
        return getTime(column, Calendar.getInstance());
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(columnLabel, Calendar.getInstance());
    }

    @Override
    public Time getTime(int column, Calendar cal) throws SQLException {
        Object value = getObject(column);
        return castToTime(value, cal);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        int column = findColumn(columnLabel);
        return getTime(column, cal);
    }

    protected static Time castToTime(Object value, Calendar cal)
            throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            Double d = (Double) value;
            Calendar c = julianDateToCalendar(d, cal);
            return new Time(c.getTimeInMillis());
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return new Time(n.longValue() * 1000L);
        } else if (value instanceof String) {
            String s = (String) value;
            DateFormat f = new SimpleDateFormat(DATE_FORMAT);
            f.setTimeZone(cal.getTimeZone());
            try {
                long millis = f.parse(s).getTime();
                return new Time(millis);
            } catch (ParseException e) {
                throw new SQLException("Parsing time error", e);
            }
        } else {
            throw castException("Time");
        }
    }

    @Override
    public Timestamp getTimestamp(int column) throws SQLException {
        return getTimestamp(column, Calendar.getInstance());
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(columnLabel, Calendar.getInstance());
    }

    @Override
    public Timestamp getTimestamp(int column, Calendar cal) throws SQLException {
        Object value = getObject(column);
        return castToTimestamp(value, cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        int column = findColumn(columnLabel);
        return getTimestamp(column, cal);
    }

    protected static Timestamp castToTimestamp(Object value, Calendar cal)
            throws SQLException {
        if (value == null) {
            return null;
        } else if (value instanceof Double) {
            Double d = (Double) value;
            Calendar c = julianDateToCalendar(d, cal);
            return new Timestamp(c.getTimeInMillis());
        } else if (value instanceof Number) {
            Number n = (Number) value;
            return new Timestamp(n.longValue() * 1000L);
        } else if (value instanceof String) {
            String s = (String) value;
            DateFormat f = new SimpleDateFormat(DATE_FORMAT);
            f.setTimeZone(cal.getTimeZone());
            try {
                long millis = f.parse(s).getTime();
                return new Timestamp(millis);
            } catch (ParseException e) {
                throw new SQLException("Parsing timestamp error", e);
            }
        } else {
            throw castException("Timestamp");
        }
    }

    protected static Calendar julianDateToCalendar(Double d) {
        return julianDateToCalendar(d, Calendar.getInstance());
    }

    protected static Calendar julianDateToCalendar(Double d, Calendar cal) {
        if (d == null) {
            return null;
        }

        int yyyy, dd, mm, hh, mn, ss, ms , A;

        double w = d + 0.5;
        int Z = (int)w;
        double F = w - Z;

        if (Z < 2299161) {
            A = Z;
        }
        else {
            int alpha = (int)((Z - 1867216.25) / 36524.25);
            A = Z + 1 + alpha - (int)(alpha / 4.0);
        }

        int B = A + 1524;
        int C = (int)((B - 122.1) / 365.25);
        int D = (int)(365.25 * C);
        int E = (int)((B - D) / 30.6001);

        //  month
        mm = E - ((E < 13.5) ? 1 : 13);

        // year
        yyyy = C - ((mm > 2.5) ? 4716 : 4715);

        // Day
        double jjd = B - D - (int)(30.6001 * E) + F;
        dd = (int)jjd;

        // Hour
        double hhd = jjd - dd;
        hh = (int)(24 * hhd);

        // Minutes
        double mnd = (24 * hhd) - hh;
        mn = (int)(60 * mnd);

        // Seconds
        double ssd = (60 * mnd) - mn;
        ss = (int)(60 * ssd);

        // Milliseconds
        double msd = (60 * ssd) - ss;
        ms = (int)(1000 * msd);

        cal.set(yyyy, mm-1, dd, hh, mn, ss);
        cal.set(Calendar.MILLISECOND, ms);

        if (yyyy<1) {
            cal.set(Calendar.ERA, GregorianCalendar.BC);
            cal.set(Calendar.YEAR, -(yyyy-1));
        }

        return cal;
    }

    protected JdbcRMIResultSetMetaData initMetaData() throws SQLException {
        if (this.metaData == null) {
            this.metaData = invoke(() -> {
                RMIResultSetMetaData metaData = this.rmiRs.getMetaData();
                return new JdbcRMIResultSetMetaData(metaData);
            }, this.conn.props);
        }

        return this.metaData;
    }

    @Override
    public JdbcRMIResultSetMetaData getMetaData() throws SQLException {
        checkOpen();
        JdbcRMIResultSetMetaData metaData = this.metaData;
        if (metaData == null) {
            return initMetaData();
        } else {
            return metaData;
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkOpen();
        return invoke(this.rmiRs::getFetchSize, this.conn.props);
    }

    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        checkOpen();
        invoke(() -> this.rmiRs.setFetchSize(fetchSize), this.conn.props);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkOpen();
        return invoke(this.rmiRs::getFetchDirection, this.conn.props);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkOpen();
        invoke(() -> this.rmiRs.setFetchDirection(direction), this.conn.props);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        JdbcRMIResultSetMetaData metaData = getMetaData();
        return metaData.findColumn(columnLabel);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.closed;
    }

    @Override
    public boolean wasNull() throws SQLException {
        Object value = getObject(this.column);
        return (value == null);
    }

    protected void checkOpen() throws SQLException {
        if (isClosed()) {
            throw new SQLException("ResultSet closed");
        }
    }

    protected int checkColumn(int c) throws SQLException {
        JdbcRMIResultSetMetaData metaData = getMetaData();
        if (c < 1 || c > metaData.getColumnCount()) {
            int n = metaData.getColumnCount();
            String s = "Column " + column + " out of bounds [1, " + n + "]";
            throw new SQLException(s);
        }
        return --c;
    }

    @Override
    public void close() {
        IOUtils.close(this.rmiRs);
        this.metaData = null;
        this.column = 0;
        this.closed = true;
    }

}
