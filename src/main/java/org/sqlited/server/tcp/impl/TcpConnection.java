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

package org.sqlited.server.tcp.impl;

import org.sqlite.SQLiteConnection;
import org.sqlited.io.Protocol;
import org.sqlited.io.Transfer;
import org.sqlited.server.Config;
import static org.sqlited.server.util.SQLiteUtils.*;
import org.sqlited.util.IOUtils;
import org.sqlited.util.logging.LoggerFactory;

import static java.lang.Integer.*;
import java.io.IOException;
import java.net.Socket;
import java.sql.*;
import static java.sql.Statement.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TcpConnection implements Protocol, Runnable, AutoCloseable {
    static final Logger log = LoggerFactory.getLogger(TcpConnection.class);

    protected final Config config;

    // Conn management
    protected final int id;
    protected final String name;
    protected final Socket socket;
    protected Transfer ch;
    protected SQLiteConnection sqlConn;
    private volatile boolean open = true;
    private boolean readonly;

    // Stmt management
    private int nextStmtId;
    private final Map<Integer, TcpStatement> stmtMap = new HashMap<>();
    private Statement auxStmt;

    // Tx management
    private final Map<Integer, Savepoint> spMap = new HashMap<>();

    public TcpConnection(int id, Socket socket, Config config) {
        this.id = id;
        this.name = "tc-" + this.id;
        this.socket = socket;
        this.config = config;
    }

    @Override
    public void run() {
        try {
            if (openDB()) {
                process();
            }
        } catch (IOException e) {
            String s = this + ": network failure";
            log.log(Level.FINE, s, e);
        } finally {
            close();
        }
    }

    protected void process() throws IOException {
        Transfer ch = this.ch;

        while (true) {
            final int cmd = ch.read();
            if (cmd == -1) {
                log.fine(() -> this + ": peer quit");
                break;
            }
            try {
                switch (cmd) {
                    case CMD_CREATE_STMT:
                        processCreateStmt();
                        break;
                    case CMD_EXECUTE:
                        processExecute();
                        break;
                    case CMD_FETCH_ROWS:
                        processFetch();
                        break;
                    case CMD_CLOSE_STMT:
                        processCloseStmt();
                        break;
                    case CMD_SET_RO:
                        processSetReadOnly();
                        break;
                    case CMD_SET_TI:
                        processSetTxIsolation();
                        break;
                    case CMD_SET_AC:
                        processSetAutoCommit();
                        break;
                    case CMD_SET_SP:
                        processSetSavepoint();
                        break;
                    case CMD_REL_SP:
                        processReleaseSavepoint();
                        break;
                    case CMD_COMMIT:
                        processCommit();
                        break;
                    case CMD_ROLLBACK:
                        processRollback();
                        break;
                    case CMD_SET_HD:
                        processSetHoldability();
                        break;
                    default:
                        String s = "Unknown command: 0x" + toHexString(cmd);
                        ch.sendError(s, "08000");
                        close();
                        return;
                }
            } catch (SQLException e) {
                ch.sendError(e);
                log.log(Level.FINE, "SQL error", e);
            }
        }
    }

    protected void processSetHoldability() throws IOException, SQLException {
        // In: holdability
        int holdability = this.ch.read(true) & 0xFF;
        this.sqlConn.setHoldability(holdability);
        sendOK();
    }

    protected void processSetTxIsolation() throws IOException, SQLException {
        // In: level
        int level = this.ch.read(true) & 0xFF;
        this.sqlConn.setTransactionIsolation(level);
        sendOK();
    }

    protected void processSetReadOnly() throws IOException, SQLException {
        // In: readonly flag
        boolean readonly = this.ch.readBoolean();
        boolean old = this.readonly;

        if (old == readonly) {
            sendOK();
        } else {
            Connection conn = this.sqlConn;
            Statement stmt = getAuxStmt();
            setQueryOnly(stmt, readonly);
            if (readonly == queryOnly(conn, stmt)) {
                this.readonly = readonly;
                sendOK();
            } else {
                this.ch.sendError("Set readonly failure");
            }
        }
    }

    protected void processReleaseSavepoint() throws IOException, SQLException {
        // In: id, name
        Transfer ch = this.ch;
        int id = ch.readInt();
        ch.readString();
        Savepoint sp = this.spMap.remove(id);
        if (sp == null) {
            String s = "Savepoint has released, rollback, or not set";
            throw new SQLException(s);
        }
        String name = sp.getSavepointName();
        log.fine(() -> String.format("release %s", name));
        this.sqlConn.releaseSavepoint(sp);
        sendOK();
    }

    protected void processSetSavepoint() throws IOException, SQLException {
        // In: [name]
        String name = this.ch.readString();
        Savepoint sp;
        if (name == null) sp = this.sqlConn.setSavepoint();
        else sp = this.sqlConn.setSavepoint(name);
        int id = sp.getSavepointId();
        this.spMap.put(id, sp);
        sendOK(id);
    }

    protected void processRollback() throws IOException, SQLException {
        // In: Savepoint-id, Savepoint-name or null
        Object savePoint = this.ch.readArray();
        if (savePoint == null) {
            this.sqlConn.rollback();
            this.spMap.clear();
        } else {
            Object[] a = (Object[])savePoint;
            int id = ((Number)a[0]).intValue();
            Savepoint sp = this.spMap.remove(id);
            if (sp == null) {
                String s = "Savepoint has rollback, released or not set";
                throw new SQLException(s);
            }
            String name = sp.getSavepointName();
            log.fine(() -> String.format("rollback to %s", name));
            this.sqlConn.rollback(sp);
        }
        sendOK();
    }

    protected void processCommit() throws SQLException, IOException {
        this.sqlConn.commit();
        this.spMap.clear();
        sendOK();
    }

    protected void processSetAutoCommit() throws IOException, SQLException {
        // In: 1|0
        Transfer ch = this.ch;
        boolean ac = ch.readBoolean();
        Connection conn = this.sqlConn;
        conn.setAutoCommit(ac);
        sendOK();
    }

    protected void processCloseStmt() throws IOException, SQLException {
        // In: id
        Transfer ch = this.ch;
        int id = ch.readInt();
        TcpStatement ts = this.stmtMap.remove(id);
        IOUtils.close(ts);
        sendOK();
    }

    protected void processFetch() throws IOException, SQLException {
        // In: id, fetch-size
        Transfer ch = this.ch;
        int id = ch.readInt();
        int n = ch.readInt();

        TcpStatement ts = this.stmtMap.get(id);
        if (ts == null) {
            ch.sendError("Statement has been closed");
        } else {
            ts.stmt.setFetchSize(n);
            ts.fetchRows();
        }
    }

    protected void processExecute() throws IOException, SQLException {
        // In: id, sql, genKeys, column-indexes/names or null
        Transfer ch = this.ch;
        int id = ch.readInt();
        String sql = ch.readString();
        int genKeys = ch.readInt();
        ch.readArray(); // Ignore column-indexes/names indicator

        log.fine(() -> String.format("execute \"%s\"", sql));
        TcpStatement ts = this.stmtMap.get(id);
        if (ts == null) {
            ch.sendError("Statement has been closed");
            return;
        }

        boolean autoGeneratedKeys = (RETURN_GENERATED_KEYS == genKeys);
        AutoGenKeysListener listener = null;
        SQLiteConnection conn = this.sqlConn;
        boolean result;

        if (autoGeneratedKeys) {
            boolean ac = conn.getAutoCommit();
            listener = new AutoGenKeysListener(ts, ac);
            conn.addUpdateListener(listener);
            try {
                result = ts.stmt.execute(sql);
            } finally {
                conn.removeUpdateListener(listener);
            }
        } else {
            result = ts.stmt.execute(sql);
        }
        if (result) {
            ts.sendResultSet(true);
        } else {
            int affectedRows = ts.stmt.getUpdateCount();
            if (listener == null) {
                sendOK(0, affectedRows);
            } else {
                sendOK(listener.max, affectedRows);
                listener.sendResultSet();
            }
        }
    }

    protected void processCreateStmt() throws IOException, SQLException {
        // In: rsType, rsConcur, rsHold
        Connection conn = this.sqlConn;
        Transfer ch = this.ch;
        int rsType = ch.readInt(), rsConcur = ch.readInt(), rsHold = ch.readInt();
        Statement stmt = conn.createStatement(rsType, rsConcur, rsHold);
        boolean failed = true;
        try {
            TcpStatement ts = new TcpStatement(this, stmt);
            Map<Integer, TcpStatement> stmtMap = this.stmtMap;
            while (true) {
                int id = this.nextStmtId++;
                if (this.nextStmtId < 0) this.nextStmtId = 0;
                TcpStatement old = stmtMap.putIfAbsent(id, ts);
                if (old == null) {
                    sendOK(id);
                    break;
                }
            }
            failed = false;
        } finally {
            if (failed) IOUtils.close(stmt);
        }
    }

    protected boolean openDB() throws IOException {
        int maxBuffer = this.config.getMaxBufferSize();
        Transfer ch = this.ch = new Transfer(this.socket, maxBuffer);
        String url = ch.readString();
        int n = ch.readInt();
        Properties info = new Properties();
        for (int i = 0; i < n; ++i) {
            String name = ch.readString();
            String value = ch.readString();
            info.setProperty(name, value);
        }
        boolean failed = true;
        try {
            String dataDir = this.config.getDataDir();
            url = wrapURL(dataDir, url);
            this.sqlConn = open(url, info);
            Statement stmt = getAuxStmt();
            this.readonly = queryOnly(this.sqlConn, stmt);
            sendOK();
            failed = false;
            return true;
        } catch (SQLException e) {
            ch.sendError(e);
            return false;
        } finally {
            if (failed) close();
        }
    }

    public void sendOK() throws SQLException, IOException {
        sendOK(0, 0, 0);
    }

    public void sendOK(long lastId) throws SQLException, IOException {
        sendOK(0, lastId, 0);
    }

    public void sendOK(long lastInsertId, long affectedRows)
            throws SQLException, IOException {
        sendOK(0, lastInsertId, affectedRows);
    }

    public void sendOK(int status, long lastInsertId, long affectedRows)
            throws SQLException, IOException {
        status = getStatus(this.sqlConn, this.readonly, status);
        if ((status & 0x2) != 0x0) this.spMap.clear();
        this.ch.sendOK(status, lastInsertId, affectedRows);
    }

    protected Statement getAuxStmt() throws SQLException {
        Statement stmt = this.auxStmt;
        if (stmt == null || stmt.isClosed()) {
            return (this.auxStmt = this.sqlConn.createStatement());
        } else {
            return stmt;
        }
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        this.ch = null;
        this.stmtMap.clear();
        this.spMap.clear();
        IOUtils.close(this.auxStmt);
        IOUtils.close(this.sqlConn);
        IOUtils.close(this.socket);
        this.open = false;
    }

    @Override
    public String toString() {
        return this.name;
    }

}
