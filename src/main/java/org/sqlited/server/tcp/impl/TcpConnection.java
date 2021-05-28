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

import org.sqlited.io.Protocol;
import org.sqlited.io.Transfer;
import org.sqlited.server.Config;
import org.sqlited.server.tcp.TcpServer;
import org.sqlited.server.util.SQLiteUtils;
import org.sqlited.util.IOUtils;
import org.sqlited.util.logging.LoggerFactory;

import static java.lang.Integer.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.sql.*;
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
    protected Connection sqlConn;
    private volatile boolean open = true;

    // Stmt management
    private int nextStmtId;
    private final Map<Integer, TcpStatement> stmtMap = new HashMap<>();

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
            final int cmd = ch.readByte();
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
                    default:
                        String s = "Unknown command: 0x" + toHexString(cmd);
                        ch.writeError(s, "08000");
                        close();
                        return;
                }
            } catch (SQLException e) {
                ch.writeError(e);
            }
        }
    }

    protected void processReleaseSavepoint() throws IOException, SQLException {
        // In: id, name
        Transfer ch = this.ch;
        int id = ch.readInt();
        String name = ch.readString();
        Savepoint sp = this.spMap.remove(id);

        if (sp == null) {
            String s = "Savepoint '" + name + "' has released, rollback or not set";
            throw new SQLException(s);
        } else {
            this.sqlConn.releaseSavepoint(sp);
        }
        sendOK();
    }

    protected void processSetSavepoint() throws IOException, SQLException {
        // In: [name]
        String name = this.ch.readString();
        Savepoint sp;
        if (name == null) sp = this.sqlConn.setSavepoint();
        else sp = this.sqlConn.setSavepoint(name);
        this.spMap.put(sp.getSavepointId(), sp);
        sendOK();
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
                String s = "Savepoint '" + a[1] + "' has rollback, released or not set";
                throw new SQLException(s);
            } else {
                this.sqlConn.rollback(sp);
            }
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
            ch.writeError("Statement has been closed");
        } else {
            ts.stmt.setFetchSize(n);
            ts.fetchRows();
        }
    }

    protected void processExecute() throws IOException, SQLException {
        // In: id, sql, agKeys, column-indexes/names or null
        Transfer ch = this.ch;
        int id = ch.readInt();
        String sql = ch.readString();
        int agKeys = ch.readInt();
        ch.readArray(); // Skip column-indexes/names
        if (Statement.RETURN_GENERATED_KEYS == agKeys) {
            String s = "Returning generated keys not implemented";
            throw new SQLFeatureNotSupportedException(s, "0A000");
        }
        TcpStatement ts = this.stmtMap.get(id);
        if (ts == null) {
            ch.writeError("Statement has been closed");
        } else {
            boolean result = ts.stmt.execute(sql);
            if (result) {
                ts.setResultSet(ts.stmt.getResultSet());
                ts.sendResultSet(true);
            } else {
                int affectedRows = ts.stmt.getUpdateCount();
                sendOK(0, affectedRows);
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
        InputStream in = this.socket.getInputStream();
        OutputStream out = this.socket.getOutputStream();
        Transfer ch = this.ch = new Transfer(in, out);
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
            url = SQLiteUtils.wrapURL(dataDir, url);
            this.sqlConn = SQLiteUtils.open(url, info);
            sendOK();
            failed = false;
            return true;
        } catch (SQLException e) {
            ch.writeError(e);
            return false;
        } finally {
            if (failed) close();
        }
    }

    public void sendOK() throws SQLException, IOException {
        sendOK(0, 0);
    }

    public void sendOK(long lastId) throws SQLException, IOException {
        sendOK(lastId, 0);
    }

    public void sendOK(long lastInsertId, long affectedRows)
            throws SQLException, IOException {
        Connection conn = this.sqlConn;
        boolean ro = conn.isReadOnly();
        boolean ac = conn.getAutoCommit();
        int status = (ro ? 0x1: 0x0) | (ac? 0x10: 0x00);
        if (ac) this.spMap.clear();
        ch.writeOK(status, lastInsertId, affectedRows);
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        this.stmtMap.clear();
        this.spMap.clear();
        IOUtils.close(this.sqlConn);
        IOUtils.close(this.socket);
        this.open = false;
    }

    @Override
    public String toString() {
        return this.name;
    }

}
