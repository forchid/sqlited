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

package org.sqlited.jdbc.tcp.impl;

import org.sqlited.io.Transfer;
import org.sqlited.jdbc.adapter.ConnectionAdapter;
import org.sqlited.util.IOUtils;

import java.io.IOException;
import java.net.Socket;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

public class JdbcTcpConnection extends ConnectionAdapter {

    protected final Socket socket;
    protected Transfer ch;

    protected int status;

    public JdbcTcpConnection(Socket socket) {
        this.socket = socket;
    }

    public void openDB(String url, Properties info)
            throws SQLException, IOException {
        Transfer ch = this.ch;
        if (ch == null) ch = this.ch = new Transfer(this.socket);
        ch.writeString(url);
        ch.writeInt(info.size());
        for (Map.Entry<Object, Object> i: info.entrySet()) {
            String name = (String) i.getKey();
            String value = (String)i.getValue();
            ch.writeString(name).writeString(value);
        }
        ch.flush();
        readOK();
    }

    protected long[] readOK() throws SQLException {
        Transfer ch = this.ch;
        try {
            int result = ch.readByte(true);
            return readOK(result);
        } catch (IOException e) {
            String s = "Read server result error";
            throw handle(s, e);
        }
    }

    protected long[] readOK(int result) throws SQLException, IOException {
        if (Transfer.RESULT_OK == result) {
            this.status = ch.readInt();
            return new long[] {
                    ch.readLong(), // last ID
                    ch.readLong()  // affected rows
            };
        } else if (Transfer.RESULT_ER == result) {
            String s = ch.readString();
            String sqlState = ch.readString();
            int vendorCode = ch.readInt();
            throw new SQLException(s, sqlState, vendorCode);
        } else {
            String s = "Unknown server result type: " + result;
            throw new SQLNonTransientConnectionException(s, "08000");
        }
    }

    @Override
    public Statement createStatement(int rsType, int rsConcur, int rsHold)
            throws SQLException {
        Transfer ch = this.ch;
        try {
            ch.writeByte(Transfer.CMD_CREATE_STMT)
                    .writeInt(rsType)
                    .writeInt(rsConcur)
                    .writeInt(rsHold)
                    .flush();
            long id = readOK()[0];
            return new JdbcTcpStatement(this, (int)id);
        } catch (IOException e) {
            String s = "Create statement error";
            throw handle(s, e);
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.socket.isClosed();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return ((this.status & 0x1) != 0x0);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return ((this.status & 0x02) != 0x0);
    }

    @Override
    public void setAutoCommit(boolean ac) throws SQLException {
        Transfer ch = this.ch;
        try {
            ch.writeByte(Transfer.CMD_SET_AC).writeBoolean(ac)
                    .flush();
            readOK();
        } catch (IOException e) {
            String s = "Set autoCommit error";
            throw handle(s, e);
        }
    }

    @Override
    public void commit() throws SQLException {
        Transfer ch = this.ch;
        try {
            ch.writeByte(Transfer.CMD_COMMIT).flush();
            readOK();
        } catch (IOException e) {
            String s = "Set autoCommit error";
            throw handle(s, e);
        }
    }

    @Override
    public void rollback() throws SQLException {
        doRollback(null);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        if (savepoint == null) {
            throw new NullPointerException("savepoint null");
        }
        doRollback(savepoint);
    }

    protected void doRollback(Savepoint savepoint) throws SQLException {
        Transfer ch = this.ch;
        try {
            ch.writeByte(Transfer.CMD_ROLLBACK);
            if (savepoint == null) {
                ch.writeArray(null);
            } else {
                Object[] sp = {savepoint.getSavepointId(),
                        savepoint.getSavepointName()
                };
                ch.writeArray(sp);
            }
            ch.flush();
            readOK();
        } catch (IOException e) {
            String s = "Rollback error";
            throw handle(s, e);
        }
    }

    @Override
    public void close() throws SQLException {
        IOUtils.close(this.socket);
        super.close();
    }

    protected SQLException handle(IOException e) {
        String s = "Network failure";
        return handle(s, e);
    }

    protected SQLException handle(String message, IOException e) {
        return new SQLNonTransientConnectionException(message, "08000", e);
    }

}
