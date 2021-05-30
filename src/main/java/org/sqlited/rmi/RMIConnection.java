package org.sqlited.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.sql.Savepoint;

public interface RMIConnection extends Remote, AutoCloseable {

    RMIStatement createStatement() throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

    boolean getAutoCommit() throws RemoteException, SQLException;

    void setAutoCommit(boolean autoCommit) throws RemoteException, SQLException;

    void commit() throws RemoteException, SQLException;

    void rollback() throws RemoteException, SQLException;

    Savepoint setSavepoint(String name) throws RemoteException, SQLException;

    void rollback(Savepoint savepoint) throws RemoteException, SQLException;

    void releaseSavepoint(Savepoint savepoint) throws RemoteException, SQLException;

    boolean isReadonly() throws RemoteException, SQLException;

    void setReadOnly(boolean readonly) throws RemoteException, SQLException;

}
