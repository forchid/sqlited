package org.sqlited.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIConnection extends Remote, AutoCloseable {

    RMIStatement createStatement() throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

    boolean getAutoCommit() throws RemoteException, SQLException;

    void setAutoCommit(boolean autoCommit) throws RemoteException, SQLException;

    void commit() throws RemoteException, SQLException;

    void rollback() throws RemoteException, SQLException;

}
