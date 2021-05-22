package org.sqlited.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIStatement extends Remote, AutoCloseable {

    RMIResultSet executeQuery(String s) throws RemoteException, SQLException;

    int executeUpdate(String s) throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

}
