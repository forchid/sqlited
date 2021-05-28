package org.sqlited.rmi;

import org.sqlited.result.RowIterator;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIStatement extends RMIResultSet, Remote, AutoCloseable {

    RowIterator executeQuery(String s) throws RemoteException, SQLException;

    int executeUpdate(String s) throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

}
