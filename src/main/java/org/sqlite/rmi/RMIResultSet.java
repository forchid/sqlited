package org.sqlite.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIResultSet extends Remote, AutoCloseable {

    boolean next() throws RemoteException, SQLException;

    int getInt(String column) throws RemoteException, SQLException;

    String getString(String column) throws RemoteException, SQLException;

    RMIResultSetMetaData getMetaData() throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

}
