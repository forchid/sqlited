package org.sqlited.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIResultSet extends Remote, AutoCloseable {

    RowIterator next(boolean meta) throws RemoteException, SQLException;

    RMIResultSetMetaData getMetaData() throws RemoteException, SQLException;

    int getFetchSize() throws RemoteException, SQLException;

    void setFetchSize(int rows) throws RemoteException, SQLException;

    int getFetchDirection() throws RemoteException, SQLException;

    void setFetchDirection(int direction) throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

}
