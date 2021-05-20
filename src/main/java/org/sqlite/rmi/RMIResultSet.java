package org.sqlite.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.List;

public interface RMIResultSet extends Remote, AutoCloseable {

    List<Object[]> next() throws RemoteException, SQLException;

    RMIResultSetMetaData getMetaData() throws RemoteException, SQLException;

    int getFetchSize() throws RemoteException, SQLException;

    void setFetchSize(int fetchSize) throws RemoteException, SQLException;

    int getFetchDirection() throws RemoteException, SQLException;

    void setFetchDirection(int direction) throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

}
