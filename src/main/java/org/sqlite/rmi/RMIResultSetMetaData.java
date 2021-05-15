package org.sqlite.rmi;

import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIResultSetMetaData {

    int getColumnCount() throws RemoteException, SQLException;

    String getColumnName(int column) throws RemoteException, SQLException;

    int getColumnType(int column) throws RemoteException, SQLException;

    int getColumnDisplaySize(int column) throws RemoteException, SQLException;

}
