package org.sqlite.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;

public interface RMIConnection extends Remote, AutoCloseable {

    RMIStatement createStatement() throws RemoteException, SQLException;

    @Override
    void close() throws RemoteException;

}
