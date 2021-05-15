package org.sqlite.jdbc.rmi.util;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.sql.SQLException;

@FunctionalInterface
public interface RMIMethod<R> {

    R invoke() throws RemoteException,
            NotBoundException, SQLException;

}
