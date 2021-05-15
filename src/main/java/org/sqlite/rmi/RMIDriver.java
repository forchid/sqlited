package org.sqlite.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.sql.SQLException;
import java.util.Properties;

public interface RMIDriver extends Remote {

    RMIConnection connect(String url, Properties info)
            throws RemoteException, SQLException;

}
