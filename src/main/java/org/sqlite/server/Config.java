/*
 * Copyright (c) 2021 little-pan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sqlite.server;

import org.sqlite.util.logging.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Properties;
import java.util.logging.Logger;

public class Config implements Cloneable {
    static final Logger log = LoggerFactory.getLogger(Config.class);
    private static final Properties DEFAULT = new Properties();
    private static final String CONFIG_FILE = "config.properties";

    static {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        try (InputStream in = loader.getResourceAsStream(CONFIG_FILE)) {
            String f = "Load config file '%s' from classpath";
            log.info(() -> String.format(f, CONFIG_FILE));
            if (in != null) {
                DEFAULT.load(in);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    String protocol = DEFAULT.getProperty("protocol", "rmi");
    String host = DEFAULT.getProperty("host", "localhost");
    int port = Integer.decode(DEFAULT.getProperty("port", "3515"));
    String user = DEFAULT.getProperty("user", "root");
    String password = DEFAULT.getProperty("password");

    String baseDir = DEFAULT.getProperty("baseDir", System.getProperty("user.dir"));
    String dataDir = DEFAULT.getProperty("dataDir", this.baseDir + File.separator + "data");

    RMIServerSocketFactory rmiServerSocketFactory;
    RMIClientSocketFactory rmiClientSocketFactory;

    public Config() {

    }

    public String getProtocol() {
        return protocol;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getBaseDir() {
        return baseDir;
    }

    public String getDataDir() {
        return dataDir;
    }

    public Properties getAuthProperties() {
        final Properties props = new Properties();

        props.setProperty("host", this.host);
        props.setProperty("user", this.user);
        if (this.password != null) {
            props.setProperty("password", this.password);
        }

        return props;
    }

    public RMIServerSocketFactory getRMIServerSocketFactory() {
        return this.rmiServerSocketFactory;
    }

    public void setRMIServerSocketFactory(RMIServerSocketFactory rmiServerSocketFactory) {
        this.rmiServerSocketFactory = rmiServerSocketFactory;
    }

    public RMIClientSocketFactory getRMIClientSocketFactory() {
        return this.rmiClientSocketFactory;
    }

    public void setRMIClientSocketFactory(RMIClientSocketFactory rmiClientSocketFactory) {
        this.rmiClientSocketFactory = rmiClientSocketFactory;
    }

    @Override
    public Config clone() {
        Config copy = new Config();
        copy.baseDir = this.baseDir;
        copy.dataDir = this.dataDir;
        copy.host = this.host;
        copy.password = this.password;
        copy.port = this.port;
        copy.protocol = this.protocol;
        copy.user = this.user;
        copy.rmiClientSocketFactory = this.rmiClientSocketFactory;
        copy.rmiServerSocketFactory = this.rmiServerSocketFactory;
        return copy;
    }

}
