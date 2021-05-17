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

package org.sqlite.util.logging;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public final class LoggerFactory {

    static {
        final String logFile = "java.util.logging.config.file";
        if (System.getProperty(logFile) == null) {
            Thread current = Thread.currentThread();
            ClassLoader loader = current.getContextClassLoader();
            URL logUrl = loader.getResource("logging.properties");
            if (logUrl != null) {
                try {
                    String s = logUrl.toURI().getPath();
                    System.setProperty(logFile, s);
                    LogManager.getLogManager().readConfiguration();
                } catch (URISyntaxException | IOException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
        }
    }

    private LoggerFactory() {}

    public static Logger getLogger(Class<?> clazz) {
        return Logger.getLogger(clazz.getName());
    }

    public static Logger getLogger(String name) {
        return Logger.getLogger(name);
    }

    public static Logger getLogger(String name, String bundleName) {
        return Logger.getLogger(name, bundleName);
    }

}
