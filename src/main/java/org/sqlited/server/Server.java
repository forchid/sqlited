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

package org.sqlited.server;

public interface Server extends Runnable {

    String NAME = "SQLited";
    String VERSION = "0.0.1";

    void init() throws IllegalStateException;

    Server start() throws IllegalStateException;

    void stop() throws IllegalStateException;

    boolean isStopped();

    Config getConfig();

    default String getName() {
        Config c = getConfig();
        String f = "%s-%s@%s:%s";
        String proto = c.getProtocol();
        String host = c.getHost();
        int port = c.getPort();
        return String.format(f, NAME, proto, host, port);
    }

}
