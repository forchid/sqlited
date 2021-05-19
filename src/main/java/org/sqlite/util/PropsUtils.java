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

package org.sqlite.util;

import java.util.Properties;

public final class PropsUtils {

    private PropsUtils() {

    }

    public static String remove(Properties props, String name) {
        return (String) props.remove(name);
    }

    public static String remove(Properties props, String name, String def) {
        String value = (String) props.remove(name);
        if (value == null) {
            return def;
        } else {
            return value;
        }
    }

    public static void setIfAbsent(Properties props, String name, String value) {
        if (props.getProperty(name) == null && value != null) {
            props.setProperty(name, value);
        }
    }

    public static void setNullSafe(Properties props, String name, String value) {
        if (value != null) {
            props.setProperty(name, value);
        }
    }

}
