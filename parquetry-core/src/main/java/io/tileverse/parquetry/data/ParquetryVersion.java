/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tileverse.parquetry.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Resolves the parquetry build version recorded in the footer's {@code created_by} field.
 *
 * <p>Maven filters the project version into {@code build.properties} at build time. When that resource is missing or
 * unreadable (for example on a hand-assembled classpath) the version reads as {@code "unknown"} rather than failing a
 * write: an informational metadata field is never worth aborting over.
 */
final class ParquetryVersion {

    private static final String RESOURCE = "/io/tileverse/parquetry/build.properties";
    private static final String UNKNOWN = "unknown";
    private static final String VERSION = load();

    private ParquetryVersion() {}

    static String version() {
        return VERSION;
    }

    private static String load() {
        try (InputStream in = ParquetryVersion.class.getResourceAsStream(RESOURCE)) {
            if (in == null) {
                return UNKNOWN;
            }
            Properties properties = new Properties();
            properties.load(in);
            return properties.getProperty("version", UNKNOWN);
        } catch (IOException _) {
            return UNKNOWN;
        }
    }
}
