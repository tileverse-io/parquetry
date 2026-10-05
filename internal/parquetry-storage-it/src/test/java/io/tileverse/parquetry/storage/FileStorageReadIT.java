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
package io.tileverse.parquetry.storage;

import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

/** The storage read against a local directory. It needs no container and runs on any machine. */
class FileStorageReadIT extends AbstractStorageReadIT {

    @TempDir
    static Path directory;

    private static Path fixture;

    @BeforeAll
    static void extract() {
        fixture = extractFixture(directory);
    }

    @Override
    protected URI container() {
        return directory.toUri();
    }

    @Override
    protected Properties storageProperties() {
        return new Properties();
    }

    @Override
    protected Path localFixture() {
        return fixture;
    }
}
