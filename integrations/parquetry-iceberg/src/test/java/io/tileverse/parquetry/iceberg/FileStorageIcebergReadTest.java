/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.iceberg;

import java.nio.file.Path;

import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

/**
 * Runs the backend-agnostic read assertions against a file-backed {@link Storage} rooted at the physically-extracted
 * table while the catalog uses the table's baked-in logical location. This proves {@link StorageIcebergFileIO}'s
 * logical-to-physical key mapping end-to-end with no Docker, acting as the no-container oracle for the container-backed
 * suites.
 */
class FileStorageIcebergReadTest implements IcebergStorageReadAssertions {

    @TempDir
    Path tempDir;

    @Override
    public Backend openBackend() {
        Path tableDir = IcebergStorageReadAssertions.extractTable(tempDir.resolve(TABLE));
        Storage storage = StorageFactory.open(tableDir.toUri());
        return new Backend(storage, TABLE_LOCATION);
    }
}
