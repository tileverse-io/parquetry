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

import java.net.URI;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.s3.S3StorageProvider;

/**
 * Runs the backend-agnostic read assertions against the {@code v3_geometry} table uploaded into an S3 bucket served by
 * an s3proxy container with authorization enabled. This proves the Iceberg reader resolves metadata, reads manifests
 * and data files, and prunes by manifest bounds over the real S3 protocol with credentials, while the catalog still
 * uses the table's baked-in logical location.
 *
 * <p>The Storage borrows the harness's shared {@code S3Client}; closing a {@link Backend} between tests closes only the
 * Storage, not the client. The client opens and closes once per class in {@link AbstractS3ProxyIcebergIT}.
 */
class S3IcebergReadIT extends AbstractS3ProxyIcebergIT implements IcebergStorageReadAssertions {

    private static final String BUCKET = "parquetry-iceberg-it";

    @TempDir
    static Path corpusDir;

    @BeforeAll
    static void uploadTable() {
        Path tableDir = IcebergStorageReadAssertions.extractTable(corpusDir.resolve(TABLE));
        createBucket(BUCKET);
        uploadDirectory(tableDir, BUCKET, TABLE);
    }

    @Override
    public Backend openBackend() {
        Storage storage = S3StorageProvider.open(URI.create("s3://" + BUCKET + "/" + TABLE + "/"), s3Client);
        return new Backend(storage, TABLE_LOCATION);
    }
}
