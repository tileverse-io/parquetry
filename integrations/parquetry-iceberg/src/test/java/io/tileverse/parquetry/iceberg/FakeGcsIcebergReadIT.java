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
package io.tileverse.parquetry.iceberg;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.StorageOptions;

import io.tileverse.storage.Storage;
import io.tileverse.storage.gcs.GoogleCloudStorageProvider;

import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

/**
 * Runs the backend-agnostic read assertions against the {@code v3_geometry} table uploaded into a Google Cloud Storage
 * bucket served by a fake-gcs-server emulator. This proves the Iceberg reader resolves metadata, reads manifests and
 * data files, and prunes by manifest bounds over the GCS protocol, while the catalog still uses the table's baked-in
 * logical location.
 *
 * <p>The Storage borrows the shared GCS client; closing a {@link Backend} between tests closes only the Storage, not
 * the client. The client is created once in {@link #uploadTable()} and closed once in {@link #closeClient()}.
 */
@Testcontainers(disabledWithoutDocker = true)
class FakeGcsIcebergReadIT implements IcebergStorageReadAssertions {

    private static final String BUCKET = "parquetry-iceberg-it";
    private static final String PROJECT = "test-project";

    @Container
    @SuppressWarnings("resource")
    static FakeGcsServerContainer gcs = new FakeGcsServerContainer();

    @TempDir
    static Path corpusDir;

    // The GCS SDK client type clashes by simple name with the tileverse Storage, hence the fully-qualified type here.
    private static com.google.cloud.storage.Storage gcsClient;

    @BeforeAll
    static void uploadTable() {
        Path tableDir = IcebergStorageReadAssertions.extractTable(corpusDir.resolve(TABLE));
        String emulatorEndpoint = "http://" + gcs.getHost() + ":" + gcs.getFirstMappedPort();
        gcsClient = StorageOptions.newBuilder()
                .setHost(emulatorEndpoint)
                .setProjectId(PROJECT)
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
        gcsClient.create(BucketInfo.newBuilder(BUCKET).build());
        uploadDirectory(tableDir);
    }

    @AfterAll
    static void closeClient() throws Exception {
        if (gcsClient != null) {
            gcsClient.close();
        }
    }

    @Override
    public Backend openBackend() {
        Storage storage = GoogleCloudStorageProvider.open(URI.create("gs://" + BUCKET + "/"), gcsClient);
        return new Backend(storage, TABLE_LOCATION);
    }

    private static void uploadDirectory(Path tableDir) {
        try (Stream<Path> files = Files.walk(tableDir)) {
            files.filter(Files::isRegularFile).forEach(file -> uploadFile(tableDir, file));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to walk the table directory " + tableDir, e);
        }
    }

    private static void uploadFile(Path tableDir, Path file) {
        String objectName = relativeObjectName(tableDir, file);
        BlobInfo blobInfo = BlobInfo.newBuilder(BUCKET, objectName).build();
        try {
            gcsClient.create(blobInfo, Files.readAllBytes(file));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to read the table file " + file, e);
        }
    }

    private static String relativeObjectName(Path tableDir, Path file) {
        return tableDir.relativize(file).toString().replace(File.separatorChar, '/');
    }
}
