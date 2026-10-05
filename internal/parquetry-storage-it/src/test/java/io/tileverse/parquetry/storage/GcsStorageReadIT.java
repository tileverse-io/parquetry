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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

/**
 * The storage read against Google Cloud Storage served by fake-gcs-server. The GCS SDK client only creates the bucket;
 * the fixture is uploaded and read through the tileverse Storage, unsigned as required by the emulator.
 */
@Testcontainers(disabledWithoutDocker = true)
class GcsStorageReadIT extends AbstractStorageReadIT {

    private static final String PROJECT = "parquetry-it";

    private static final String BUCKET = "parquetry-it";

    @Container
    @SuppressWarnings("resource")
    static FakeGcsServerContainer gcs = new FakeGcsServerContainer();

    @TempDir
    static Path workDir;

    private static Path fixture;

    @BeforeAll
    static void uploadFixture() throws Exception {
        fixture = extractFixture(workDir);
        createBucket();
        putFixture(bucket(), emulatorProperties(), fixture);
    }

    private static void createBucket() throws Exception {
        StorageOptions options = StorageOptions.newBuilder()
                .setHost(endpoint())
                .setProjectId(PROJECT)
                .setCredentials(NoCredentials.getInstance())
                .build();
        try (Storage client = options.getService()) {
            client.create(BucketInfo.newBuilder(BUCKET).build());
        }
    }

    @Override
    protected URI container() {
        return bucket();
    }

    @Override
    protected Properties storageProperties() {
        return emulatorProperties();
    }

    @Override
    protected Path localFixture() {
        return fixture;
    }

    private static URI bucket() {
        return URI.create("gs://" + BUCKET + "/");
    }

    private static Properties emulatorProperties() {
        Properties properties = new Properties();
        properties.setProperty("storage.gcs.endpoint", endpoint());
        properties.setProperty("storage.gcs.project-id", PROJECT);
        properties.setProperty("storage.gcs.anonymous", "true");
        return properties;
    }

    private static String endpoint() {
        return "http://" + gcs.getHost() + ":" + gcs.getFirstMappedPort();
    }
}
