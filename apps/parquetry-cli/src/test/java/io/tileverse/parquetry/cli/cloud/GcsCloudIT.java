/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.cli.cloud;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

import io.tileverse.parquetry.cli.support.CliRunner;
import io.tileverse.parquetry.cli.support.Fixtures;

import io.aiven.testcontainers.fakegcsserver.FakeGcsServerContainer;

/**
 * Drives the {@code par} CLI against a GCS bucket served by fake-gcs-server, exercising the {@code --endpoint} (GCS
 * host override) and {@code --gcs-project} flags end to end over a {@code gs://bucket/key} URL. With a host override in
 * effect the provider authenticates anonymously, matching the emulator.
 */
@Testcontainers(disabledWithoutDocker = true)
class GcsCloudIT {

    private static final String PROJECT = "par-it";
    private static final String BUCKET = "par-it";
    private static final String KEY = "cities.parquet";

    @TempDir
    static Path workDir;

    @Container
    @SuppressWarnings("resource")
    static FakeGcsServerContainer gcs = new FakeGcsServerContainer();

    @BeforeAll
    static void uploadFixture() throws Exception {
        Path fixture = workDir.resolve(KEY);
        Fixtures.writeCities(fixture);
        Storage storage = StorageOptions.newBuilder()
                .setProjectId(PROJECT)
                .setHost(endpoint())
                .setCredentials(NoCredentials.getInstance())
                .build()
                .getService();
        storage.create(BucketInfo.newBuilder(BUCKET).build());
        storage.create(BlobInfo.newBuilder(BUCKET, KEY).build(), Files.readAllBytes(fixture));
    }

    @AfterAll
    static void noop() {
        // fake-gcs-server is torn down by Testcontainers; the upload client holds no closeable resources to release.
    }

    @Test
    void metaReadsFromGcs() {
        String out = run("meta", "gs://" + BUCKET + "/" + KEY);
        assertThat(out).contains("rows", "4");
    }

    @Test
    void catReadsRowsFromGcs() {
        String out = run("cat", "gs://" + BUCKET + "/" + KEY);
        assertThat(out.strip().split("\n")).hasSize(4);
        assertThat(out).contains("Rosario");
    }

    private static String endpoint() {
        return "http://" + gcs.getHost() + ":" + gcs.getFirstMappedPort();
    }

    private static String run(String command, String uri) {
        CliRunner.Result result = CliRunner.run(command, uri, "--endpoint", endpoint(), "--gcs-project", PROJECT);
        assertThat(result.exitCode())
                .as("par %s exit code; stderr was: %s", command, result.stderr())
                .isZero();
        return result.stdout();
    }
}
