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
package io.tileverse.parquetry.cli.cloud;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.WriteOptions;

import io.tileverse.parquetry.cli.support.CliRunner;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Drives {@code par explain} against the {@code v3_geometry} Iceberg table uploaded into an S3 bucket served by
 * LocalStack. The assertion proves the remote prefix opens as an Iceberg table whose data files are pruned by manifest
 * bounds, not as a flat glob of every snapshot's parquet files.
 *
 * <p>The corpus bakes a {@code file:///iceberg-geo-testbed/v3_geometry} logical location into its metadata, while the
 * upload places the bytes under an {@code s3://bucket/v3_geometry/} prefix. The CLI's remote-prefix routing detects the
 * Iceberg metadata over storage and reconciles that baked-in logical location against the physical S3 bytes; a
 * California window then keeps {@code california.parquet} and prunes the other nine files. A flat-fileset misread would
 * instead keep all ten files with no manifest pruning.
 *
 * <p>The bucket is created by the {@code awslocal} of the container and the table is uploaded through a {@link Storage}
 * opened from the same {@code storage.s3.*} properties produced by the CLI flags: the class uses no AWS SDK type.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3IcebergExplainIT {

    private static final String TABLE = "v3_geometry";
    private static final String BUCKET = "par-iceberg-it";
    private static final String CALIFORNIA_FILTER = "ST_Intersects(geom, ST_MakeEnvelope(-124, 32, -114, 42))";

    @TempDir
    static Path corpusDir;

    @Container
    @SuppressWarnings("resource")
    static LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0")).withServices("s3");

    @BeforeAll
    static void uploadTable() throws IOException, InterruptedException {
        Path tableDir = TestCorpus.extractDirectory("iceberg-geo-testbed/" + TABLE, corpusDir);
        createBucket();
        uploadDirectory(tableDir);
    }

    private static void createBucket() throws IOException, InterruptedException {
        ExecResult created = localstack.execInContainer("awslocal", "s3api", "create-bucket", "--bucket", BUCKET);
        assertThat(created.getExitCode())
                .as("awslocal create-bucket: %s", created.getStderr())
                .isZero();
    }

    @Test
    void explainPrunesIcebergDataFilesByManifestBounds() {
        CliRunner.Result result = CliRunner.run(
                "explain",
                "s3://" + BUCKET + "/" + TABLE + "/",
                "--filter",
                CALIFORNIA_FILTER,
                "--provider",
                "s3",
                "--region",
                localstack.getRegion(),
                "--access-key",
                localstack.getAccessKey(),
                "--secret-key",
                localstack.getSecretKey(),
                "--endpoint",
                endpoint());

        assertThat(result.exitCode())
                .as("par explain over s3 exit code; stderr was: %s", result.stderr())
                .isZero();

        String report = result.stdout();
        // Manifest-bound pruning over the moved table keeps california.parquet and skips the other nine data
        // files. A flat-fileset misread would keep all ten with no skips.
        assertThat(report)
                .contains("1 kept, 9 skipped")
                .contains("california.parquet")
                .contains("SKIP")
                .contains("skipped");
    }

    private static String endpoint() {
        return localstack.getEndpoint().toString().replaceAll("/+$", "");
    }

    private static void uploadDirectory(Path tableDir) {
        try (Storage storage = StorageFactory.open(URI.create("s3://" + BUCKET + "/"), storageProperties());
                Stream<Path> files = Files.walk(tableDir)) {
            files.filter(Files::isRegularFile).forEach(file -> uploadFile(storage, tableDir, file));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to upload the table directory " + tableDir, e);
        }
    }

    private static void uploadFile(Storage storage, Path tableDir, Path file) {
        String key = TABLE + "/" + relativeKey(tableDir, file);
        storage.put(key, file, WriteOptions.defaults());
    }

    private static String relativeKey(Path tableDir, Path file) {
        return tableDir.relativize(file).toString().replace(File.separatorChar, '/');
    }

    private static Properties storageProperties() {
        Properties properties = new Properties();
        properties.setProperty("storage.s3.endpoint", endpoint());
        properties.setProperty("storage.s3.region", localstack.getRegion());
        properties.setProperty("storage.s3.aws-access-key-id", localstack.getAccessKey());
        properties.setProperty("storage.s3.aws-secret-access-key", localstack.getSecretKey());
        properties.setProperty("storage.s3.force-path-style", "true");
        return properties;
    }
}
