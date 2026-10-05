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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Properties;

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
import io.tileverse.parquetry.cli.support.Fixtures;

/**
 * Drives the {@code par} CLI against an S3 bucket served by LocalStack, exercising the cloud-storage configuration
 * flags ({@code --provider}, {@code --region}, {@code --access-key}, {@code --secret-key}, {@code --path-style}) end to
 * end. A custom S3 endpoint is addressed either by passing the full {@code http://host:port/bucket/key} URL or by
 * passing a canonical {@code s3://bucket/key} URI together with the {@code --endpoint} flag.
 *
 * <p>The bucket is created by the {@code awslocal} of the container and the fixture is uploaded through a
 * {@link Storage} opened from the same {@code storage.s3.*} properties produced by the CLI flags: the class uses no AWS
 * SDK type.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3CloudIT {

    private static final String BUCKET = "par-it";
    private static final String KEY = "cities.parquet";

    @TempDir
    static Path workDir;

    @Container
    @SuppressWarnings("resource")
    static LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.2.0")).withServices("s3");

    @BeforeAll
    static void uploadFixture() throws Exception {
        Path fixture = workDir.resolve(KEY);
        Fixtures.writeCities(fixture);
        createBucket();
        try (Storage storage = StorageFactory.open(URI.create("s3://" + BUCKET + "/"), storageProperties())) {
            storage.put(KEY, fixture, WriteOptions.defaults());
        }
    }

    private static void createBucket() throws IOException, InterruptedException {
        ExecResult created = localstack.execInContainer("awslocal", "s3api", "create-bucket", "--bucket", BUCKET);
        assertThat(created.getExitCode())
                .as("awslocal create-bucket: %s", created.getStderr())
                .isZero();
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

    @Test
    void metaReadsFromS3() {
        String out = run("meta", objectUrl());
        assertThat(out).contains("rows", "4");
    }

    @Test
    void catReadsRowsFromS3() {
        String out = run("cat", objectUrl());
        assertThat(out.strip().split("\n")).hasSize(4);
        assertThat(out).contains("Rosario");
    }

    @Test
    void copiesLocalFileToS3(@TempDir Path tempDir) throws Exception {
        Path local = tempDir.resolve("upload.parquet");
        Fixtures.writeCities(local);
        String target = objectUrl("uploaded.parquet");

        CliRunner.Result copy = CliRunner.run(
                "cp",
                local.toString(),
                target,
                "--dst-provider",
                "s3",
                "--dst-region",
                localstack.getRegion(),
                "--dst-access-key",
                localstack.getAccessKey(),
                "--dst-secret-key",
                localstack.getSecretKey(),
                "--dst-path-style");
        assertThat(copy.exitCode())
                .as("par cp local->s3 exit code; stderr was: %s", copy.stderr())
                .isZero();

        String count = run("row-count", target);
        assertThat(count.strip()).isEqualTo("4");
    }

    @Test
    void copiesS3FileToLocal(@TempDir Path tempDir) {
        Path local = tempDir.resolve("downloaded.parquet");

        CliRunner.Result copy = CliRunner.run(
                "cp",
                objectUrl(),
                local.toString(),
                "--provider",
                "s3",
                "--region",
                localstack.getRegion(),
                "--access-key",
                localstack.getAccessKey(),
                "--secret-key",
                localstack.getSecretKey(),
                "--path-style");
        assertThat(copy.exitCode())
                .as("par cp s3->local exit code; stderr was: %s", copy.stderr())
                .isZero();

        CliRunner.Result count = CliRunner.run("row-count", local.toString());
        assertThat(count.exitCode())
                .as("par row-count exit code; stderr was: %s", count.stderr())
                .isZero();
        assertThat(count.stdout().strip()).isEqualTo("4");
    }

    @Test
    void metaReadsFromS3ViaEndpointFlag() {
        String canonicalUri = "s3://" + BUCKET + "/" + KEY;

        // No --path-style: setting --endpoint defaults storage.s3.force-path-style to true, which
        // resolves a canonical s3://bucket/key URI to endpoint/bucket/key path-style addressing.
        CliRunner.Result result = CliRunner.run(
                "meta",
                canonicalUri,
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
                .as("par meta via --endpoint exit code; stderr was: %s", result.stderr())
                .isZero();
        assertThat(result.stdout()).contains("rows", "4");
    }

    private static String objectUrl() {
        return objectUrl(KEY);
    }

    private static String objectUrl(String key) {
        return endpoint() + "/" + BUCKET + "/" + key;
    }

    private static String endpoint() {
        return localstack.getEndpoint().toString().replaceAll("/+$", "");
    }

    private static String run(String command, String url) {
        CliRunner.Result result = CliRunner.run(
                command,
                url,
                "--provider",
                "s3",
                "--region",
                localstack.getRegion(),
                "--access-key",
                localstack.getAccessKey(),
                "--secret-key",
                localstack.getSecretKey(),
                "--path-style");
        assertThat(result.exitCode())
                .as("par %s exit code; stderr was: %s", command, result.stderr())
                .isZero();
        return result.stdout();
    }
}
