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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.tileverse.storage.Storage;
import io.tileverse.storage.s3.S3StorageProvider;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * Runs the backend-agnostic read assertions against the {@code v3_geometry} table uploaded into an S3 bucket served by
 * an s3proxy container with authorization enabled. This proves the Iceberg reader resolves metadata, reads manifests
 * and data files, and prunes by manifest bounds over the real S3 protocol with credentials, while the catalog still
 * uses the table's baked-in logical location.
 *
 * <p>The Storage borrows the shared {@link S3Client}; closing a {@link Backend} between tests closes only the Storage,
 * not the client. The client is created once in {@link #uploadTable()} and closed once in {@link #closeClient()}.
 */
@Testcontainers(disabledWithoutDocker = true)
class S3IcebergReadIT extends AbstractIcebergStorageRead {

    private static final String IDENTITY = "parquetry-it";
    private static final String CREDENTIAL = "parquetry-it-secret";
    private static final String BUCKET = "parquetry-iceberg-it";

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> s3proxy = new GenericContainer<>(DockerImageName.parse("andrewgaul/s3proxy:2.6.0"))
            .withExposedPorts(80)
            .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
            .withEnv("S3PROXY_IDENTITY", IDENTITY)
            .withEnv("S3PROXY_CREDENTIAL", CREDENTIAL)
            .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:80")
            .withEnv("JCLOUDS_PROVIDER", "transient")
            .waitingFor(Wait.forListeningPort());

    @TempDir
    static Path corpusDir;

    private static S3Client s3Client;

    @BeforeAll
    static void uploadTable() {
        Path tableDir = extractTable(corpusDir.resolve(TABLE));
        s3Client = newS3Client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        uploadDirectory(tableDir);
    }

    @AfterAll
    static void closeClient() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Override
    protected Backend openBackend() {
        Storage storage = S3StorageProvider.open(URI.create("s3://" + BUCKET + "/" + TABLE + "/"), s3Client);
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
        String key = TABLE + "/" + relativeKey(tableDir, file);
        PutObjectRequest request =
                PutObjectRequest.builder().bucket(BUCKET).key(key).build();
        s3Client.putObject(request, RequestBody.fromFile(file));
    }

    private static String relativeKey(Path tableDir, Path file) {
        return tableDir.relativize(file).toString().replace(File.separatorChar, '/');
    }

    private static S3Client newS3Client() {
        String endpoint = "http://" + s3proxy.getHost() + ":" + s3proxy.getMappedPort(80);
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(IDENTITY, CREDENTIAL)))
                .forcePathStyle(true)
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .build();
    }
}
