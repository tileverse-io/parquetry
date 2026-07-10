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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

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
 * The shared s3proxy harness for the S3-backed Iceberg ITs: an authorization-enabled s3proxy container per test class,
 * an {@link S3Client} against it opened before and closed after each class, and upload helpers that place a directory
 * tree under a bucket key prefix. Subclasses create their bucket and upload their fixtures in their own
 * {@code @BeforeAll} (the harness client is ready by then) and keep their own bucket and prefix constants.
 *
 * <p>{@code @Testcontainers(disabledWithoutDocker = true)} is {@code @Inherited}: every subclass self-skips when Docker
 * is absent.
 */
@Testcontainers(disabledWithoutDocker = true)
abstract class AbstractS3ProxyIcebergIT {

    protected static final String IDENTITY = "parquetry-it";
    protected static final String CREDENTIAL = "parquetry-it-secret";

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

    protected static S3Client s3Client;

    @BeforeAll
    static void openS3Client() {
        s3Client = newS3Client();
    }

    @AfterAll
    static void closeS3Client() {
        if (s3Client != null) {
            s3Client.close();
            s3Client = null;
        }
    }

    protected static void createBucket(String bucket) {
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    }

    /** Upload every regular file under {@code dir} to {@code bucket} keyed {@code <keyPrefix>/<relative path>}. */
    protected static void uploadDirectory(Path dir, String bucket, String keyPrefix) {
        try (Stream<Path> files = Files.walk(dir)) {
            files.filter(Files::isRegularFile).forEach(file -> uploadFile(dir, file, bucket, keyPrefix));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to walk the directory " + dir, e);
        }
    }

    private static void uploadFile(Path dir, Path file, String bucket, String keyPrefix) {
        String key = keyPrefix + "/" + relativeKey(dir, file);
        PutObjectRequest request =
                PutObjectRequest.builder().bucket(bucket).key(key).build();
        s3Client.putObject(request, RequestBody.fromFile(file));
    }

    private static String relativeKey(Path dir, Path file) {
        return dir.relativize(file).toString().replace(File.separatorChar, '/');
    }

    private static S3Client newS3Client() {
        String endpoint = "http://" + s3proxy.getHost() + ":" + s3proxy.getMappedPort(80);
        // s3proxy does not implement the additional integrity checksums the SDK sends by default.
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
