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
import java.util.Properties;
import java.util.stream.Stream;

import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.WriteOptions;

/**
 * The shared s3proxy harness for the S3-backed Iceberg ITs: an authorization-enabled s3proxy container per test class,
 * the {@code storage.s3.*} properties addressing it, and helpers to create a bucket and to place a directory tree under
 * a bucket key prefix. Subclasses create their bucket and upload their fixtures in their own {@code @BeforeAll} and
 * keep their own bucket and prefix constants.
 *
 * <p>Uploads and reads alike go through a {@link Storage} opened by {@link StorageFactory#open(URI, Properties)}, as a
 * deployment opens it, never from an SDK client built by the test.
 *
 * <p>s3proxy keeps each bucket as a subdirectory of {@value #BACKEND_DIR}: {@link #createBucket(String)} makes one with
 * {@code mkdir}. An in-memory backend offers no way to create a bucket other than the S3 API itself, reachable from a
 * test only through an SDK client of its own.
 *
 * <p>{@code @Testcontainers(disabledWithoutDocker = true)} is {@code @Inherited}: every subclass self-skips when Docker
 * is absent.
 */
@Testcontainers(disabledWithoutDocker = true)
abstract class AbstractS3ProxyIcebergIT {

    private static final String IDENTITY = "parquetry-it";
    private static final String CREDENTIAL = "parquetry-it-secret";

    /** The directory tree holding the buckets inside the container. */
    private static final String BACKEND_DIR = "/data";

    @Container
    @SuppressWarnings("resource")
    static GenericContainer<?> s3proxy = new GenericContainer<>(DockerImageName.parse("andrewgaul/s3proxy:2.6.0"))
            .withExposedPorts(80)
            .withEnv("S3PROXY_AUTHORIZATION", "aws-v2-or-v4")
            .withEnv("S3PROXY_IDENTITY", IDENTITY)
            .withEnv("S3PROXY_CREDENTIAL", CREDENTIAL)
            .withEnv("S3PROXY_ENDPOINT", "http://0.0.0.0:80")
            .withEnv("JCLOUDS_PROVIDER", "filesystem")
            .withEnv("JCLOUDS_FILESYSTEM_BASEDIR", BACKEND_DIR)
            .waitingFor(Wait.forListeningPort());

    /** Opens a Storage rooted at {@code uri}, a bucket or a bucket prefix of this container. */
    protected static Storage openStorage(URI uri) {
        return StorageFactory.open(uri, storageProperties());
    }

    /** The {@code storage.s3.*} properties addressing this container. */
    private static Properties storageProperties() {
        Properties properties = new Properties();
        properties.setProperty("storage.s3.endpoint", endpoint());
        properties.setProperty("storage.s3.region", "us-east-1");
        properties.setProperty("storage.s3.aws-access-key-id", IDENTITY);
        properties.setProperty("storage.s3.aws-secret-access-key", CREDENTIAL);
        properties.setProperty("storage.s3.force-path-style", "true");
        return properties;
    }

    private static String endpoint() {
        return "http://" + s3proxy.getHost() + ":" + s3proxy.getMappedPort(80);
    }

    protected static void createBucket(String bucket) {
        ExecResult created = exec("mkdir", "-p", BACKEND_DIR + "/" + bucket);
        if (created.getExitCode() != 0) {
            throw new IllegalStateException("mkdir of the bucket %s failed: %s".formatted(bucket, created.getStderr()));
        }
    }

    private static ExecResult exec(String... command) {
        try {
            return s3proxy.execInContainer(command);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to run %s in the container".formatted(String.join(" ", command)), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted running %s".formatted(String.join(" ", command)), e);
        }
    }

    /** Upload every regular file under {@code dir} to {@code bucket} keyed {@code <keyPrefix>/<relative path>}. */
    protected static void uploadDirectory(Path dir, String bucket, String keyPrefix) {
        try (Storage storage = openStorage(URI.create("s3://" + bucket + "/"));
                Stream<Path> files = Files.walk(dir)) {
            files.filter(Files::isRegularFile).forEach(file -> uploadFile(storage, dir, file, keyPrefix));
        } catch (IOException e) {
            throw new UncheckedIOException("failed to upload the directory " + dir, e);
        }
    }

    private static void uploadFile(Storage storage, Path dir, Path file, String keyPrefix) {
        String key = keyPrefix + "/" + relativeKey(dir, file);
        storage.put(key, file, WriteOptions.defaults());
    }

    private static String relativeKey(Path dir, Path file) {
        return dir.relativize(file).toString().replace(File.separatorChar, '/');
    }
}
