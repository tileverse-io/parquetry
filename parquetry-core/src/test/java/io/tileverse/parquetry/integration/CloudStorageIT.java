/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.s3.S3StorageProvider;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

import io.tileverse.io.ByteBufferPool;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

/**
 * End-to-end integration tests that drive the parquetry pipeline through a real cloud-storage {@link RangeReader} -
 * specifically tileverse-storage's S3 reader pointed at a LocalStack container.
 *
 * <p>Each read uses an isolated {@link ByteBufferPool} and materializes column values into heap-owned {@code byte[]}
 * <em>inside</em> the stream's try-with-resources scope. That lets the assertions safely compare results across threads
 * and across calls without depending on the lifetime of pool-backed buffers.
 *
 * <p>Disabled automatically when no Docker daemon is reachable (see {@link Testcontainers#disabledWithoutDocker()}).
 */
@Testcontainers(disabledWithoutDocker = true)
class CloudStorageIT {

    private static final Path FIXTURE = CorpusFixtures.parquetTestingData().resolve("binary.parquet");
    private static final String BUCKET = "parquetry-it";
    private static final String KEY = "binary.parquet";
    private static final ColumnPath FOO = ColumnPath.of("foo");

    @Container
    @SuppressWarnings("resource")
    static LocalStackContainer localstack = new LocalStackContainer(
                    DockerImageName.parse("localstack/localstack:3.2.0"))
            .withServices(LocalStackContainer.Service.S3);

    private static S3Client s3Client;
    private static StaticCredentialsProvider credentials;

    @BeforeAll
    static void uploadFixture() {
        assertThat(FIXTURE).as("parquet-testing submodule must be initialized").isRegularFile();
        credentials = StaticCredentialsProvider.create(
                AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey()));
        s3Client = newS3Client();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        s3Client.putObject(
                PutObjectRequest.builder().bucket(BUCKET).key(KEY).build(), RequestBody.fromFile(FIXTURE.toFile()));
    }

    @AfterAll
    static void shutdownS3() {
        if (s3Client != null) {
            s3Client.close();
        }
    }

    @Test
    void readingFromS3MatchesReadingFromDisk() throws Exception {
        List<byte[]> local = readFooColumn(localStorage(), KEY);
        List<byte[]> remote;
        try (S3Client client = newS3Client()) {
            remote = readFooColumn(s3Storage(client), KEY);
        }
        assertFooColumnEquals(local, remote);
    }

    @Test
    void sixteenVirtualThreadsReadTheSameObject() throws Exception {
        List<byte[]> expected = readFooColumn(localStorage(), KEY);
        int workers = 16;
        List<Future<List<byte[]>>> futures = new ArrayList<>(workers);
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < workers; i++) {
                futures.add(executor.submit(readS3FooColumn()));
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(2, TimeUnit.MINUTES))
                    .as("all 16 virtual-thread reads must complete within 2 minutes")
                    .isTrue();
        }
        for (int i = 0; i < workers; i++) {
            List<byte[]> actual = futures.get(i).get();
            assertFooColumnEquals(expected, actual);
        }
    }

    // --- helpers ---

    private static Callable<List<byte[]>> readS3FooColumn() {
        return () -> {
            try (S3Client client = newS3Client()) {
                return readFooColumn(s3Storage(client), KEY);
            }
        };
    }

    /**
     * Reads the {@code foo} column of {@code binary.parquet} into heap-owned {@code byte[]}s (one entry per row; null
     * for the absent rows). The materialization happens <em>inside</em> the stream's try-with-resources block while the
     * per-call {@link ByteBufferPool} buffers are still live; the returned list survives any subsequent pool release.
     */
    private static List<byte[]> readFooColumn(Storage storage, String key) throws IOException {
        ByteBufferPool pool = new ByteBufferPool();
        ReadOptions opts = ReadOptions.builder().byteBufferPool(pool).build();
        try (storage;
                RangeReader reader = storage.openRangeReader(key)) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, opts)) {
                return stream.map(CloudStorageIT::fooOf).toList();
            }
        }
    }

    private static byte[] fooOf(ParquetRecord row) {
        return row.isNull(FOO) ? null : row.getBinary(FOO);
    }

    private static void assertFooColumnEquals(List<byte[]> expected, List<byte[]> actual) {
        assertThat(actual).as("row count must match").hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            byte[] e = expected.get(i);
            byte[] a = actual.get(i);
            assertThat(a).as("row %d bytes", i).isEqualTo(e);
        }
    }

    private static Storage localStorage() {
        Path absolute = FIXTURE.toAbsolutePath();
        return StorageFactory.open(absolute.getParent().toUri());
    }

    private static Storage s3Storage(S3Client client) {
        return S3StorageProvider.open(URI.create("s3://" + BUCKET + "/"), client);
    }

    private static S3Client newS3Client() {
        return S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .region(Region.of(localstack.getRegion()))
                .credentialsProvider(credentials)
                .forcePathStyle(true)
                .build();
    }
}
