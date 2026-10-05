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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.WriteOptions;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads {@code binary.parquet} through a tileverse {@link Storage} and compares it with the same read of the local
 * file. A subclass names one backend by a container URI and the {@code storage.*} properties addressing it, and puts
 * the fixture there. The Storage is opened by {@link StorageFactory#open(URI, Properties)}, as a deployment opens it,
 * never from an SDK client built by the test.
 *
 * <p>Column values are copied into heap arrays inside the stream's try-with-resources scope, while the decode buffers
 * are live. The assertions then compare them across threads without depending on those buffers.
 *
 * <p>The class is abstract: Failsafe runs its tests once per subclass.
 */
abstract class AbstractStorageReadIT {

    /** The key of the fixture inside {@link #container()}. */
    protected static final String KEY = "binary.parquet";

    private static final ColumnPath FOO = ColumnPath.of("foo");

    private static final int CONCURRENT_READS = 16;

    /** The container holding the fixture at {@link #KEY}. */
    protected abstract URI container();

    /** The {@code storage.*} properties addressing {@link #container()}. */
    protected abstract Properties storageProperties();

    /** The local copy of the fixture: what each read is compared with. */
    protected abstract Path localFixture();

    @Test
    void readingThroughTheStorageMatchesReadingTheLocalFile() throws IOException {
        List<byte[]> expected = readLocalFile();

        List<byte[]> actual = readThroughStorage();

        assertSameValues(expected, actual);
    }

    @Test
    void sixteenVirtualThreadsReadTheSameObject() throws Exception {
        List<byte[]> expected = readLocalFile();
        Callable<List<byte[]>> read = this::readThroughStorage;
        List<Future<List<byte[]>>> reads = new ArrayList<>(CONCURRENT_READS);

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < CONCURRENT_READS; i++) {
                reads.add(executor.submit(read));
            }
            executor.shutdown();
            boolean finished = executor.awaitTermination(2, TimeUnit.MINUTES);
            assertThat(finished)
                    .as("the concurrent reads must finish within 2 minutes")
                    .isTrue();
        }

        for (Future<List<byte[]>> finishedRead : reads) {
            assertSameValues(expected, finishedRead.get());
        }
    }

    /** Extracts the fixture into {@code directory}, named {@link #KEY}. */
    protected static Path extractFixture(Path directory) {
        return TestCorpus.extractFile("parquet-testing/data/" + KEY, directory);
    }

    /** Uploads {@code fixture} to {@code container} at {@link #KEY} through the Storage opened for it. */
    protected static void putFixture(URI container, Properties storageProperties, Path fixture) throws IOException {
        try (Storage storage = StorageFactory.open(container, storageProperties)) {
            storage.put(KEY, fixture, WriteOptions.defaults());
        }
    }

    private List<byte[]> readThroughStorage() throws IOException {
        try (Storage storage = StorageFactory.open(container(), storageProperties());
                RangeReader reader = storage.openRangeReader(KEY);
                ByteRangeSource bytes = ByteRangeSource.of(reader)) {
            return readFooColumn(bytes);
        }
    }

    private List<byte[]> readLocalFile() {
        try (ByteRangeSource bytes = ByteRangeSource.ofFile(localFixture())) {
            return readFooColumn(bytes);
        }
    }

    /** The {@code foo} column, one entry per row and null for an absent value. */
    private static List<byte[]> readFooColumn(ByteRangeSource bytes) {
        ParquetFileReader file = ParquetFileReader.open(bytes);
        try (Stream<ParquetRecord> rows = file.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(AbstractStorageReadIT::fooOf).toList();
        }
    }

    private static byte[] fooOf(ParquetRecord row) {
        return row.isNull(FOO) ? null : row.getBinary(FOO);
    }

    private static void assertSameValues(List<byte[]> expected, List<byte[]> actual) {
        assertThat(expected).as("rows of the fixture").isNotEmpty();
        assertThat(actual).as("row count").hasSameSizeAs(expected);
        for (int row = 0; row < expected.size(); row++) {
            assertThat(actual.get(row)).as("row %d", row).isEqualTo(expected.get(row));
        }
    }
}
