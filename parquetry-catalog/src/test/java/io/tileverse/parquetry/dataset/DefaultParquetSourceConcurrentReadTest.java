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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Covers the multi-file fan-out on {@link DefaultParquetSource}: a probe-less multi-reader read drains its files
 * concurrently through {@link ConcurrentFileMerge}, while a probe-bearing read keeps the sequential visit order the
 * probe's paint gate assumes. The concurrent read must deliver exactly the rows a sequential read would, and closing it
 * early must return every borrowed buffer and reservation across the fanned-out readers.
 */
class DefaultParquetSourceConcurrentReadTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    /**
     * Distinct per-file row counts give the file-ordered sequence a non-repeating shape a concurrent interleave would
     * not reproduce.
     */
    private static final int[] ROW_COUNTS = {1_500, 900, 2_100, 1_200};

    /** A probe that keeps every unit; its presence alone must pin the read to the sequential visit order. */
    private static final SpatialReadProbe KEEP_EVERYTHING =
            (minX, minY, maxX, maxY) -> SpatialReadProbe.Decision.keep();

    @Test
    void concurrentRowsEqualSequentialRowsAsMultiset(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp);
        List<String> sequential = sequentialReferenceKeys(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(4));
            List<String> concurrent = readRowKeys(source);
            assertThat(concurrent).hasSize(sequential.size());
            assertThat(sorted(concurrent)).isEqualTo(sorted(sequential));
        } finally {
            closeAll(sources);
        }
    }

    @Test
    void concurrentBatchesDeliverEveryRowExactlyOnce(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp);
        List<String> sequential = sequentialReferenceKeys(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(4));
            List<String> keys = new ArrayList<>();
            try (Stream<ParquetRecordBatch> batches =
                    source.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> collectRowKeys(batch, keys));
            }
            assertThat(keys).hasSize(sequential.size());
            assertThat(sorted(keys)).isEqualTo(sorted(sequential));
        } finally {
            closeAll(sources);
        }
    }

    @Test
    void probePresenceKeepsTheSequentialPath(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp);
        List<String> fileOrdered = sequentialReferenceKeys(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(4));
            ReadOptions probing =
                    ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();
            List<String> keys;
            try (Stream<ParquetRecord> stream = source.read(Predicate.ALWAYS_TRUE, Projection.ALL, probing)) {
                keys = stream.map(DefaultParquetSourceConcurrentReadTest::rowKey)
                        .toList();
            }
            assertThat(keys).containsExactlyElementsOf(fileOrdered);
        } finally {
            closeAll(sources);
        }
    }

    @Test
    void earlyCloseOnTheConcurrentPathRestoresPoolAndBudget(@TempDir Path tmp) throws IOException {
        int rowsPerFile = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rowsPerFile);
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        List<ByteRangeSource> sources = openSources(Collections.nCopies(4, file));
        try {
            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(4)
                            .maxConcurrentFiles(4)
                            .build())
                    .build();
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), openOptions);
            try (Stream<ParquetRecord> stream =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stepInto(stream, 10);
            }
        } finally {
            closeAll(sources);
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers borrowed across the fanned-out readers are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across the fanned-out readers on early close")
                .isEqualTo(capacityBefore);
    }

    /** Consumes the first {@code rows} rows and abandons the rest, exercising an early close mid-fan-out. */
    private static void stepInto(Stream<ParquetRecord> stream, int rows) {
        Iterator<ParquetRecord> it = stream.iterator();
        for (int consumed = 0; consumed < rows; consumed++) {
            assertThat(it.hasNext()).isTrue();
            it.next();
        }
    }

    /** Reads every row of {@code source} through the default fan-out path and maps each to its stable key. */
    private static List<String> readRowKeys(ParquetSource source) {
        try (Stream<ParquetRecord> stream = source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return stream.map(DefaultParquetSourceConcurrentReadTest::rowKey).toList();
        }
    }

    /**
     * The file-ordered row keys a sequential read would deliver: each file read on its own as a single-reader source
     * (which never fans out) and the results concatenated in fileset index order.
     */
    private static List<String> sequentialReferenceKeys(List<Path> files) {
        List<String> keys = new ArrayList<>();
        for (Path file : files) {
            try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
                ParquetSource single = ParquetSource.open(source);
                try (Stream<ParquetRecord> rows =
                        single.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    rows.map(DefaultParquetSourceConcurrentReadTest::rowKey).forEach(keys::add);
                }
            }
        }
        return keys;
    }

    private static void collectRowKeys(ParquetRecordBatch batch, List<String> into) {
        try (batch) {
            for (int row = 0; row < batch.rowCount(); row++) {
                into.add(rowKey(batch.materialize(row)));
            }
        }
    }

    /** A per-row key over all three columns; two rows with the same key are genuinely identical rows. */
    private static String rowKey(ParquetRecord row) {
        int year = row.getInt(YEAR);
        String country = row.getString(COUNTRY);
        double value = row.getDouble(VALUE);
        return year + "|" + country + "|" + value;
    }

    private static List<String> sorted(List<String> keys) {
        List<String> copy = new ArrayList<>(keys);
        Collections.sort(copy);
        return copy;
    }

    private static OpenOptions fanOutOptions(int maxConcurrentFiles) {
        return OpenOptions.builder()
                .runtime(ParquetRuntime.builder()
                        .maxConcurrentFiles(maxConcurrentFiles)
                        .build())
                .build();
    }

    /**
     * A per-file footer failure inside the overlapped opens propagates to the caller with the type the sequential open
     * loop would have thrown. The sources stay borrowed either way; the caller closes them.
     */
    @Test
    void openPropagatesAPerFileFooterFailure(@TempDir Path tmp) throws IOException {
        Path good = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 100);
        Path junk = tmp.resolve("junk.parquet");
        Files.write(junk, new byte[64]);

        List<ByteRangeSource> sources = new ArrayList<>();
        try {
            sources.add(TestParquetFiles.openRangeReader(good));
            sources.add(TestParquetFiles.openRangeReader(junk));
            sources.add(TestParquetFiles.openRangeReader(good));

            assertThatThrownBy(() -> ParquetSource.open(TestFilesets.of(sources)))
                    .isInstanceOf(ParquetFormatException.class);
        } finally {
            closeAll(sources);
        }
    }

    /**
     * Rows handed out by {@code Stream.iterator()} are flyweight views valid until the next pull; each must read its
     * own file's values at delivery. Single-row files sharpen the check: their batches close almost immediately after
     * production, and a flatten that released a batch before its rows were delivered would hand the iterator views over
     * recycled memory. The key comparison against the sequential reference catches that as wrong content even when the
     * recycled bytes happen to decode without an exception.
     */
    @Test
    void iteratorPulledRowsMatchTheSequentialReference(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp, new int[] {1_500, 1, 900, 1});
        List<String> sequential = sequentialReferenceKeys(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(4));
            List<String> delivered = new ArrayList<>();
            try (Stream<ParquetRecord> stream =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                Iterator<ParquetRecord> it = stream.iterator();
                while (it.hasNext()) {
                    delivered.add(rowKey(it.next()));
                }
            }
            assertThat(delivered).hasSize(sequential.size());
            assertThat(sorted(delivered)).isEqualTo(sorted(sequential));
        } finally {
            closeAll(sources);
        }
    }

    /** The probe-pinned sequential path must give iterator pulls the same one-pull row validity as the fan-out. */
    @Test
    void probePathIteratorPullMatchesTheFileOrderedReference(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp, new int[] {700, 1, 400});
        List<String> fileOrdered = sequentialReferenceKeys(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(4));
            ReadOptions probing =
                    ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();
            List<String> delivered = new ArrayList<>();
            try (Stream<ParquetRecord> stream = source.read(Predicate.ALWAYS_TRUE, Projection.ALL, probing)) {
                Iterator<ParquetRecord> it = stream.iterator();
                while (it.hasNext()) {
                    delivered.add(rowKey(it.next()));
                }
            }
            assertThat(delivered).containsExactlyElementsOf(fileOrdered);
        } finally {
            closeAll(sources);
        }
    }

    private static List<Path> writeDistinctFiles(Path tmp) throws IOException {
        return writeDistinctFiles(tmp, ROW_COUNTS);
    }

    private static List<Path> writeDistinctFiles(Path tmp, int[] rowCounts) throws IOException {
        List<Path> files = new ArrayList<>(rowCounts.length);
        for (int i = 0; i < rowCounts.length; i++) {
            Path dir = Files.createDirectories(tmp.resolve("file-" + i));
            files.add(TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, rowCounts[i]));
        }
        return files;
    }

    private static List<ByteRangeSource> openSources(List<Path> files) {
        List<ByteRangeSource> sources = new ArrayList<>(files.size());
        for (Path file : files) {
            sources.add(TestParquetFiles.openRangeReader(file));
        }
        return sources;
    }

    private static void closeAll(List<ByteRangeSource> sources) {
        for (ByteRangeSource source : sources) {
            source.close();
        }
    }
}
