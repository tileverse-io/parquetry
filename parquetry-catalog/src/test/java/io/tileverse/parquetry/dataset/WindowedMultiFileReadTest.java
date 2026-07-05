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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins deterministic windowed reads over the concurrent multi-file fan-out. A query with an offset or a limit must run
 * the file merge in survivor (file) order: the window then slices the same row sequence a single-file-at-a-time read
 * produces, identically on every run. Both the record read and the columnar batch read take that path, and an
 * early-limit read that abandons the later files still returns every borrowed buffer and fetch reservation on close.
 */
class WindowedMultiFileReadTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");

    /** Distinct per-file row counts give the fileset an asymmetric file-order shape. */
    private static final int[] ROW_COUNTS = {1_500, 900, 2_100, 1_200, 1_800, 600};

    private static final int MAX_CONCURRENT_FILES = 4;

    /**
     * A window that begins inside file 0 and ends inside file 1: it straddles the boundary where an unordered
     * interleave would blur which rows fall in the slice, and it sits in the region K=4 keeps concurrently in flight.
     */
    private static final long WINDOW_OFFSET = 1_450L;

    private static final long WINDOW_LIMIT = 200L;

    @Test
    void windowedBatchReadIsDeterministicAndMatchesSequential(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp);
        List<String> reference = sequentialWindowReference(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(MAX_CONCURRENT_FILES));
            Query windowed = windowQuery();

            List<String> firstRun = batchWindowKeys(source, windowed);
            List<String> secondRun = batchWindowKeys(source, windowed);

            assertThat(firstRun)
                    .as("two windowed batch reads return the same rows")
                    .isEqualTo(secondRun);
            assertThat(firstRun)
                    .as("the windowed batch read equals the sequential slice")
                    .isEqualTo(reference);
        } finally {
            closeAll(sources);
        }
    }

    @Test
    void windowedRecordReadIsDeterministicAndMatchesSequential(@TempDir Path tmp) throws IOException {
        List<Path> files = writeDistinctFiles(tmp);
        List<String> reference = sequentialWindowReference(files);

        List<ByteRangeSource> sources = openSources(files);
        try {
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), fanOutOptions(MAX_CONCURRENT_FILES));
            Query windowed = windowQuery();

            List<String> firstRun = recordWindowKeys(source, windowed);
            List<String> secondRun = recordWindowKeys(source, windowed);

            assertThat(firstRun)
                    .as("two windowed record reads return the same rows")
                    .isEqualTo(secondRun);
            assertThat(firstRun)
                    .as("the windowed record read equals the sequential slice")
                    .isEqualTo(reference);
        } finally {
            closeAll(sources);
        }
    }

    @Test
    void earlyLimitReadRestoresPoolAndBudget(@TempDir Path tmp) throws IOException {
        int rowsPerFile = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rowsPerFile);
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        List<ByteRangeSource> sources = openSources(Collections.nCopies(MAX_CONCURRENT_FILES, file));
        try {
            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(4)
                            .maxConcurrentFiles(MAX_CONCURRENT_FILES)
                            .build())
                    .build();
            ParquetSource source = ParquetSource.open(TestFilesets.of(sources), openOptions);

            // A limit far smaller than one file ends the window inside the first file while the later files have
            // decoded ahead. Closing the stream must discard their buffered batches.
            Query earlyLimit = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .limit(10)
                    .build();
            try (Stream<ParquetRecordBatch> batches = source.readBatches(earlyLimit, ReadOptions.DEFAULTS)) {
                for (ParquetRecordBatch batch : batches.toList()) {
                    batch.close();
                }
            }
        } finally {
            closeAll(sources);
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers decoded ahead across the fanned-out readers are returned when the limit ends the read")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across the fanned-out readers when the limit ends the read")
                .isEqualTo(capacityBefore);
    }

    private static Query windowQuery() {
        return Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                .offset(WINDOW_OFFSET)
                .limit(WINDOW_LIMIT)
                .build();
    }

    /**
     * The exact file-order slice the window must reproduce: the sequential keys narrowed to {@code [offset, +limit)}.
     */
    private static List<String> sequentialWindowReference(List<Path> files) {
        List<String> sequential = sequentialReferenceKeys(files);
        int from = (int) WINDOW_OFFSET;
        int to = (int) (WINDOW_OFFSET + WINDOW_LIMIT);
        return sequential.subList(from, to);
    }

    private static List<String> batchWindowKeys(ParquetSource source, Query windowed) {
        List<String> keys = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches = source.readBatches(windowed, ReadOptions.DEFAULTS)) {
            for (ParquetRecordBatch batch : batches.toList()) {
                collectRowKeys(batch, keys);
            }
        }
        return keys;
    }

    private static List<String> recordWindowKeys(ParquetSource source, Query windowed) {
        try (Stream<ParquetRecord> rows = source.read(windowed, ReadOptions.DEFAULTS)) {
            return rows.map(WindowedMultiFileReadTest::rowKey).toList();
        }
    }

    /**
     * The file-ordered row keys a single-file-at-a-time read would deliver: each file read on its own as a
     * single-reader source (which never fans out) and the results concatenated in fileset index order.
     */
    private static List<String> sequentialReferenceKeys(List<Path> files) {
        List<String> keys = new ArrayList<>();
        for (Path file : files) {
            try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
                ParquetSource single = ParquetSource.open(source);
                try (Stream<ParquetRecord> rows =
                        single.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    rows.map(WindowedMultiFileReadTest::rowKey).forEach(keys::add);
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

    /**
     * A per-row key over all three columns; the file-order sequence of these keys is what the window must reproduce.
     */
    private static String rowKey(ParquetRecord row) {
        int year = row.getInt(YEAR);
        String country = row.getString(COUNTRY);
        double value = row.getDouble(VALUE);
        return year + "|" + country + "|" + value;
    }

    private static OpenOptions fanOutOptions(int maxConcurrentFiles) {
        return OpenOptions.builder()
                .runtime(ParquetRuntime.builder()
                        .maxConcurrentFiles(maxConcurrentFiles)
                        .build())
                .build();
    }

    private static List<Path> writeDistinctFiles(Path tmp) throws IOException {
        List<Path> files = new ArrayList<>(ROW_COUNTS.length);
        for (int i = 0; i < ROW_COUNTS.length; i++) {
            Path dir = Files.createDirectories(tmp.resolve("file-" + i));
            files.add(TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, ROW_COUNTS[i]));
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
