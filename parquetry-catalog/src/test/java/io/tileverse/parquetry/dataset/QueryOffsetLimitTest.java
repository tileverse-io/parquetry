/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Query;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins {@link Query} offset/limit: it returns exactly the window {@code read(predicate).skip(offset).limit(limit)}
 * would, in natural read order, across batch boundaries and across files, with no empty batch on an exhausted window.
 */
class QueryOffsetLimitTest {

    private static final int ROWS = 4_000;
    private static final ColumnPath VALUE = ColumnPath.of("value"); // monotonic: row i holds i * 1.5

    // A small batch size forces many batches so offset and limit straddle batch boundaries.
    private static final ReadOptions SMALL_BATCHES =
            ReadOptions.builder().batchSize(64).build();

    @Test
    void offsetLimitEqualsSkipThenLimitWithoutAPredicate(@TempDir Path tmp) throws Exception {
        try (ByteRangeSource bytes = file(tmp)) {
            ParquetSource source = ParquetSource.open(bytes);
            assertWindowMatchesSkipLimit(source, Predicate.ALWAYS_TRUE, 100, 250);
            assertWindowMatchesSkipLimit(source, Predicate.ALWAYS_TRUE, 0, 10);
            assertWindowMatchesSkipLimit(source, Predicate.ALWAYS_TRUE, 3990, 100); // limit runs past the end
        }
    }

    @Test
    void offsetLimitEqualsSkipThenLimitWithAPredicate(@TempDir Path tmp) throws Exception {
        try (ByteRangeSource bytes = file(tmp)) {
            ParquetSource source = ParquetSource.open(bytes);
            Predicate everyFifthRow = Pred.col("year").eq(2022);
            assertWindowMatchesSkipLimit(source, everyFifthRow, 50, 123);
            assertWindowMatchesSkipLimit(source, everyFifthRow, 0, 1);
        }
    }

    @Test
    void readBatchesRowSumMatchesTheWindowSize(@TempDir Path tmp) throws Exception {
        try (ByteRangeSource bytes = file(tmp)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query windowed = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .offset(100)
                    .limit(250)
                    .build();

            long rowSum = 0L;
            try (Stream<ParquetRecordBatch> batches = source.readBatches(windowed, SMALL_BATCHES)) {
                for (ParquetRecordBatch batch : batches.toList()) {
                    assertThat(batch.rowCount()).as("no empty batch is emitted").isPositive();
                    rowSum += batch.rowCount();
                    batch.close();
                }
            }
            assertThat(rowSum).isEqualTo(250);
        }
    }

    @Test
    void offsetBeyondTheMatchCountReturnsNothing(@TempDir Path tmp) throws Exception {
        try (ByteRangeSource bytes = file(tmp)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query past = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .offset(ROWS + 10)
                    .limit(5)
                    .build();

            try (Stream<ParquetRecord> rows = source.read(past, SMALL_BATCHES)) {
                assertThat(rows.count()).isZero();
            }
            try (Stream<ParquetRecordBatch> batches = source.readBatches(past, SMALL_BATCHES)) {
                assertThat(batches.toList())
                        .as("an exhausted window emits no batch at all")
                        .isEmpty();
            }
        }
    }

    @Test
    void countIgnoresOffsetAndLimit(@TempDir Path tmp) throws Exception {
        try (ByteRangeSource bytes = file(tmp)) {
            ParquetSource source = ParquetSource.open(bytes);
            Query windowed = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .offset(100)
                    .limit(10)
                    .build();
            assertThat(source.count(windowed, SMALL_BATCHES))
                    .as("count is predicate-only and ignores the read-shaping window")
                    .isEqualTo(ROWS);
        }
    }

    /**
     * A windowed read over a two-file dataset returns the file-order slice. The reference is built from single-file
     * reads concatenated in fileset order, not from an unwindowed read of the two-file source: a windowless multi-file
     * read has no defined row order, whereas the window contract is defined against survivor (file) order.
     */
    @Test
    void offsetAndLimitSpanFiles(@TempDir Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        try (ByteRangeSource first = TestParquetFiles.openRangeReader(file);
                ByteRangeSource second = TestParquetFiles.openRangeReader(file)) {
            ParquetSource source = ParquetSource.open(TestFilesets.of(List.of(first, second)));

            // File-order reference: file one's rows then file two's rows (both are the same file read on its own).
            List<Double> fileOrder = new ArrayList<>();
            fileOrder.addAll(singleFileValues(file));
            fileOrder.addAll(singleFileValues(file));
            assertThat(fileOrder).hasSize(2 * ROWS);

            // Window straddles the file boundary: starts inside file one, ends inside file two.
            Query windowed = Query.builder(Predicate.ALWAYS_TRUE, Projection.ALL)
                    .offset(ROWS - 50)
                    .limit(100)
                    .build();
            List<Double> window = windowValues(source, windowed);

            assertThat(window).isEqualTo(fileOrder.subList(ROWS - 50, ROWS - 50 + 100));
        }
    }

    /** Reads {@code file} as its own single-reader source (which never fans out), yielding its values in file order. */
    private static List<Double> singleFileValues(Path file) {
        try (ByteRangeSource bytes = TestParquetFiles.openRangeReader(file)) {
            ParquetSource single = ParquetSource.open(bytes);
            try (Stream<ParquetRecord> rows = single.read(Predicate.ALWAYS_TRUE, Projection.ALL, SMALL_BATCHES)) {
                return rows.map(row -> row.getDouble(VALUE)).toList();
            }
        }
    }

    private void assertWindowMatchesSkipLimit(ParquetSource source, Predicate predicate, long offset, long limit) {
        List<Double> expected = values(source, predicate, SMALL_BATCHES).stream()
                .skip(offset)
                .limit(limit)
                .toList();
        Query windowed = Query.builder(predicate, Projection.ALL)
                .offset(offset)
                .limit(limit)
                .build();
        assertThat(windowValues(source, windowed))
                .as("offset=%d limit=%d window equals skip-then-limit", offset, limit)
                .isEqualTo(expected);
    }

    private static List<Double> values(ParquetSource source, Predicate predicate, ReadOptions options) {
        try (Stream<ParquetRecord> rows = source.read(predicate, Projection.ALL, options)) {
            return rows.map(row -> row.getDouble(VALUE)).toList();
        }
    }

    private static List<Double> windowValues(ParquetSource source, Query query) {
        try (Stream<ParquetRecord> rows = source.read(query, SMALL_BATCHES)) {
            return rows.map(row -> row.getDouble(VALUE)).toList();
        }
    }

    private static ByteRangeSource file(Path tmp) throws Exception {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, ROWS);
        return TestParquetFiles.openRangeReader(file);
    }
}
