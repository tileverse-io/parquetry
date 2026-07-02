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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.filter.RecordAccessors;
import io.tileverse.parquetry.internal.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Oracle that the streaming row path ({@link ParquetFileReader#read}) returns, for every nested corpus fixture, the
 * same rows the eager batch path ({@link ParquetFileReader#readBatches}) materializes. An unfiltered read assembles
 * LIST and MAP groups in the eager Arrow form; a filtered read assembles them in the levels form. Both must reconstruct
 * identical values, which keeps both batch forms covered: the unfiltered case pins the assembled rows path, the
 * filtered case the levels rows path.
 *
 * <p>The filtered parity check reads with a predicate through {@code read} (the levels path) and compares against the
 * eager batches filtered row-by-row through the same {@link RecordLevelEvaluator} the row pipeline uses, which keeps
 * the two paths comparing the same selected rows. A further check confirms {@code count(predicate)} over a flat-column
 * predicate on a nested file matches {@code read(predicate).count()}, which the levels form keeps correct by leaving
 * flat leaves addressable.
 */
class StreamingLevelsParityIT {

    static Stream<Arguments> nestedFixtures() {
        return Stream.of(
                Arguments.of("nested_lists.snappy.parquet"),
                Arguments.of("nested_maps.snappy.parquet"),
                Arguments.of("repeated_no_annotation.parquet"),
                Arguments.of("list_columns.parquet"));
    }

    @ParameterizedTest
    @MethodSource("nestedFixtures")
    void streamingRowsMatchEagerBatchRows(String fixture) {
        Path file = CorpusFixtures.parquetTestingData().resolve(fixture);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader =
                    ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.empty());

            List<ParquetRecord> streamed = detachAll(reader);
            List<ParquetRecord> eager = materializeAllBatches(reader);

            assertThat(streamed).as("row count for %s", fixture).hasSameSizeAs(eager);
            for (int row = 0; row < streamed.size(); row++) {
                assertRecordEqual(eager.get(row), streamed.get(row));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("filteredFixtures")
    void filteredStreamingRowsMatchEagerBatchRowsFiltered(String fixture, Predicate predicate) {
        Path file = CorpusFixtures.parquetTestingData().resolve(fixture);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader =
                    ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.empty());

            List<ParquetRecord> streamed = detachFiltered(reader, predicate);
            List<ParquetRecord> eager = materializeBatchesFiltered(reader, predicate);

            assertThat(streamed)
                    .as("filtered row count for %s", fixture)
                    .isNotEmpty()
                    .hasSameSizeAs(eager);
            for (int row = 0; row < streamed.size(); row++) {
                assertRecordEqual(eager.get(row), streamed.get(row));
            }
        }
    }

    static Stream<Arguments> filteredFixtures() {
        return Stream.of(
                Arguments.of("nested_lists.snappy.parquet", new Predicate.Eq(ColumnPath.of("b"), new Value.IntVal(1))),
                Arguments.of("nested_maps.snappy.parquet", new Predicate.Eq(ColumnPath.of("b"), new Value.IntVal(1))),
                Arguments.of(
                        "repeated_no_annotation.parquet", new Predicate.Gt(ColumnPath.of("id"), new Value.IntVal(2))));
    }

    @ParameterizedTest
    @MethodSource("flatPredicateFixtures")
    void countOverFlatPredicateMatchesStreamingCount(String fixture, ColumnPath flatColumn, int threshold) {
        Path file = CorpusFixtures.parquetTestingData().resolve(fixture);
        Predicate predicate = new Predicate.Gt(flatColumn, new Value.IntVal(threshold));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader =
                    ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.empty());

            long counted = reader.count(predicate, ReadOptions.DEFAULTS);

            long streamed;
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                streamed = rows.count();
            }
            assertThat(counted)
                    .as("count over %s on %s", flatColumn.dot(), fixture)
                    .isEqualTo(streamed);
        }
    }

    static Stream<Arguments> flatPredicateFixtures() {
        return Stream.of(
                Arguments.of("nested_lists.snappy.parquet", ColumnPath.of("b"), 0),
                Arguments.of("nested_maps.snappy.parquet", ColumnPath.of("b"), 0),
                Arguments.of("repeated_no_annotation.parquet", ColumnPath.of("id"), 2));
    }

    private static List<ParquetRecord> detachAll(ParquetFileReader reader) {
        try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).toList();
        }
    }

    private static List<ParquetRecord> detachFiltered(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).toList();
        }
    }

    private static List<ParquetRecord> materializeBatchesFiltered(ParquetFileReader reader, Predicate predicate) {
        List<ParquetRecord> rows = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                try (batch) {
                    for (int row = 0; row < batch.rowCount(); row++) {
                        ParquetRecord record = batch.materialize(row);
                        if (RecordLevelEvaluator.test(predicate, RecordAccessors.of(record))) {
                            rows.add(record.detach());
                        }
                    }
                }
            });
        }
        return rows;
    }

    private static List<ParquetRecord> materializeAllBatches(ParquetFileReader reader) {
        List<ParquetRecord> rows = new ArrayList<>();
        try (Stream<ParquetRecordBatch> batches =
                reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                try (batch) {
                    for (int row = 0; row < batch.rowCount(); row++) {
                        rows.add(batch.materialize(row).detach());
                    }
                }
            });
        }
        return rows;
    }

    // --- deep comparison through the record API ---

    private static void assertRecordEqual(ParquetRecord expected, ParquetRecord actual) {
        assertThat(actual.columnCount()).isEqualTo(expected.columnCount());
        for (int col = 0; col < expected.columnCount(); col++) {
            ColumnPath path = expected.columnPath(col);
            assertThat(actual.isNull(col)).as("null flag at %s", path.dot()).isEqualTo(expected.isNull(col));
            assertDeepEqual(expected.get(path), actual.get(path));
        }
    }

    private static void assertDeepEqual(Object expected, Object actual) {
        if (expected == null || actual == null) {
            assertThat(actual).isEqualTo(expected);
            return;
        }
        if (expected instanceof ParquetRecord expectedRecord) {
            assertRecordEqual(expectedRecord, (ParquetRecord) actual);
            return;
        }
        if (expected instanceof Map<?, ?> expectedMap) {
            assertMapEqual(expectedMap, (Map<?, ?>) actual);
            return;
        }
        if (expected instanceof List<?> expectedList) {
            assertListEqual(expectedList, (List<?>) actual);
            return;
        }
        if (expected instanceof MemorySegment expectedSegment) {
            assertThat(bytes((MemorySegment) actual)).isEqualTo(bytes(expectedSegment));
            return;
        }
        assertThat(actual).isEqualTo(expected);
    }

    private static void assertListEqual(List<?> expected, List<?> actual) {
        assertThat(actual).hasSameSizeAs(expected);
        for (int i = 0; i < expected.size(); i++) {
            assertDeepEqual(expected.get(i), actual.get(i));
        }
    }

    private static void assertMapEqual(Map<?, ?> expected, Map<?, ?> actual) {
        List<?> expectedKeys = new ArrayList<>(expected.keySet());
        List<?> actualKeys = new ArrayList<>(actual.keySet());
        assertThat(actualKeys).hasSameSizeAs(expectedKeys);
        for (int i = 0; i < expectedKeys.size(); i++) {
            Object expectedKey = expectedKeys.get(i);
            Object actualKey = actualKeys.get(i);
            assertDeepEqual(expectedKey, actualKey);
            assertDeepEqual(expected.get(expectedKey), actual.get(actualKey));
        }
    }

    private static byte[] bytes(MemorySegment segment) {
        return segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
    }
}
