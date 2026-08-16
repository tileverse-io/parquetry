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
package io.tileverse.parquetry.internal.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * The rows a filtered read returns from a nested file are the same however many batches the row group is decoded in.
 * Each fixture is read twice: once with the batch size capped at a single row, which runs the level-form assembly once
 * per row of the row group, and once with the cap open, which assembles the row group in one pass. Both reads must
 * return identical records, in the same order, down to every list, map, struct, and null.
 *
 * <p>Every predicate here matches all of the fixture's rows. A predicate is what puts the read on the level form (an
 * unfiltered read assembles the eager Arrow form instead), and matching every row keeps the batch count at its maximum.
 */
class NestedReadBatchSizeInvarianceTest {

    static Stream<Arguments> nestedFixtures() {
        return Stream.of(
                Arguments.of("nullable.impala.parquet", new Predicate.Gt(ColumnPath.of("id"), new Value.LongVal(0L))),
                Arguments.of("nested_lists.snappy.parquet", new Predicate.Gt(ColumnPath.of("b"), new Value.IntVal(0))),
                Arguments.of("nested_maps.snappy.parquet", new Predicate.Gt(ColumnPath.of("b"), new Value.IntVal(0))),
                Arguments.of(
                        "repeated_no_annotation.parquet", new Predicate.Gt(ColumnPath.of("id"), new Value.IntVal(0))),
                Arguments.of("datapage_v2.snappy.parquet", new Predicate.Gt(ColumnPath.of("b"), new Value.IntVal(0))));
    }

    @ParameterizedTest
    @MethodSource("nestedFixtures")
    void rowsAreIndependentOfBatchSize(String fixture, Predicate predicate) {
        Path file = CorpusFixtures.parquetTestingData().resolve(fixture);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader =
                    ParquetFileReader.open(source, ParquetRuntime.defaultRuntime(), Optional.empty());

            List<ParquetRecord> perRowBatches = readDetached(reader, predicate, oneRowPerBatch());
            List<ParquetRecord> wholeRowGroupBatches = readDetached(reader, predicate, ReadOptions.DEFAULTS);

            assertThat(perRowBatches)
                    .as("row count for %s", fixture)
                    .isNotEmpty()
                    .hasSameSizeAs(wholeRowGroupBatches);
            for (int row = 0; row < wholeRowGroupBatches.size(); row++) {
                assertRecordEqual(wholeRowGroupBatches.get(row), perRowBatches.get(row));
            }
        }
    }

    private static ReadOptions oneRowPerBatch() {
        return ReadOptions.builder().batchSize(1).build();
    }

    private static List<ParquetRecord> readDetached(
            ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
            return rows.map(ParquetRecord::detach).toList();
        }
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
        return segment.toArray(ValueLayout.JAVA_BYTE);
    }
}
