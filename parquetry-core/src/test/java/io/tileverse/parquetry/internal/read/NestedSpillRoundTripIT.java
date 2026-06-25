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

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetRuntime;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * End-to-end check that spilling a streamed nested read to disk is transparent. A decode budget below one batch forces
 * every decoded batch to spill; the levels-form batches the streaming path produces are serialized through the spill
 * codec (which bridges a level vector to its Arrow form at encode time) and restored. The restored rows must equal an
 * unconstrained read value-for-value, which exercises encode-time {@code toArrowForm} over level vectors end to end.
 *
 * <p>The streaming read uses the levels form only when a record-level filter runs. The fixtures with a flat column pass
 * a filter every row matches, keeping the same row set as the unconstrained reference while forcing the levels form the
 * spill codec exists to bridge. {@code list_columns} has no flat column to filter on; it reads unfiltered (the
 * assembled form) and pins the assembled spill path instead, while its plain LIST shape is also covered in the levels
 * form by {@code nested_lists}.
 */
class NestedSpillRoundTripIT {

    static Stream<Arguments> fixtures() {
        return Stream.of(
                Arguments.of("nested_lists.snappy.parquet", new Predicate.Eq(ColumnPath.of("b"), new Value.IntVal(1))),
                Arguments.of("nested_maps.snappy.parquet", new Predicate.Eq(ColumnPath.of("b"), new Value.IntVal(1))),
                Arguments.of(
                        "repeated_no_annotation.parquet", new Predicate.GtEq(ColumnPath.of("id"), new Value.IntVal(1))),
                Arguments.of("list_columns.parquet", Predicate.ALWAYS_TRUE));
    }

    @ParameterizedTest
    @MethodSource("fixtures")
    @Timeout(30)
    void spilledNestedReadReturnsTheSameRows(String fixture, Predicate allMatching) {
        Path file = CorpusFixtures.parquetTestingData().resolve(fixture);

        List<ParquetRecord> unconstrained =
                readRows(file, ParquetRuntime.defaultRuntime(), ReadOptions.DEFAULTS, Predicate.ALWAYS_TRUE);
        List<ParquetRecord> spilled = readRows(file, spillingRuntime(), smallBatches(), allMatching);

        assertThat(spilled).as("row count for spilled read of %s", fixture).hasSameSizeAs(unconstrained);
        for (int row = 0; row < unconstrained.size(); row++) {
            assertRecordEqual(unconstrained.get(row), spilled.get(row));
        }
    }

    private static ParquetRuntime spillingRuntime() {
        return ParquetRuntime.builder()
                .decodeBudget(DecodeBudget.ofBytes(1))
                .diskBudget(DiskBudget.ofBytes(256L << 20))
                .spillEnabled(true)
                .build();
    }

    private static ReadOptions smallBatches() {
        return ReadOptions.builder().batchSize(1).build();
    }

    private static List<ParquetRecord> readRows(
            Path file, ParquetRuntime runtime, ReadOptions options, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source, runtime, Optional.empty());
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
                return rows.map(ParquetRecord::detach).toList();
            }
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
        return segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
    }
}
