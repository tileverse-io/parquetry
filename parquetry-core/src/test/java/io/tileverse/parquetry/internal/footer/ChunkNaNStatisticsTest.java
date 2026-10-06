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
package io.tileverse.parquetry.internal.footer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Covers what the compact footer keeps of a floating-point chunk's NaN statistics: whether the writer recorded a NaN
 * count of zero, and whether its NaN and null counts add up to the values of the chunk.
 */
class ChunkNaNStatisticsTest {

    private static final int RATIO = 0;
    private static final int MEASURE = 1;
    private static final int COUNT_COLUMN = 2;
    private static final int HALF = 3;

    /** {@code schema { float ratio, double measure, int32 count, float16 half }}. */
    private static final List<SchemaElement> SCHEMA = List.of(
            group("schema", 4),
            leaf("ratio", PhysicalType.FLOAT, OptionalInt.empty(), Optional.empty()),
            leaf("measure", PhysicalType.DOUBLE, OptionalInt.empty(), Optional.empty()),
            leaf("count", PhysicalType.INT32, OptionalInt.empty(), Optional.empty()),
            leaf(
                    "half",
                    PhysicalType.FIXED_LEN_BYTE_ARRAY,
                    OptionalInt.of(2),
                    Optional.of(new LogicalType.Float16Type())));

    @Test
    void aRecordedNaNCountOfZeroTellsTheChunkHoldsNoNaN() {
        CompactFooter compact = footerWithNaNCounts(nans(0L), nans(3L), OptionalLong.empty());

        assertThat(compact.chunk(0, RATIO).holdsNoNaN()).as("NaN count of zero").isTrue();
        assertThat(compact.chunk(0, MEASURE).holdsNoNaN())
                .as("NaN count of three")
                .isFalse();
        assertThat(compact.chunk(0, COUNT_COLUMN).holdsNoNaN())
                .as("no NaN count")
                .isFalse();
    }

    @Test
    void aChunkWithoutStatisticsIsNotKnownToHoldNoNaN() {
        CompactFooter compact = footerWithNaNCounts(nans(0L), nans(0L), OptionalLong.empty());

        assertThat(compact.chunk(0, HALF).holdsNoNaN()).isFalse();
    }

    static Stream<Arguments> countedChunks() {
        return Stream.of(
                Arguments.of("NaN and null counts adding up to the values", nans(2L), nulls(1L), 3L, true),
                Arguments.of("NaN count equal to the values", nans(3L), nulls(0L), 3L, true),
                Arguments.of("a number among the values", nans(2L), nulls(0L), 3L, false),
                Arguments.of("no NaN count", OptionalLong.empty(), nulls(3L), 3L, false),
                Arguments.of("no null count", nans(3L), OptionalLong.empty(), 3L, false),
                Arguments.of("nulls only", nans(0L), nulls(3L), 3L, false),
                Arguments.of("no values", nans(0L), nulls(0L), 0L, false));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("countedChunks")
    void aChunkHoldsOnlyNaNWhenItsNaNAndNullCountsAddUpToItsValues(
            String scenario, OptionalLong nanCount, OptionalLong nullCount, long numValues, boolean onlyNaN) {
        Statistics statistics =
                Statistics.builder().nullCount(nullCount).nanCount(nanCount).build();
        ColumnChunk ratio = chunk("ratio", PhysicalType.FLOAT, Optional.of(statistics), numValues);

        CompactFooter compact = footerWithRatioChunk(ratio);

        assertThat(compact.chunk(0, RATIO).holdsOnlyNaN()).isEqualTo(onlyNaN);
    }

    private static OptionalLong nans(long count) {
        return OptionalLong.of(count);
    }

    private static OptionalLong nulls(long count) {
        return OptionalLong.of(count);
    }

    /** A footer with the given chunk of the {@code ratio} column and no statistics for the other columns. */
    private static CompactFooter footerWithRatioChunk(ColumnChunk ratio) {
        List<ColumnChunk> chunks = new ArrayList<>();
        chunks.add(ratio);
        chunks.add(chunk("measure", PhysicalType.DOUBLE, Optional.empty()));
        chunks.add(chunk("count", PhysicalType.INT32, Optional.empty()));
        chunks.add(chunk("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, Optional.empty()));
        return CompactFooter.encode(footer(chunks), LeafIndex.of(SchemaBuilder.build(SCHEMA)));
    }

    /** A footer recording the given NaN counts for the {@code ratio}, {@code measure} and {@code count} columns. */
    private static CompactFooter footerWithNaNCounts(
            OptionalLong ratioNaNs, OptionalLong measureNaNs, OptionalLong countNaNs) {
        List<ColumnChunk> chunks = new ArrayList<>();
        chunks.add(chunk("ratio", PhysicalType.FLOAT, Optional.of(statistics(ratioNaNs))));
        chunks.add(chunk("measure", PhysicalType.DOUBLE, Optional.of(statistics(measureNaNs))));
        chunks.add(chunk("count", PhysicalType.INT32, Optional.of(statistics(countNaNs))));
        chunks.add(chunk("half", PhysicalType.FIXED_LEN_BYTE_ARRAY, Optional.empty()));
        return CompactFooter.encode(footer(chunks), LeafIndex.of(SchemaBuilder.build(SCHEMA)));
    }

    private static FileMetaData footer(List<ColumnChunk> chunks) {
        RowGroup rowGroup = RowGroup.builder().columns(chunks).numRows(1L).build();
        return FileMetaData.builder()
                .version(2)
                .schema(SCHEMA)
                .numRows(1L)
                .rowGroups(List.of(rowGroup))
                .build();
    }

    private static Statistics statistics(OptionalLong nanCount) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(0L))
                .nanCount(nanCount)
                .build();
    }

    private static ColumnChunk chunk(String name, PhysicalType type, Optional<Statistics> statistics) {
        return chunk(name, type, statistics, 1L);
    }

    private static ColumnChunk chunk(String name, PhysicalType type, Optional<Statistics> statistics, long numValues) {
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(type)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of(name))
                .numValues(numValues)
                .totalCompressedSize(1L)
                .dataPageOffset(4L)
                .statistics(statistics)
                .build();
        return ColumnChunk.builder().metaData(Optional.of(meta)).build();
    }

    private static SchemaElement group(String name, int children) {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.of(FieldRepetitionType.REQUIRED),
                name,
                OptionalInt.of(children),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leaf(
            String name, PhysicalType type, OptionalInt typeLength, Optional<LogicalType> logicalType) {
        return new SchemaElement(
                Optional.of(type),
                typeLength,
                Optional.of(FieldRepetitionType.OPTIONAL),
                name,
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                logicalType,
                OptionalInt.empty());
    }
}
