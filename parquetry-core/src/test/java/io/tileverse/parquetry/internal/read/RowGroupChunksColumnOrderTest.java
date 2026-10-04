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

import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.ColumnOrder;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnPageStats;
import io.tileverse.parquetry.internal.filter.FilterPipeline.ColumnStats;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * Covers how the footer's column orders reach the pruning inputs. The format asks a reader to ignore the min and max of
 * a column in an unsupported order; the page bounds of the column index follow the same order. Null counts stay usable
 * either way.
 */
class RowGroupChunksColumnOrderTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final long NULL_COUNT = 3L;

    static Stream<Arguments> orders() {
        return Stream.of(
                Arguments.of(PhysicalType.INT64, new ColumnOrder.TypeDefined(), Optional.empty(), true),
                Arguments.of(PhysicalType.DOUBLE, new ColumnOrder.Ieee754TotalOrder(), Optional.empty(), true),
                Arguments.of(PhysicalType.INT96, new ColumnOrder.Int96TimestampOrder(), Optional.empty(), true),
                Arguments.of(PhysicalType.INT64, new ColumnOrder.Unknown((short) 4), Optional.empty(), false),
                Arguments.of(
                        PhysicalType.INT64,
                        new ColumnOrder.Unknown(ColumnOrder.Unknown.NO_CASE),
                        Optional.empty(),
                        false),
                Arguments.of(PhysicalType.INT64, new ColumnOrder.Ieee754TotalOrder(), Optional.empty(), false),
                Arguments.of(PhysicalType.INT64, new ColumnOrder.Int96TimestampOrder(), Optional.empty(), false),
                Arguments.of(
                        PhysicalType.FIXED_LEN_BYTE_ARRAY,
                        new ColumnOrder.TypeDefined(),
                        Optional.of(ConvertedType.INTERVAL),
                        false));
    }

    @ParameterizedTest(name = "{0} in {1}, converted type {2}: ordered bounds = {3}")
    @MethodSource("orders")
    void chunkReportsWhetherItsBoundsFollowAnAppliedOrder(
            PhysicalType type, ColumnOrder order, Optional<ConvertedType> convertedType, boolean ordered) {
        FileMetaData footer = footer(type, convertedType, Optional.of(List.of(order)));

        RowGroupChunks chunks = chunksOf(footer, indexSectionsNotRead());

        assertThat(chunks.chunk(V).orElseThrow().boundsOrdered()).isEqualTo(ordered);
    }

    @Test
    void footerWithoutColumnOrdersKeepsTheBoundsOrdered() {
        FileMetaData footer = footer(PhysicalType.INT64, Optional.empty(), Optional.empty());

        RowGroupChunks chunks = chunksOf(footer, indexSectionsNotRead());

        assertThat(chunks.chunk(V).orElseThrow().boundsOrdered()).isTrue();
        assertThat(chunks.stats(V).orElseThrow().minValue()).isPresent();
    }

    @Test
    void unknownOrderDropsTheChunkBoundsAndKeepsTheNullCount() {
        FileMetaData footer =
                footer(PhysicalType.INT64, Optional.empty(), Optional.of(List.of(new ColumnOrder.Unknown((short) 9))));

        ColumnStats stats = chunksOf(footer, indexSectionsNotRead()).stats(V).orElseThrow();

        assertThat(stats.minValue()).isEmpty();
        assertThat(stats.maxValue()).isEmpty();
        assertThat(stats.nullCount()).hasValue(NULL_COUNT);
    }

    @Test
    void unknownOrderMarksThePageBoundsUnordered() {
        FileMetaData footer =
                footer(PhysicalType.INT64, Optional.empty(), Optional.of(List.of(new ColumnOrder.Unknown((short) 9))));

        ColumnPageStats pageStats =
                chunksOf(footer, fixedIndexSections()).pageStats(V).orElseThrow();

        assertThat(pageStats.boundsOrdered()).isFalse();
    }

    @Test
    void typeDefinedOrderMarksThePageBoundsOrdered() {
        FileMetaData footer =
                footer(PhysicalType.INT64, Optional.empty(), Optional.of(List.of(new ColumnOrder.TypeDefined())));

        ColumnPageStats pageStats =
                chunksOf(footer, fixedIndexSections()).pageStats(V).orElseThrow();

        assertThat(pageStats.boundsOrdered()).isTrue();
    }

    private static RowGroupChunks chunksOf(FileMetaData footer, IndexSectionLoader loader) {
        ParquetSchema schema = SchemaBuilder.build(footer.schema());
        return TestRowGroupChunks.of(footer, 0, schema, loader);
    }

    /** A one-column, one-row-group footer with a chunk recording min 1, max 9, and a null count. */
    private static FileMetaData footer(
            PhysicalType type, Optional<ConvertedType> convertedType, Optional<List<ColumnOrder>> orders) {
        return FileMetaData.builder()
                .version(2)
                .schema(List.of(rootElement(), leafElement(type, convertedType)))
                .numRows(10L)
                .rowGroups(List.of(rowGroup(type)))
                .columnOrders(orders)
                .build();
    }

    private static SchemaElement rootElement() {
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                "schema",
                OptionalInt.of(1),
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static SchemaElement leafElement(PhysicalType type, Optional<ConvertedType> convertedType) {
        OptionalInt typeLength = type == PhysicalType.FIXED_LEN_BYTE_ARRAY ? OptionalInt.of(12) : OptionalInt.empty();
        return new SchemaElement(
                Optional.of(type),
                typeLength,
                Optional.of(FieldRepetitionType.OPTIONAL),
                "v",
                OptionalInt.empty(),
                convertedType,
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }

    private static RowGroup rowGroup(PhysicalType type) {
        Statistics statistics = Statistics.builder()
                .minValue(int64(1L))
                .maxValue(int64(9L))
                .nullCount(OptionalLong.of(NULL_COUNT))
                .build();
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(type)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of("v"))
                .numValues(10L)
                .totalCompressedSize(10L)
                .dataPageOffset(4L)
                .statistics(Optional.of(statistics))
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .metaData(Optional.of(meta))
                .columnIndexOffset(OptionalLong.of(100L))
                .columnIndexLength(OptionalInt.of(16))
                .offsetIndexOffset(OptionalLong.of(200L))
                .offsetIndexLength(OptionalInt.of(16))
                .build();
        return RowGroup.builder().columns(List.of(chunk)).numRows(10L).build();
    }

    private static MemorySegment int64(long value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Long.BYTES]);
        segment.set(INT64, 0L, value);
        return segment;
    }

    private static IndexSectionLoader indexSectionsNotRead() {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                throw new AssertionError("no index section is read");
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                throw new AssertionError("no index section is read");
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("no bloom filter is read");
            }
        };
    }

    /** Serves a one-page column index and offset index for any locator. */
    private static IndexSectionLoader fixedIndexSections() {
        return new IndexSectionLoader() {
            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                return new OffsetIndex(List.of(new PageLocation(4L, 6, 0L)), Optional.empty());
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                return new ColumnIndex(
                        List.of(false),
                        List.of(int64(1L)),
                        List.of(int64(9L)),
                        BoundaryOrder.UNORDERED,
                        Optional.of(List.of(NULL_COUNT)),
                        Optional.empty(),
                        Optional.empty());
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("no bloom filter is read");
            }
        };
    }
}
