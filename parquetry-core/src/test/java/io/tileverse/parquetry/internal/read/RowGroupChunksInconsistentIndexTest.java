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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.CompressionCodec;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.internal.filter.bloom.SplitBlockBloomFilter;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;

/**
 * A page index not describing the pages of its chunk is left unused, and the chunk reads as one without it. An offset
 * index describes them when its first page starts at the first row of the row group and each later page starts after
 * the previous one, before the last row ends, with the bytes of the pages following one another in the file. A column
 * index does when its null flags and bounds have one entry per page of the offset index.
 */
class RowGroupChunksInconsistentIndexTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final long ROWS = 10L;
    private static final int PAGE_BYTES = 8;

    @ParameterizedTest(name = "{0}")
    @MethodSource("offsetIndexes")
    void offsetIndexIsUsedWhenItsPagesSplitTheRowsInOrder(String scenario, long[] firstRows, boolean usable) {
        ColumnIndex bounds = columnIndex(firstRows.length, firstRows.length, firstRows.length);
        RowGroupChunks chunks = chunks(bounds, offsetIndex(firstRows));

        assertThat(chunks.offsetIndex(V).isPresent()).as("offset index in use").isEqualTo(usable);
        assertThat(chunks.pageStats(V).isPresent()).as("page statistics in use").isEqualTo(usable);
    }

    static Stream<Arguments> offsetIndexes() {
        return Stream.of(
                Arguments.of("pages in row order", new long[] {0, 4, 8}, true),
                Arguments.of("a single page", new long[] {0}, true),
                Arguments.of("a last page of one row", new long[] {0, 9}, true),
                Arguments.of("no pages for a row group with rows", new long[] {}, false),
                Arguments.of("a first page past the first row", new long[] {2, 4, 8}, false),
                Arguments.of("a negative first row", new long[] {-1, 4, 8}, false),
                Arguments.of("pages out of row order", new long[] {0, 8, 4}, false),
                Arguments.of("a page without rows", new long[] {0, 4, 4}, false),
                Arguments.of("a page starting past the last row", new long[] {0, 4, 10}, false));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pageByteLocations")
    void offsetIndexIsUsedWhenItsPagesFollowOneAnotherInTheFile(
            String scenario, long[] fileOffsets, int[] byteSizes, boolean usable) {
        OffsetIndex pages = offsetIndex(new long[] {0, 4, 8}, fileOffsets, byteSizes);
        RowGroupChunks chunks = chunks(columnIndex(3, 3, 3), pages);

        assertThat(chunks.offsetIndex(V).isPresent()).as("offset index in use").isEqualTo(usable);
        assertThat(chunks.pageStats(V).isPresent()).as("page statistics in use").isEqualTo(usable);
    }

    static Stream<Arguments> pageByteLocations() {
        int[] eightBytesEach = {8, 8, 8};
        return Stream.of(
                Arguments.of("pages back to back", new long[] {4, 12, 20}, eightBytesEach, true),
                Arguments.of("pages with bytes between them", new long[] {4, 20, 40}, eightBytesEach, true),
                Arguments.of("pages out of file order", new long[] {4, 20, 12}, eightBytesEach, false),
                Arguments.of("pages sharing bytes", new long[] {4, 8, 20}, eightBytesEach, false),
                Arguments.of("a page of no bytes", new long[] {4, 12, 20}, new int[] {8, 0, 8}, false),
                Arguments.of("a first page of no bytes", new long[] {4, 12, 20}, new int[] {0, 8, 8}, false),
                Arguments.of("a page of a negative size", new long[] {4, 12, 20}, new int[] {8, -1, 8}, false));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("columnIndexes")
    void columnIndexIsUsedWhenItHasOneEntryPerPage(
            String scenario, int nullFlags, int minimums, int maximums, boolean usable) {
        ColumnIndex bounds = columnIndex(nullFlags, minimums, maximums);
        RowGroupChunks chunks = chunks(bounds, offsetIndex(0, 4, 8));

        assertThat(chunks.pageStats(V).isPresent()).as("page statistics in use").isEqualTo(usable);
        assertThat(chunks.offsetIndex(V)).as("the offset index stays in use").isPresent();
    }

    static Stream<Arguments> columnIndexes() {
        return Stream.of(
                Arguments.of("one entry per page", 3, 3, 3, true),
                Arguments.of("fewer null flags than pages", 2, 3, 3, false),
                Arguments.of("fewer minimums than pages", 3, 2, 3, false),
                Arguments.of("fewer maximums than pages", 3, 3, 2, false),
                Arguments.of("more entries than pages", 4, 4, 4, false));
    }

    private static RowGroupChunks chunks(ColumnIndex columnIndex, OffsetIndex offsetIndex) {
        FileMetaData footer = footer();
        ParquetSchema schema = SchemaBuilder.build(footer.schema());
        return TestRowGroupChunks.of(footer, 0, schema, serving(columnIndex, offsetIndex));
    }

    private static IndexSectionLoader serving(ColumnIndex columnIndex, OffsetIndex offsetIndex) {
        return new IndexSectionLoader() {

            @Override
            public OffsetIndex readOffsetIndex(long offset, int length) {
                return offsetIndex;
            }

            @Override
            public ColumnIndex readColumnIndex(long offset, int length) {
                return columnIndex;
            }

            @Override
            public SplitBlockBloomFilter readBloom(long offset, int length) {
                throw new AssertionError("no bloom filter is read");
            }
        };
    }

    /** Pages of {@link #PAGE_BYTES} bytes back to back in the file, starting at the given rows. */
    private static OffsetIndex offsetIndex(long... firstRows) {
        long[] fileOffsets = new long[firstRows.length];
        int[] byteSizes = new int[firstRows.length];
        for (int page = 0; page < firstRows.length; page++) {
            fileOffsets[page] = 4L + (long) page * PAGE_BYTES;
            byteSizes[page] = PAGE_BYTES;
        }
        return offsetIndex(firstRows, fileOffsets, byteSizes);
    }

    private static OffsetIndex offsetIndex(long[] firstRows, long[] fileOffsets, int[] byteSizes) {
        List<PageLocation> pages = new ArrayList<>(firstRows.length);
        for (int page = 0; page < firstRows.length; page++) {
            pages.add(new PageLocation(fileOffsets[page], byteSizes[page], firstRows[page]));
        }
        return new OffsetIndex(pages, Optional.empty());
    }

    private static ColumnIndex columnIndex(int nullFlags, int minimums, int maximums) {
        return new ColumnIndex(
                Collections.nCopies(nullFlags, Boolean.FALSE),
                Collections.nCopies(minimums, int64(0L)),
                Collections.nCopies(maximums, int64(9L)),
                BoundaryOrder.UNORDERED,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static MemorySegment int64(long value) {
        return MemorySegment.ofArray(new long[] {value});
    }

    /** A one-row-group footer of {@link #ROWS} rows with one INT64 column holding both page index sections. */
    private static FileMetaData footer() {
        ColumnMetaData meta = ColumnMetaData.builder()
                .type(PhysicalType.INT64)
                .codec(CompressionCodec.UNCOMPRESSED)
                .pathInSchema(List.of("v"))
                .numValues(ROWS)
                .totalCompressedSize(64L)
                .dataPageOffset(4L)
                .build();
        ColumnChunk chunk = ColumnChunk.builder()
                .metaData(Optional.of(meta))
                .offsetIndexOffset(OptionalLong.of(100L))
                .offsetIndexLength(OptionalInt.of(10))
                .columnIndexOffset(OptionalLong.of(110L))
                .columnIndexLength(OptionalInt.of(10))
                .build();
        RowGroup rowGroup =
                RowGroup.builder().columns(List.of(chunk)).numRows(ROWS).build();
        return FileMetaData.builder()
                .version(2)
                .schema(List.of(element("schema", Optional.empty(), OptionalInt.of(1)), leafElement()))
                .numRows(ROWS)
                .rowGroups(List.of(rowGroup))
                .build();
    }

    private static SchemaElement leafElement() {
        return element("v", Optional.of(PhysicalType.INT64), OptionalInt.empty());
    }

    private static SchemaElement element(String name, Optional<PhysicalType> type, OptionalInt children) {
        Optional<FieldRepetitionType> repetition =
                type.isPresent() ? Optional.of(FieldRepetitionType.OPTIONAL) : Optional.empty();
        return new SchemaElement(
                type,
                OptionalInt.empty(),
                repetition,
                name,
                children,
                Optional.empty(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                OptionalInt.empty());
    }
}
