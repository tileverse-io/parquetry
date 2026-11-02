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
package io.tileverse.parquetry.internal.read.page;

import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.footer.CompactFooter;
import io.tileverse.parquetry.internal.footer.LeafIndex;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

import lombok.NonNull;

class PageHeaderScanTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath STRING_COL = ColumnPath.of("string_col");
    private static final ParquetSchema IDS =
            WriteFixtures.schemaOf(WriteFixtures.requiredLeaf("id", PrimitiveKind.INT64));
    private static final int ID_COUNT = 450;
    private static final int PAGE_VALUES = 100;

    /** The bytes of the minimum recorded by each page of the chunks testing the cap on bounds. */
    private static final int BOUND_BYTES = 1 << 20;

    @TempDir
    Path tempDir;

    @Test
    void threePagesFromAV1Chunk() {
        MemorySegment chunk = PageFixtures.v1FlatChunk(10, 20, 30);
        long firstPage = PageFixtures.v1FlatChunk(10).byteSize();
        long firstTwoPages = PageFixtures.v1FlatChunk(10, 20).byteSize();

        LearnedPages pages = scan(chunk, 60).orElseThrow();

        assertThat(pages.pageCount()).isEqualTo(3);
        assertThat(eachPage(pages, pages::offset)).containsExactly(0L, firstPage, firstTwoPages);
        assertThat(eachPage(pages, pages::firstRow)).containsExactly(0L, 10L, 30L);
        assertThat(totalSize(pages)).isEqualTo(chunk.byteSize());
    }

    @Test
    void dictionaryPageIsSkipped() {
        MemorySegment dictionary = PageFixtures.dictionaryPage(4);
        MemorySegment chunk = PageFixtures.concat(dictionary, PageFixtures.v1FlatChunk(10, 20));

        LearnedPages pages = scan(chunk, 30).orElseThrow();

        assertThat(pages.pageCount()).isEqualTo(2);
        assertThat(pages.offset(0)).isEqualTo(dictionary.byteSize());
        assertThat(eachPage(pages, pages::firstRow)).containsExactly(0L, 10L);
    }

    @Test
    void v2RowsComeFromNumRows() {
        MemorySegment chunk =
                PageFixtures.concat(PageFixtures.v2PageDeclaring(10, 0, 10), PageFixtures.v2PageDeclaring(20, 5, 20));

        LearnedPages pages = scan(chunk, 30).orElseThrow();

        assertThat(eachPage(pages, pages::firstRow)).containsExactly(0L, 10L);
    }

    // A flat column holds one value per row, and a header counting other rows disagrees with itself.
    @Test
    void v2RowsMustMatchValues() {
        MemorySegment chunk = PageFixtures.v2PageDeclaring(10, 0, 5);

        assertThat(scan(chunk, 5)).as("the rows of the header").isEmpty();
        assertThat(scan(chunk, 10)).as("the values of the header").isEmpty();
    }

    @Test
    void pagesMustFillTheChunk() {
        MemorySegment chunk = PageFixtures.v1FlatChunk(10, 20, 30);
        MemorySegment trailingByte = PageFixtures.concat(chunk, MemorySegment.ofArray(new byte[1]));
        MemorySegment cutPayload = chunk.asSlice(0, chunk.byteSize() - 1);

        assertThat(scan(chunk, 61)).as("one row more than the pages hold").isEmpty();
        assertThat(scan(trailingByte, 60)).as("a zero byte after the last page").isEmpty();
        assertThat(scan(cutPayload, 60))
                .as("a last page running past the chunk")
                .isEmpty();
    }

    // The first page alone holds the 10 rows, and the scan must not keep it.
    @Test
    void aBadHeaderLearnsNothing() {
        long firstPage = PageFixtures.v1FlatChunk(10).byteSize();
        MemorySegment cutHeader = PageFixtures.v1FlatChunk(10, 20).asSlice(0, firstPage + 3);

        assertThat(scan(cutHeader, 10)).isEmpty();
    }

    static Stream<Arguments> negativeCounts() {
        return Stream.of(
                Arguments.of(
                        "V1 values",
                        PageFixtures.concat(PageFixtures.v1FlatChunk(11), PageFixtures.v1PageDeclaring(-1))),
                Arguments.of(
                        "V2 values and rows",
                        PageFixtures.concat(
                                PageFixtures.v2PageDeclaring(11, 0, 11), PageFixtures.v2PageDeclaring(-1, 0, -1))),
                Arguments.of("V2 nulls", PageFixtures.v2PageDeclaring(10, -1, 10)));
    }

    // The rows of the pages add up to the 10 rows, and only the negative count is wrong.
    @ParameterizedTest(name = "{0}")
    @MethodSource("negativeCounts")
    void negativeCountsLearnNothing(String count, MemorySegment chunk) {
        assertThat(scan(chunk, 10)).as("negative %s", count).isEmpty();
    }

    static Stream<Arguments> emptyPages() {
        return Stream.of(
                Arguments.of("V1", PageFixtures.concat(PageFixtures.v1FlatChunk(10), PageFixtures.v1PageDeclaring(0))),
                Arguments.of(
                        "V2",
                        PageFixtures.concat(
                                PageFixtures.v2PageDeclaring(10, 0, 10), PageFixtures.v2PageDeclaring(0, 0, 0))));
    }

    // The rows of the pages add up to the 10 rows, and the last page holds no value.
    @ParameterizedTest(name = "{0}")
    @MethodSource("emptyPages")
    void anEmptyPageLearnsNothing(String version, MemorySegment chunk) {
        assertThat(scan(chunk, 10)).as("a %s page without values", version).isEmpty();
    }

    // The pages hold the rows of the row group, one row each, and only their count exceeds the ceiling.
    @Test
    void tooManyDataPagesLearnNothing() {
        int pageCount = PageHeaderScan.MAX_DATA_PAGES + 1;
        MemorySegment chunk = PageFixtures.repeated(PageFixtures.v1PageDeclaring(1), pageCount);

        assertThat(scan(chunk, pageCount)).isEmpty();
    }

    @Test
    void aChunkAtTheCeilingLearns() {
        int pageCount = PageHeaderScan.MAX_DATA_PAGES;
        MemorySegment chunk = PageFixtures.repeated(PageFixtures.v1PageDeclaring(1), pageCount);

        Optional<LearnedPages> pages = scan(chunk, pageCount);

        assertThat(pages).map(LearnedPages::pageCount).contains(pageCount);
    }

    // Each page records a minimum of 1 MiB, and only the bytes of the bounds exceed the cap.
    @Test
    void tooManyBoundBytesLearnNothing() {
        int pageCount = PageHeaderScan.MAX_BOUND_BYTES / BOUND_BYTES + 1;
        MemorySegment chunk = PageFixtures.repeated(PageFixtures.v1PageWithMinValue(BOUND_BYTES), pageCount);

        assertThat(scan(chunk, pageCount)).isEmpty();
    }

    @Test
    void aChunkAtTheBoundCapLearns() {
        int pageCount = PageHeaderScan.MAX_BOUND_BYTES / BOUND_BYTES;
        MemorySegment chunk = PageFixtures.repeated(PageFixtures.v1PageWithMinValue(BOUND_BYTES), pageCount);

        Optional<LearnedPages> pages = scan(chunk, pageCount);

        assertThat(pages).map(LearnedPages::pageCount).contains(pageCount);
    }

    @Test
    void noDataPageLearnsNothing() {
        assertThat(scan(MemorySegment.ofArray(new byte[0]), 0))
                .as("an empty chunk")
                .isEmpty();
        assertThat(scan(PageFixtures.dictionaryPage(4), 0))
                .as("a dictionary page alone")
                .isEmpty();
    }

    // The third page starts at row 2^32 - 2, beyond an int, and the builder rejects it.
    @Test
    void firstRowBeyondIntLearnsNothing() {
        MemorySegment page = PageFixtures.v1PageDeclaring(Integer.MAX_VALUE);
        MemorySegment chunk = PageFixtures.concat(page, page, page);

        assertThat(scan(chunk, 3L * Integer.MAX_VALUE)).isEmpty();
    }

    // Only the V2 file declares an offset index. Its headers are longer, and the V1 pages match its first rows only.
    @Test
    void locationsMatchADeclaredIndex() throws IOException {
        Path declaring = writeIds("declaring.parquet", ParquetVersion.V2_0);
        Path undeclared = writeIds("undeclared.parquet", ParquetVersion.V1_1);
        List<PageLocation> declared = declaredPagesInChunk(declaring, ID);

        LearnedPages v2Pages = scanChunk(declaring, ID).orElseThrow();
        LearnedPages v1Pages = scanChunk(undeclared, ID).orElseThrow();

        assertThat(declared.getFirst().offset())
                .as("a dictionary page before the data pages")
                .isPositive();
        assertThat(locationsOf(v2Pages)).containsExactlyElementsOf(declared);
        assertThat(eachPage(v1Pages, v1Pages::firstRow)).containsExactlyElementsOf(firstRowsOf(declared));
    }

    // parquet-mr wrote the file with V1 pages and an offset index. Its string column starts with a dictionary page.
    @Test
    void v1CorpusPagesMatchItsIndex() throws IOException {
        Path file = CorpusFixtures.parquetTestingData().resolve("alltypes_tiny_pages.parquet");
        List<PageLocation> declaredIds = declaredPagesInChunk(file, ID);
        List<PageLocation> declaredStrings = declaredPagesInChunk(file, STRING_COL);

        LearnedPages ids = scanChunk(file, ID).orElseThrow();
        LearnedPages strings = scanChunk(file, STRING_COL).orElseThrow();

        assertThat(declaredStrings.getFirst().offset())
                .as("a dictionary page before the string pages")
                .isPositive();
        assertThat(locationsOf(ids)).containsExactlyElementsOf(declaredIds);
        assertThat(locationsOf(strings)).containsExactlyElementsOf(declaredStrings);
    }

    @Test
    void headerStatisticsBecomeBounds() throws IOException {
        Path file = writeIds("bounded.parquet", ParquetVersion.V1_1);

        LearnedPages pages = scanChunk(file, ID).orElseThrow();

        assertThat(pages.hasBounds()).isTrue();
        assertThat(longOf(pages.min(0))).isZero();
        assertThat(longOf(pages.max(0))).isEqualTo(PAGE_VALUES - 1L);
    }

    private static Optional<LearnedPages> scan(MemorySegment chunk, long rowCount) {
        return PageHeaderScan.scan(chunk, rowCount, PrimitiveKind.INT64, Optional.empty());
    }

    /** Writes the ids from zero up in one row group, in pages of {@link #PAGE_VALUES} values. */
    private Path writeIds(String fileName, ParquetVersion version) throws IOException {
        WriteOptions options = WriteOptions.builder()
                .parquetVersion(version)
                .pageValueLimit(PAGE_VALUES)
                .build();
        List<Map<ColumnPath, Object>> rows = LongStream.range(0, ID_COUNT)
                .mapToObj(id -> Map.<ColumnPath, Object>of(ID, id))
                .toList();
        return WriteFixtures.writeRows(tempDir.resolve(fileName), IDS, options, rows);
    }

    /**
     * Scans the chunk of {@code column} in the first row group of {@code file}, read whole from its first page to its
     * end.
     */
    private static Optional<LearnedPages> scanChunk(Path file, ColumnPath column) throws IOException {
        MemorySegment bytes = MemorySegment.ofArray(Files.readAllBytes(file));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FirstChunk first = FirstChunk.read(source, column);
            ChunkMeta chunk = first.chunk();
            SchemaNode.Primitive leaf = first.leaf();
            MemorySegment whole = bytes.asSlice(chunk.chunkStart(), chunk.totalCompressedSize());
            return PageHeaderScan.scan(whole, first.rowCount(), leaf.kind(), leaf.logicalType());
        }
    }

    /**
     * The pages declared by the offset index of the chunk of {@code column} in the first row group of {@code file},
     * placed from the chunk start.
     */
    private static List<PageLocation> declaredPagesInChunk(Path file, ColumnPath column) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ChunkMeta chunk = FirstChunk.read(source, column).chunk();
            long offsetIndexOffset = chunk.offsetIndexOffset().orElseThrow();
            OffsetIndex declared = ParquetFormat.readOffsetIndex(source, offsetIndexOffset, chunk.offsetIndexLength());
            return declared.pageLocations().stream()
                    .map(page -> placedFrom(chunk.chunkStart(), page))
                    .toList();
        }
    }

    private static PageLocation placedFrom(long chunkStart, PageLocation page) {
        return new PageLocation(page.offset() - chunkStart, page.compressedPageSize(), page.firstRowIndex());
    }

    /** The pages of {@code pages} as an offset index places them, from the start of the chunk. */
    private static List<PageLocation> locationsOf(LearnedPages pages) {
        return eachPage(pages, page -> new PageLocation(pages.offset(page), pages.size(page), pages.firstRow(page)));
    }

    private static List<Long> firstRowsOf(List<PageLocation> pages) {
        return pages.stream().map(PageLocation::firstRowIndex).toList();
    }

    private static <T> List<T> eachPage(LearnedPages pages, IntFunction<T> accessor) {
        return IntStream.range(0, pages.pageCount()).mapToObj(accessor).toList();
    }

    private static long totalSize(LearnedPages pages) {
        return IntStream.range(0, pages.pageCount()).mapToLong(pages::size).sum();
    }

    private static long longOf(Optional<MemorySegment> bound) {
        return bound.orElseThrow().get(INT64, 0);
    }

    /**
     * The chunk of a column in the first row group of a file. The footer places it with the rules applied by the
     * reader.
     *
     * @param chunk the chunk of the column
     * @param leaf the leaf of the column in the schema of the file
     * @param rowCount the rows of the row group
     */
    private record FirstChunk(
            @NonNull ChunkMeta chunk, @NonNull SchemaNode.Primitive leaf, long rowCount) {

        static FirstChunk read(ByteRangeSource source, ColumnPath column) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            LeafIndex leaves = LeafIndex.of(schema);
            CompactFooter compact = CompactFooter.encode(footer, leaves);
            SchemaNode.Primitive leaf =
                    (SchemaNode.Primitive) schema.find(column).orElseThrow();
            return new FirstChunk(compact.chunk(0, leaves.ordinalOf(column)), leaf, compact.numRows(0));
        }
    }
}
