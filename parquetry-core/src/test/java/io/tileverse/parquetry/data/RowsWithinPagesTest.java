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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A page of a repeated column starts at the first value of a row, as required of version 2 data pages and of pages
 * under a page index. A row holding more values than the page limit stays whole in one page.
 *
 * <p>The file has 400 rows in one row group, with pages limited to 100 values: a row number, and a list of integers of
 * 250, 10, 5, 300 and 3 elements in turn. Element {@code k} of the list of row {@code i} is {@code 1000 * i + k}.
 */
class RowsWithinPagesTest {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath NUMBERS = ColumnPath.of("numbers");
    private static final int ROWS = 400;
    private static final int PAGE_VALUE_LIMIT = 100;
    private static final int[] LIST_SIZES = {250, 10, 5, 300, 3};
    private static final int NUMBERS_CHUNK = 1;
    private static final int LIST_REPETITION_LEVEL = 1;
    private static final int HEADER_PROBE_BYTES = 128;

    @TempDir
    static Path tempDir;

    private static Path file;

    @BeforeAll
    static void writeFile() throws IOException {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive element = new SchemaNode.Primitive(
                "element", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        Optional<LogicalType> listType = Optional.of(new LogicalType.ListType());
        SchemaNode.Group numbers =
                new SchemaNode.Group("numbers", Repetition.OPTIONAL, List.of(repeated), listType, -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, numbers), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(PAGE_VALUE_LIMIT)
                .build();
        file = tempDir.resolve("lists.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (int row = 0; row < ROWS; row++) {
                appender.setLong(ID, row);
                appender.beginList(NUMBERS);
                for (long number : numbersOf(row)) {
                    appender.addLong(number);
                }
                appender.endList();
                appender.endRow();
            }
        }
    }

    @Test
    void eachPageOfTheListColumnHoldsWholeRows() {
        OffsetIndex offsetIndex = offsetIndexOfTheListColumn();
        List<DataPageHeaderV2> pages = pageHeadersAt(offsetIndex.pageLocations());

        assertThat(pages).as("pages of the list column").hasSizeGreaterThan(1);
        long rowsBeforeThePage = 0L;
        for (int page = 0; page < pages.size(); page++) {
            assertThat(offsetIndex.pageLocations().get(page).firstRowIndex())
                    .as("first row of page %d", page)
                    .isEqualTo(rowsBeforeThePage);
            assertThat(pages.get(page).numRows()).as("rows of page %d", page).isPositive();
            rowsBeforeThePage += pages.get(page).numRows();
        }
        assertThat(rowsBeforeThePage).as("rows in the pages").isEqualTo(ROWS);
    }

    @Test
    void eachPageOfTheListColumnOpensWithTheFirstValueOfARow() {
        List<PageLocation> pages = offsetIndexOfTheListColumn().pageLocations();

        for (int page = 0; page < pages.size(); page++) {
            int[] repetitionLevels = repetitionLevelsOfThePageAt(pages.get(page));
            assertThat(repetitionLevels[0])
                    .as("repetition level of the first value of page %d", page)
                    .isZero();
        }
    }

    @Test
    void rowLargerThanThePageLimitStaysInOnePage() {
        List<DataPageHeaderV2> pages =
                pageHeadersAt(offsetIndexOfTheListColumn().pageLocations());

        assertThat(pages).anyMatch(page -> page.numValues() >= LIST_SIZES[3]);
    }

    @Test
    void filterSkippingPagesReadsEachListWhole() {
        Predicate tenRows = new Predicate.And(List.of(
                new Predicate.GtEq(ID, new Value.LongVal(250L)), new Predicate.LtEq(ID, new Value.LongVal(259L))));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            List<List<Long>> lists = numbersRead(reader, tenRows);

            assertThat(lists).hasSize(10);
            for (int offset = 0; offset < lists.size(); offset++) {
                assertThat(lists.get(offset)).as("list of row %d", 250 + offset).isEqualTo(numbersOf(250 + offset));
            }
        }
    }

    @Test
    void fullReadReturnsEachListAsWritten() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            List<List<Long>> lists = numbersRead(reader, Predicate.ALWAYS_TRUE);

            assertThat(lists).hasSize(ROWS);
            for (int row = 0; row < ROWS; row++) {
                assertThat(lists.get(row)).as("list of row %d", row).isEqualTo(numbersOf(row));
            }
        }
    }

    private static List<Long> numbersOf(int row) {
        int size = LIST_SIZES[row % LIST_SIZES.length];
        List<Long> numbers = new ArrayList<>(size);
        for (int k = 0; k < size; k++) {
            numbers.add(1000L * row + k);
        }
        return numbers;
    }

    private static List<List<Long>> numbersRead(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(RowsWithinPagesTest::numbersOf).toList();
        }
    }

    private static List<Long> numbersOf(ParquetRecord row) {
        List<?> numbers = (List<?>) row.get(NUMBERS);
        List<Long> copied = new ArrayList<>(numbers.size());
        for (Object number : numbers) {
            copied.add((Long) number);
        }
        return copied;
    }

    private static OffsetIndex offsetIndexOfTheListColumn() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            ColumnChunk chunk = footer.rowGroups().get(0).columns().get(NUMBERS_CHUNK);
            long offset = chunk.offsetIndexOffset().orElseThrow();
            int length = chunk.offsetIndexLength().orElseThrow();
            return ParquetFormat.readOffsetIndex(source, offset, length);
        }
    }

    private static List<DataPageHeaderV2> pageHeadersAt(List<PageLocation> pages) {
        List<DataPageHeaderV2> headers = new ArrayList<>(pages.size());
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            for (PageLocation page : pages) {
                headers.add(dataPageHeaderAt(source, page.offset()));
            }
        }
        return headers;
    }

    private static DataPageHeaderV2 dataPageHeaderAt(ByteRangeSource source, long offset) {
        int probed = (int) Math.min(HEADER_PROBE_BYTES, source.size() - offset);
        byte[] bytes = new byte[probed];
        source.readFully(offset, MemorySegment.ofArray(bytes));
        PageHeader header = ParquetFormat.readPageHeader(new ByteArrayInputStream(bytes));
        return header.dataPageHeaderV2().orElseThrow();
    }

    private static int[] repetitionLevelsOfThePageAt(PageLocation location) {
        byte[] page = new byte[location.compressedPageSize()];
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            source.readFully(location.offset(), MemorySegment.ofArray(page));
        }
        ByteArrayInputStream pageBytes = new ByteArrayInputStream(page);
        PageHeader pageHeader = ParquetFormat.readPageHeader(pageBytes);
        DataPageHeaderV2 header = pageHeader.dataPageHeaderV2().orElseThrow();
        int headerLength = page.length - pageBytes.available();
        MemorySegment afterTheHeader = MemorySegment.ofArray(page).asSlice(headerLength);
        LevelDecoder decoder = LevelDecoder.forMaxLevel(LIST_REPETITION_LEVEL, "repetition levels");
        decoder.load(afterTheHeader.asSlice(0L, header.repetitionLevelsByteLength()));
        int[] levels = new int[header.numValues()];
        decoder.decode(levels.length, levels, 0);
        return levels;
    }
}
