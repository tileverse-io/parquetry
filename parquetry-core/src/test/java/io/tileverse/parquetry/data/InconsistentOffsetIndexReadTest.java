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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A file with an offset index naming its pages out of order, by their rows or by their bytes, reads as a file without
 * that index: a filter selects the rows of a full scan.
 *
 * <p>The file has 300 rows in three pages of 100: an integer column holding the row number. Each case overwrites its
 * offset index in place, with the first rows or the byte locations of the last two pages swapped.
 */
class InconsistentOffsetIndexReadTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final int ROWS = 300;
    private static final int ROWS_PER_PAGE = 100;
    private static final ReadOptions METADATA_PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFile() throws IOException {
        file = tempDir.resolve("pages-out-of-order.parquet");
        writeRowNumbers(file);
    }

    @ParameterizedTest(name = "{0}: {1}")
    @MethodSource("editsAndFilters")
    void filterSelectsTheRowsOfAFullScan(
            String edit, String filter, UnaryOperator<OffsetIndex> swap, Predicate predicate, long expected)
            throws IOException {
        overwriteOffsetIndex(file, swap);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(predicate, METADATA_PRUNING_OFF))
                    .as("count without pruning")
                    .isEqualTo(expected);
            assertThat(reader.count(predicate, ReadOptions.DEFAULTS))
                    .as("count")
                    .isEqualTo(expected);
            assertThat(rowsRead(reader, predicate)).as("rows read").isEqualTo(expected);
            assertThat(rowsReadInBatches(reader, predicate))
                    .as("rows read in batches")
                    .isEqualTo(expected);
        }
    }

    static Stream<Arguments> editsAndFilters() {
        UnaryOperator<OffsetIndex> firstRows = InconsistentOffsetIndexReadTest::withTheLastTwoFirstRowsSwapped;
        UnaryOperator<OffsetIndex> byteLocations = InconsistentOffsetIndexReadTest::withTheLastTwoByteLocationsSwapped;
        Stream<Arguments> outOfRowOrder = filtersOver("first rows swapped", firstRows);
        Stream<Arguments> outOfFileOrder = filtersOver("byte locations swapped", byteLocations);
        return Stream.concat(outOfRowOrder, outOfFileOrder);
    }

    private static Stream<Arguments> filtersOver(String edit, UnaryOperator<OffsetIndex> swap) {
        Predicate middlePage = new Predicate.And(List.of(
                new Predicate.GtEq(V, new Value.LongVal(100L)), new Predicate.LtEq(V, new Value.LongVal(199L))));
        return Stream.of(
                Arguments.of(edit, "v BETWEEN 100 AND 199", swap, middlePage, 100L),
                Arguments.of(edit, "v >= 250", swap, new Predicate.GtEq(V, new Value.LongVal(250L)), 50L),
                Arguments.of(edit, "v < 10", swap, new Predicate.Lt(V, new Value.LongVal(10L)), 10L),
                Arguments.of(edit, "v IS NOT NULL", swap, new Predicate.IsNotNull(V), (long) ROWS),
                Arguments.of(edit, "v IS NULL", swap, new Predicate.IsNull(V), 0L));
    }

    private static long rowsRead(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static long rowsReadInBatches(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return batches.mapToLong(ParquetRecordBatch::rowCount).sum();
        }
    }

    private void writeRowNumbers(Path target) throws IOException {
        SchemaNode.Primitive v = new SchemaNode.Primitive(
                "v", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(v), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(ROWS_PER_PAGE)
                .build();
        try (OutputStream out = Files.newOutputStream(target);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long row = 0; row < ROWS; row++) {
                appender.setLong(V, row);
                appender.endRow();
            }
        }
    }

    /** Overwrites the offset index of the column in place with its pages as edited by {@code swap}. */
    private static void overwriteOffsetIndex(Path target, UnaryOperator<OffsetIndex> swap) throws IOException {
        ColumnChunk chunk = onlyChunk(target);
        long sectionOffset = chunk.offsetIndexOffset().orElseThrow();
        int sectionLength = chunk.offsetIndexLength().orElseThrow();
        OffsetIndex written = readOffsetIndex(target, sectionOffset, sectionLength);
        assertThat(written.pageLocations()).as("pages written").hasSize(3);

        byte[] edited = serialize(swap.apply(written));

        assertThat(edited).as("bytes of the edited index").hasSizeLessThanOrEqualTo(sectionLength);
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(edited), sectionOffset);
        }
    }

    private static ColumnChunk onlyChunk(Path target) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(target)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            return footer.rowGroups().get(0).columns().get(0);
        }
    }

    private static OffsetIndex readOffsetIndex(Path target, long offset, int length) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(target)) {
            return ParquetFormat.readOffsetIndex(source, offset, length);
        }
    }

    private static OffsetIndex withTheLastTwoFirstRowsSwapped(OffsetIndex index) {
        List<PageLocation> pages = new ArrayList<>(index.pageLocations());
        PageLocation second = pages.get(1);
        PageLocation third = pages.get(2);
        pages.set(1, new PageLocation(second.offset(), second.compressedPageSize(), third.firstRowIndex()));
        pages.set(2, new PageLocation(third.offset(), third.compressedPageSize(), second.firstRowIndex()));
        return new OffsetIndex(pages, index.unencodedByteArrayDataBytes());
    }

    private static OffsetIndex withTheLastTwoByteLocationsSwapped(OffsetIndex index) {
        List<PageLocation> pages = new ArrayList<>(index.pageLocations());
        PageLocation second = pages.get(1);
        PageLocation third = pages.get(2);
        pages.set(1, new PageLocation(third.offset(), third.compressedPageSize(), second.firstRowIndex()));
        pages.set(2, new PageLocation(second.offset(), second.compressedPageSize(), third.firstRowIndex()));
        return new OffsetIndex(pages, index.unencodedByteArrayDataBytes());
    }

    private static byte[] serialize(OffsetIndex index) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ParquetFormat.writeOffsetIndex(bytes, index);
        return bytes.toByteArray();
    }
}
