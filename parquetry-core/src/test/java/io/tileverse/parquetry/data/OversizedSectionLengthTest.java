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
import java.util.OptionalLong;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.WriteOptions.BloomFilterConfig;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.BloomFilterHeader;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.FooterRewrite;
import io.tileverse.parquetry.testsupport.RecordingByteRangeSource;

/**
 * A page index or a bloom filter declared longer than the file holds is left unread, and a page index declaring more
 * content than its own bytes too: no memory of the declared size is allocated, and a filter selects the rows of a full
 * scan.
 *
 * <p>The file has 300 rows in three pages: an integer column holding the row number, with both page indexes and a bloom
 * filter. The cases over that file edit the footer, the bloom filter header, or the first bytes of a page index, to
 * declare 2 GB. The case of a page index laid over the column chunk writes a file of its own: 30,000 rows, a chunk far
 * longer than the bytes around it.
 */
class OversizedSectionLengthTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final int ROWS = 300;
    private static final int ROWS_PER_PAGE = 100;
    private static final int TWO_GB = Integer.MAX_VALUE;
    private static final int SQUARES = 30_000;
    /** The first byte after the magic number, where the first column chunk begins. */
    private static final long DATA_START = 4L;

    private static final int TWO_GB_OF_BLOCKS = Integer.MAX_VALUE - 31;

    @TempDir
    Path tempDir;

    private Path written;

    @BeforeEach
    void writeFile() throws IOException {
        written = tempDir.resolve("written.parquet");
        writeRowNumbers(written);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("oversizedSections")
    void filterSelectsTheRowsOfAFullScan(String name, UnaryOperator<ColumnChunk> edit) throws IOException {
        Path file = FooterRewrite.rewrite(written, tempDir.resolve("edited.parquet"), eachChunk(edit));

        assertFiltersSelectTheirRows(file);
    }

    static Stream<Arguments> oversizedSections() {
        OptionalInt twoGb = OptionalInt.of(TWO_GB);
        UnaryOperator<ColumnChunk> columnIndex = chunk -> withIndexLengths(chunk, chunk.offsetIndexLength(), twoGb);
        UnaryOperator<ColumnChunk> offsetIndex = chunk -> withIndexLengths(chunk, twoGb, chunk.columnIndexLength());
        UnaryOperator<ColumnChunk> bothIndexes = chunk -> withIndexLengths(chunk, twoGb, twoGb);
        UnaryOperator<ColumnChunk> bloomFilter = chunk -> withBloomLength(chunk, OptionalLong.of(TWO_GB));
        return Stream.of(
                Arguments.of("a column index of 2 GB", columnIndex),
                Arguments.of("an offset index of 2 GB", offsetIndex),
                Arguments.of("both page indexes of 2 GB", bothIndexes),
                Arguments.of("a bloom filter of 2 GB", bloomFilter));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("pageIndexesDeclaringOversizedContent")
    void pageIndexDeclaringMoreContentThanItHoldsIsLeftUnread(String name, PageIndex index, byte[] firstBytes)
            throws IOException {
        Path file = Files.copy(written, tempDir.resolve("edited.parquet"));
        overwriteTheFirstBytesOf(index, firstBytes, file);

        assertFiltersSelectTheirRows(file);
    }

    static Stream<Arguments> pageIndexesDeclaringOversizedContent() {
        byte[] listOfNullFlags = {0x19, (byte) 0xF1};
        byte[] oneNullFlagThenOneLowerBound = {0x19, 0x11, 0x02, 0x19, 0x18};
        byte[] listOfPageLocations = {0x19, (byte) 0xFC};
        return Stream.of(
                Arguments.of(
                        "a column index listing 2 GB of null flags",
                        PageIndex.COLUMN_INDEX,
                        followedByTwoGb(listOfNullFlags)),
                Arguments.of(
                        "a column index with a lower bound of 2 GB",
                        PageIndex.COLUMN_INDEX,
                        followedByTwoGb(oneNullFlagThenOneLowerBound)),
                Arguments.of(
                        "an offset index listing 2 GB of pages",
                        PageIndex.OFFSET_INDEX,
                        followedByTwoGb(listOfPageLocations)));
    }

    @Test
    void bloomFilterHeaderDeclaringABitsetOf2GbIsLeftUnread() throws IOException {
        UnaryOperator<ColumnChunk> lengthLeftToTheHeader = chunk -> withBloomLength(chunk, OptionalLong.empty());
        Path file = FooterRewrite.rewrite(written, tempDir.resolve("edited.parquet"), eachChunk(lengthLeftToTheHeader));
        declareABloomBitsetOf(TWO_GB_OF_BLOCKS, file);

        assertFiltersSelectTheirRows(file);
    }

    // A page index declared over the column chunk is left unread, and the filter still selects its rows.
    @ParameterizedTest(name = "{0}")
    @MethodSource("pageIndexesLaidOverTheColumnChunk")
    void pageIndexOverTheChunkIsUnread(String name, UnaryOperator<ColumnChunk> edit) throws IOException {
        Path squares = tempDir.resolve("squares.parquet");
        writeSquares(squares);
        Path file = FooterRewrite.rewrite(squares, tempDir.resolve("edited.parquet"), eachChunk(edit));
        ColumnMetaData chunk = onlyChunk(file).metaData().orElseThrow();
        long bloomStart = chunk.bloomFilterOffset().orElseThrow();
        long bloomEnd = bloomStart + chunk.bloomFilterLength().orElseThrow();
        long firstOfTheLastFifty = square(SQUARES - 50);
        Predicate lastFifty = new Predicate.GtEq(V, new Value.LongVal(firstOfTheLastFifty));
        ReadOptions withoutBloomFilter =
                ReadOptions.builder().useBloomFilter(false).build();

        try (RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(file))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            source.reset();

            assertThat(reader.count(lastFifty, withoutBloomFilter)).isEqualTo(50L);
            // The bloom filter is not consulted: its bytes are read only as part of the index laid over the file.
            assertThat(source.readAnyByteIn(bloomStart, bloomEnd))
                    .as("bytes read from the bloom filter")
                    .isFalse();
        }
    }

    /** Each index is declared from the first byte after the magic number to the end of the bloom filter. */
    static Stream<Arguments> pageIndexesLaidOverTheColumnChunk() {
        UnaryOperator<ColumnChunk> columnIndex =
                chunk -> withColumnIndexAt(chunk, DATA_START, lengthToTheEndOfTheBloomFilter(chunk));
        UnaryOperator<ColumnChunk> offsetIndex =
                chunk -> withOffsetIndexAt(chunk, DATA_START, lengthToTheEndOfTheBloomFilter(chunk));
        return Stream.of(
                Arguments.of("a column index over the column chunk", columnIndex),
                Arguments.of("an offset index over the column chunk", offsetIndex));
    }

    private static int lengthToTheEndOfTheBloomFilter(ColumnChunk chunk) {
        ColumnMetaData meta = chunk.metaData().orElseThrow();
        long bloomEnd = meta.bloomFilterOffset().orElseThrow()
                + meta.bloomFilterLength().orElseThrow();
        return Math.toIntExact(bloomEnd - DATA_START);
    }

    private static ColumnChunk withColumnIndexAt(ColumnChunk chunk, long offset, int length) {
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                chunk.metaData(),
                chunk.offsetIndexOffset(),
                chunk.offsetIndexLength(),
                OptionalLong.of(offset),
                OptionalInt.of(length),
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    private static ColumnChunk withOffsetIndexAt(ColumnChunk chunk, long offset, int length) {
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                chunk.metaData(),
                OptionalLong.of(offset),
                OptionalInt.of(length),
                chunk.columnIndexOffset(),
                chunk.columnIndexLength(),
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    /**
     * Writes the squares of the row numbers in three pages, with a bloom filter: a column chunk far larger than the
     * page indexes, the bloom filter and the footer together.
     */
    private void writeSquares(Path target) throws IOException {
        SchemaNode.Primitive v = new SchemaNode.Primitive(
                "v", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(v), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(SQUARES / 3)
                .bloomFilter("v", new BloomFilterConfig(0.01, 1_000L))
                .build();
        try (OutputStream out = Files.newOutputStream(target);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long row = 0; row < SQUARES; row++) {
                appender.setLong(V, square(row));
                appender.endRow();
            }
        }
    }

    private static long square(long row) {
        return row * row;
    }

    private static void assertFiltersSelectTheirRows(Path file) {
        Predicate oneRow = new Predicate.Eq(V, new Value.LongVal(250L));
        Predicate lastFifty = new Predicate.GtEq(V, new Value.LongVal(250L));
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(oneRow, ReadOptions.DEFAULTS))
                    .as("count v = 250")
                    .isEqualTo(1L);
            assertThat(reader.count(lastFifty, ReadOptions.DEFAULTS))
                    .as("count v >= 250")
                    .isEqualTo(50L);
            assertThat(rowsRead(reader, lastFifty)).as("rows read v >= 250").isEqualTo(50L);
        }
    }

    private static long rowsRead(ParquetFileReader reader, Predicate predicate) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private enum PageIndex {
        COLUMN_INDEX,
        OFFSET_INDEX
    }

    /** The Thrift bytes of {@code header}, then the announced size: {@code 2^31 - 1} as a varint. */
    private static byte[] followedByTwoGb(byte[] header) {
        byte[] twoGb = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x07};
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        bytes.writeBytes(header);
        bytes.writeBytes(twoGb);
        return bytes.toByteArray();
    }

    private static void overwriteTheFirstBytesOf(PageIndex index, byte[] firstBytes, Path file) throws IOException {
        ColumnChunk chunk = onlyChunk(file);
        long indexOffset =
                switch (index) {
                    case COLUMN_INDEX -> chunk.columnIndexOffset().orElseThrow();
                    case OFFSET_INDEX -> chunk.offsetIndexOffset().orElseThrow();
                };
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(firstBytes), indexOffset);
        }
    }

    /** Overwrites the header of the bloom filter of the column in place. */
    private static void declareABloomBitsetOf(int bytes, Path file) throws IOException {
        long bloomOffset =
                onlyChunk(file).metaData().orElseThrow().bloomFilterOffset().orElseThrow();
        BloomFilterHeader header = new BloomFilterHeader(
                bytes,
                BloomFilterHeader.Algorithm.SPLIT_BLOCK,
                BloomFilterHeader.HashStrategy.XXHASH,
                BloomFilterHeader.Compression.UNCOMPRESSED);
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        ParquetFormat.writeBloomFilterHeader(encoded, header);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(encoded.toByteArray()), bloomOffset);
        }
    }

    private static ColumnChunk onlyChunk(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            return footer.rowGroups().get(0).columns().get(0);
        }
    }

    private static UnaryOperator<FileMetaData> eachChunk(UnaryOperator<ColumnChunk> edit) {
        return footer -> {
            List<RowGroup> rowGroups = new ArrayList<>();
            for (RowGroup rowGroup : footer.rowGroups()) {
                List<ColumnChunk> columns =
                        rowGroup.columns().stream().map(edit).toList();
                rowGroups.add(new RowGroup(
                        columns,
                        rowGroup.totalByteSize(),
                        rowGroup.numRows(),
                        rowGroup.sortingColumns(),
                        rowGroup.fileOffset(),
                        rowGroup.totalCompressedSize(),
                        rowGroup.ordinal()));
            }
            return new FileMetaData(
                    footer.version(),
                    footer.schema(),
                    footer.numRows(),
                    rowGroups,
                    footer.keyValueMetadata(),
                    footer.createdBy(),
                    footer.columnOrders(),
                    footer.encryptionAlgorithm(),
                    footer.footerSigningKeyMetadata());
        };
    }

    private static ColumnChunk withIndexLengths(
            ColumnChunk chunk, OptionalInt offsetIndexLength, OptionalInt columnIndexLength) {
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                chunk.metaData(),
                chunk.offsetIndexOffset(),
                offsetIndexLength,
                chunk.columnIndexOffset(),
                columnIndexLength,
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    private static ColumnChunk withBloomLength(ColumnChunk chunk, OptionalLong bloomFilterLength) {
        ColumnMetaData meta = chunk.metaData().orElseThrow();
        ColumnMetaData edited = new ColumnMetaData(
                meta.type(),
                meta.encodings(),
                meta.pathInSchema(),
                meta.codec(),
                meta.numValues(),
                meta.totalUncompressedSize(),
                meta.totalCompressedSize(),
                meta.keyValueMetadata(),
                meta.dataPageOffset(),
                meta.indexPageOffset(),
                meta.dictionaryPageOffset(),
                meta.statistics(),
                meta.encodingStats(),
                meta.bloomFilterOffset(),
                bloomFilterLength,
                meta.sizeStatistics(),
                meta.geospatialStatistics());
        return new ColumnChunk(
                chunk.filePath(),
                chunk.fileOffset(),
                Optional.of(edited),
                chunk.offsetIndexOffset(),
                chunk.offsetIndexLength(),
                chunk.columnIndexOffset(),
                chunk.columnIndexLength(),
                chunk.cryptoMetadata(),
                chunk.encryptedColumnMetadata());
    }

    private void writeRowNumbers(Path target) throws IOException {
        SchemaNode.Primitive v = new SchemaNode.Primitive(
                "v", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(v), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(ROWS_PER_PAGE)
                .bloomFilter("v", BloomFilterConfig.defaults())
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
}
