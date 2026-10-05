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

/**
 * A page index or a bloom filter declared longer than the file holds is left unread: no buffer of the declared length
 * is allocated, and a filter selects the rows of a full scan.
 *
 * <p>The file has 300 rows in three pages: an integer column holding the row number, with both page indexes and a bloom
 * filter. Each case edits the footer, or the bloom filter header, to declare a section of 2 GB.
 */
class OversizedSectionLengthTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final int ROWS = 300;
    private static final int ROWS_PER_PAGE = 100;
    private static final int TWO_GB = Integer.MAX_VALUE;
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

    @Test
    void bloomFilterHeaderDeclaringABitsetOf2GbIsLeftUnread() throws IOException {
        UnaryOperator<ColumnChunk> lengthLeftToTheHeader = chunk -> withBloomLength(chunk, OptionalLong.empty());
        Path file = FooterRewrite.rewrite(written, tempDir.resolve("edited.parquet"), eachChunk(lengthLeftToTheHeader));
        declareABloomBitsetOf(TWO_GB_OF_BLOCKS, file);

        assertFiltersSelectTheirRows(file);
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
