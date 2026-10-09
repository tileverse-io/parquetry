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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.data.WriteOptions.ParquetVersion;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * A page rejected by its codec makes the file malformed: compressed bytes overwritten with garbage, or a header
 * declaring fewer bytes than the page decompresses to.
 *
 * <p>The file has 1000 rows of one required integer column holding seven distinct values: a dictionary page followed by
 * data pages.
 */
class CorruptPagePayloadTest {

    private static final ColumnPath V = ColumnPath.of("v");
    private static final int ROWS = 1000;
    private static final int ROWS_PER_PAGE = 250;

    @TempDir
    Path tempDir;

    @ParameterizedTest(name = "{0}, {1} page")
    @MethodSource("compressedPages")
    void pageOfGarbageIsMalformed(String codecName, PageType pageType, Compression codec) throws IOException {
        Path file = write(codec, ParquetVersion.V2_0);
        Page page = firstPageOf(pageType, file);

        overwriteWithGarbage(page, file);

        assertThatThrownBy(() -> readAllRows(file))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column v")
                .hasMessageContaining("cannot be decompressed");
    }

    static Stream<Arguments> compressedPages() {
        List<Arguments> cases = new ArrayList<>();
        for (PageType pageType : List.of(PageType.DICTIONARY_PAGE, PageType.DATA_PAGE_V2)) {
            cases.add(Arguments.of("SNAPPY", pageType, Compression.snappy()));
            cases.add(Arguments.of("ZSTD", pageType, Compression.zstd(3)));
            cases.add(Arguments.of("LZ4_RAW", pageType, Compression.lz4Raw()));
            cases.add(Arguments.of("GZIP", pageType, Compression.gzip()));
        }
        return cases.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("uncompressedPages")
    void pageWithExtraByteIsMalformed(PageType pageType, ParquetVersion version) throws IOException {
        Path file = write(Compression.uncompressed(), version);
        Page page = firstPageOf(pageType, file);

        declareOneByteLess(page, file);

        assertThatThrownBy(() -> readAllRows(file))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Column v")
                .hasMessageContaining("cannot be decompressed");
    }

    /** The pages copied through the codec: a dictionary page, and a data page of the first version of the format. */
    static Stream<Arguments> uncompressedPages() {
        return Stream.of(
                Arguments.of(PageType.DICTIONARY_PAGE, ParquetVersion.V2_0),
                Arguments.of(PageType.DATA_PAGE, ParquetVersion.V1_1));
    }

    private static long readAllRows(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.count();
            }
        }
    }

    /** A page of the column chunk: where its header begins, the header, and where its payload begins. */
    private record Page(long headerStart, PageHeader header, long payloadStart) {}

    private static Page firstPageOf(PageType type, Path file) throws IOException {
        for (Page page : pagesOf(file)) {
            if (page.header().type() == type) {
                return page;
            }
        }
        throw new AssertionError("the column chunk holds no page of type " + type);
    }

    private static List<Page> pagesOf(Path file) throws IOException {
        ColumnMetaData chunk = onlyChunk(file);
        long chunkStart = chunk.dictionaryPageOffset().orElse(chunk.dataPageOffset());
        byte[] bytes = new byte[Math.toIntExact(chunk.totalCompressedSize())];
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.READ)) {
            channel.read(ByteBuffer.wrap(bytes), chunkStart);
        }
        List<Page> pages = new ArrayList<>();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        while (in.available() > 0) {
            int headerStart = bytes.length - in.available();
            PageHeader header = ParquetFormat.readPageHeader(in);
            int payloadStart = bytes.length - in.available();
            pages.add(new Page(chunkStart + headerStart, header, chunkStart + payloadStart));
            in.skipNBytes(header.compressedPageSize());
        }
        return pages;
    }

    private static void overwriteWithGarbage(Page page, Path file) throws IOException {
        byte[] garbage = new byte[page.header().compressedPageSize()];
        Arrays.fill(garbage, (byte) 0xFF);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(garbage), page.payloadStart());
        }
    }

    /** Rewrites the header of {@code page} in place, declaring one byte less than the page holds. */
    private static void declareOneByteLess(Page page, Path file) throws IOException {
        PageHeader header = page.header();
        PageHeader edited = new PageHeader(
                header.type(),
                header.uncompressedPageSize() - 1,
                header.compressedPageSize(),
                header.crc(),
                header.dataPageHeader(),
                header.indexPageHeader(),
                header.dictionaryPageHeader(),
                header.dataPageHeaderV2());
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(encoded, edited);
        long headerLength = page.payloadStart() - page.headerStart();
        assertThat((long) encoded.size()).as("length of the edited page header").isEqualTo(headerLength);
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.write(ByteBuffer.wrap(encoded.toByteArray()), page.headerStart());
        }
    }

    private static ColumnMetaData onlyChunk(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            FileMetaData footer = ParquetFormat.readFooter(source);
            RowGroup onlyRowGroup = footer.rowGroups().get(0);
            ColumnChunk onlyColumn = onlyRowGroup.columns().get(0);
            return onlyColumn.metaData().orElseThrow();
        }
    }

    private Path write(Compression codec, ParquetVersion version) throws IOException {
        SchemaNode.Primitive v = new SchemaNode.Primitive(
                "v", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(v), Optional.empty(), -1);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(ROWS_PER_PAGE)
                .defaultCompression(codec)
                .parquetVersion(version)
                .build();
        Path file = tempDir.resolve("written.parquet");
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, new ParquetSchema(root), options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (long row = 0; row < ROWS; row++) {
                appender.setLong(V, 1_000_000_007L * (row % 7));
                appender.endRow();
            }
        }
        return file;
    }
}
