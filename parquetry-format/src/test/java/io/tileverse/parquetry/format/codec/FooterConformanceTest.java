/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.format.codec;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;

/**
 * Conformance test: reads footer via parquetry and compares against the parquet-avro oracle for each of the 10
 * representative fixture files.
 *
 * <p>The test verifies row counts, row group counts, per-group row count, total byte size, and column count - i.e. all
 * metadata that is structurally load-bearing for page reading.
 *
 * <p>The oracle reads the raw footer bytes directly (mirroring the Parquet file layout), then decodes them via
 * {@link ParquetMetadataConverter}. This avoids loading Hadoop's filesystem and mapreduce classes, which are
 * incompatible with Java 24+.
 */
class FooterConformanceTest {

    @ParameterizedTest(name = "{0}")
    @MethodSource("fixtures")
    void footerMatchesParquetAvroOracle(String name, TestFiles.Fixture fix, @TempDir Path tmp) throws Exception {
        Path file = fix.generate(tmp);
        if (file == null) {
            // Fixture explicitly skipped (e.g. BYTE_STREAM_SPLIT not configurable)
            return;
        }

        FileMetaData parquetryFooter = readParquetryFooter(file);
        ParquetMetadata oracle = readOracleFooter(file);

        long oracleRowCount =
                oracle.getBlocks().stream().mapToLong(b -> b.getRowCount()).sum();
        assertThat(parquetryFooter.numRows()).as("total row count for %s", name).isEqualTo(oracleRowCount);

        assertThat(parquetryFooter.rowGroups())
                .as("row group count for %s", name)
                .hasSize(oracle.getBlocks().size());

        for (int i = 0; i < parquetryFooter.rowGroups().size(); i++) {
            var pBlock = parquetryFooter.rowGroups().get(i);
            var oBlock = oracle.getBlocks().get(i);
            assertThat(pBlock.numRows())
                    .as("row group[%d] row count for %s", i, name)
                    .isEqualTo(oBlock.getRowCount());
            assertThat(pBlock.totalByteSize())
                    .as("row group[%d] totalByteSize for %s", i, name)
                    .isEqualTo(oBlock.getTotalByteSize());
            assertThat(pBlock.columns())
                    .as("row group[%d] column count for %s", i, name)
                    .hasSize(oBlock.getColumns().size());
        }
    }

    private FileMetaData readParquetryFooter(Path file) throws Exception {
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            return ParquetFormat.readFooter(reader);
        }
    }

    /**
     * Reads the oracle {@link ParquetMetadata} by extracting the raw footer bytes from the Parquet file layout and
     * decoding them via {@link ParquetMetadataConverter}.
     *
     * <p>Avoids Hadoop filesystem and mapreduce class loading, which is incompatible with Java 24+ (Subject.getSubject
     * removed, FileInputFormat classloading issues).
     */
    private ParquetMetadata readOracleFooter(Path file) throws Exception {
        byte[] footerBytes = extractRawFooterBytes(file);
        ByteArrayInputStream stream = new ByteArrayInputStream(footerBytes);
        return new ParquetMetadataConverter().readParquetMetadata(stream, ParquetMetadataConverter.NO_FILTER);
    }

    /**
     * Extracts the raw Thrift-encoded footer bytes from a Parquet file by reading the trailing 8 bytes (footer length +
     * magic) and then seeking to the footer start.
     */
    private byte[] extractRawFooterBytes(Path file) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "r")) {
            long size = raf.length();
            // Last 8 bytes: 4-byte little-endian footer length + 4-byte magic "PAR1"
            raf.seek(size - 8);
            byte[] tail = new byte[8];
            raf.readFully(tail);
            int footerLen = ByteBuffer.wrap(tail, 0, 4).order(LITTLE_ENDIAN).getInt();
            byte[] footer = new byte[footerLen];
            raf.seek(size - 8 - footerLen);
            raf.readFully(footer);
            return footer;
        }
    }

    static Stream<Arguments> fixtures() {
        return Stream.of(
                Arguments.of("flat-int-snappy", (TestFiles.Fixture) TestFiles::flatIntSnappy),
                Arguments.of("flat-string-gzip", (TestFiles.Fixture) TestFiles::flatStringGzip),
                Arguments.of("nested-map", (TestFiles.Fixture) TestFiles::nestedMap),
                Arguments.of("nested-list", (TestFiles.Fixture) TestFiles::nestedList),
                Arguments.of("nullable-flat", (TestFiles.Fixture) TestFiles::nullableFlat),
                Arguments.of("multi-rowgroup", (TestFiles.Fixture) TestFiles::multiRowGroup),
                Arguments.of("dictionary-encoded", (TestFiles.Fixture) TestFiles::dictionaryEncoded),
                Arguments.of("delta-encoded", (TestFiles.Fixture) TestFiles::deltaEncoded),
                Arguments.of("byte-stream-split", (TestFiles.Fixture) TestFiles::byteStreamSplit),
                Arguments.of("zstd-compressed", (TestFiles.Fixture) TestFiles::zstdCompressed));
    }
}
