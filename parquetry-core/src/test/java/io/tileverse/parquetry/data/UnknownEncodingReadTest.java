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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.SinglePageParquetFile.Column;
import io.tileverse.parquetry.data.SinglePageParquetFile.PageVersion;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.UnsupportedFeatureException;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A file whose column chunk names an encoding unknown to parquetry, in its footer and in its data page header, still
 * opens: the footer's encoding references are informational. Decoding that page fails with
 * {@link UnsupportedFeatureException} naming the encoding and the column, while the other columns stay readable.
 */
class UnknownEncodingReadTest {

    private static final int UNKNOWN_ENCODING = 11;
    private static final int ROW_COUNT = 4;
    private static final ColumnPath READABLE = ColumnPath.of("readable");
    private static final ColumnPath UNDECODABLE = ColumnPath.of("undecodable");

    @TempDir
    Path tempDir;

    @ParameterizedTest
    @EnumSource(PageVersion.class)
    void fileOpensAndItsFooterKeepsTheKnownEncodings(PageVersion version) throws IOException {
        Path file = writeFile(version, UNKNOWN_ENCODING, PhysicalType.DOUBLE);

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            FileMetaData footer = ParquetFormat.readFooter(source);

            assertThat(reader.schema().leafColumns()).containsExactly(READABLE, UNDECODABLE);
            assertThat(footer.numRows()).isEqualTo(ROW_COUNT);
            assertThat(chunkOf(footer, UNDECODABLE).encodings()).containsExactly(Encoding.RLE);
            assertThat(chunkOf(footer, UNDECODABLE).encodingStats()).isEmpty();
            assertThat(chunkOf(footer, READABLE).encodings()).containsExactly(Encoding.RLE, Encoding.PLAIN);
        }
    }

    @ParameterizedTest
    @EnumSource(PageVersion.class)
    void otherColumnsOfTheFileStayReadable(PageVersion version) throws IOException {
        Path file = writeFile(version, UNKNOWN_ENCODING, PhysicalType.DOUBLE);
        Projection readableOnly = Projection.ofPhysical(List.of(READABLE));

        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> records = ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, readableOnly, ReadOptions.DEFAULTS)) {
            List<Double> values = records.map(row -> row.getDouble(READABLE)).toList();

            assertThat(values).containsExactly(0.0, 1.0, 2.0, 3.0);
        }
    }

    @ParameterizedTest
    @EnumSource(PageVersion.class)
    void rowApiFailsNamingTheUnknownEncodingAndTheColumn(PageVersion version) throws IOException {
        Path file = writeFile(version, UNKNOWN_ENCODING, PhysicalType.DOUBLE);

        assertThatThrownBy(() -> readAllRows(file))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("wire code " + UNKNOWN_ENCODING)
                .hasMessageContaining("column " + UNDECODABLE.dot());
    }

    @ParameterizedTest
    @EnumSource(PageVersion.class)
    void batchApiFailsNamingTheUnknownEncodingAndTheColumn(PageVersion version) throws IOException {
        Path file = writeFile(version, UNKNOWN_ENCODING, PhysicalType.DOUBLE);

        assertThatThrownBy(() -> readAllBatches(file))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("wire code " + UNKNOWN_ENCODING)
                .hasMessageContaining("column " + UNDECODABLE.dot());
    }

    @Test
    void knownEncodingWithoutADecoderForTheColumnTypeFailsAsUnsupported() throws IOException {
        Path file = writeFile(PageVersion.V2, Encoding.ALP.value(), PhysicalType.INT64);

        assertThatThrownBy(() -> readAllRows(file))
                .isInstanceOf(UnsupportedFeatureException.class)
                .hasMessageContaining("ALP")
                .hasMessageContaining("Column " + UNDECODABLE.dot() + " data page");
    }

    /**
     * Two required columns: {@code readable} holds PLAIN doubles 0 to 3, {@code undecodable} names {@code encodingCode}
     * over eight-byte values.
     */
    private Path writeFile(PageVersion version, int encodingCode, PhysicalType undecodableType) throws IOException {
        byte[] values = plainDoubles(0.0, 1.0, 2.0, 3.0);
        Column readable = Column.required(READABLE.dot(), PhysicalType.DOUBLE, Encoding.PLAIN.value(), values);
        Column undecodable = Column.required(UNDECODABLE.dot(), undecodableType, encodingCode, values);
        byte[] bytes = SinglePageParquetFile.build(version, ROW_COUNT, List.of(readable, undecodable));
        Path file = tempDir.resolve("encoding-" + encodingCode + "-" + version + ".parquet");
        Files.write(file, bytes);
        return file;
    }

    private static byte[] plainDoubles(double... values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (double value : values) {
            buffer.putDouble(value);
        }
        return buffer.array();
    }

    private static ColumnMetaData chunkOf(FileMetaData footer, ColumnPath column) {
        return footer.rowGroups().getFirst().columns().stream()
                .map(chunk -> chunk.metaData().orElseThrow())
                .filter(metaData -> metaData.pathInSchema().equals(List.of(column.dot())))
                .findFirst()
                .orElseThrow();
    }

    private static void readAllRows(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> records = ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(row -> row.isNull(UNDECODABLE));
        }
    }

    private static void readAllBatches(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecordBatch> batches = ParquetFileReader.open(source)
                        .readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(ParquetRecordBatch::close);
        }
    }
}
