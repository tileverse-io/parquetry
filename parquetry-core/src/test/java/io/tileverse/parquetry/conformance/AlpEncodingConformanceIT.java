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
package io.tileverse.parquetry.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the ALP fixture of {@code apache/parquet-testing} and checks each ALP-encoded column against the PLAIN column
 * holding the same values, bit for bit and with nulls aligned, through the row and the batch read APIs. Per the
 * fixture's README, the ALP columns cover 1024-, 4096- and 32-value vectors, exceptions at vector boundaries, NaN
 * payloads, infinities, negative zero, subnormals, all-exception and constant vectors, 64-bit frame-of-reference widths
 * and a partial trailing vector with nulls.
 */
class AlpEncodingConformanceIT {

    private static final String FIXTURE = "parquet-testing/data/alp_extended.zstd.parquet";
    private static final int ROW_COUNT = 9032;
    private static final List<Integer> NULL_ROWS = IntStream.rangeClosed(82, 89)
            .map(hundreds -> hundreds * 100)
            .boxed()
            .toList();

    @TempDir
    Path tempDir;

    private Path fixture;

    @BeforeEach
    void extractFixture() {
        fixture = TestCorpus.extractFile(FIXTURE, tempDir);
    }

    static Stream<String> alpColumns() {
        return Stream.of(
                "float_alp_1024",
                "float_alp_4096",
                "float_alp_32",
                "double_alp_1024",
                "double_alp_4096",
                "double_alp_32");
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("alpColumns")
    void footerListsAlpForEachChunkOfTheColumn(String alpColumn) {
        List<List<Encoding>> chunkEncodings = chunkEncodingsOf(alpColumn);

        assertThat(chunkEncodings)
                .hasSize(5)
                .allSatisfy(encodings -> assertThat(encodings).contains(Encoding.ALP));
    }

    @Test
    void plainReferenceHoldsTheDocumentedEdgeValues() {
        List<Long> floats = readBitsViaRowApi(ColumnPath.of("float_plain"));
        List<Long> doubles = readBitsViaRowApi(ColumnPath.of("double_plain"));

        assertThat(floats.get(1024)).isEqualTo(0x7FC00000L);
        assertThat(floats.get(1500)).isEqualTo(0x7FC0DEADL);
        assertThat(floats.get(2047)).isEqualTo(0xFFC00001L);
        assertThat(floats.get(2002)).isEqualTo(Integer.toUnsignedLong(Float.floatToRawIntBits(-0.0f)));
        assertThat(doubles.get(1024)).isEqualTo(0x7FF8000000000000L);
        assertThat(doubles.get(1500)).isEqualTo(0x7FF800DEADBEEF00L);
        assertThat(doubles.get(2047)).isEqualTo(0xFFF8000000000001L);
        assertThat(doubles.get(2000)).isEqualTo(Double.doubleToRawLongBits(Double.POSITIVE_INFINITY));
        assertThat(doubles.get(9001)).isEqualTo(Double.doubleToRawLongBits(8e18));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("alpColumns")
    void rowApiDecodesTheAlpColumnBitwiseEqualToItsPlainReference(String alpColumn) {
        ColumnPath alp = ColumnPath.of(alpColumn);

        List<Long> alpBits = readBitsViaRowApi(alp);
        List<Long> plainBits = readBitsViaRowApi(plainReferenceOf(alp));

        assertThat(alpBits).hasSize(ROW_COUNT);
        assertBitwiseEqual(alpColumn, alpBits, plainBits);
        assertThat(nullRowsOf(alpBits)).containsExactlyElementsOf(NULL_ROWS);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("alpColumns")
    void batchApiDecodesTheAlpColumnBitwiseEqualToItsPlainReference(String alpColumn) {
        ColumnPath alp = ColumnPath.of(alpColumn);

        List<Long> alpBits = readBitsViaBatchApi(alp);
        List<Long> plainBits = readBitsViaBatchApi(plainReferenceOf(alp));

        assertThat(alpBits).hasSize(ROW_COUNT);
        assertBitwiseEqual(alpColumn, alpBits, plainBits);
        assertThat(nullRowsOf(alpBits)).containsExactlyElementsOf(NULL_ROWS);
    }

    /**
     * A filter keeping about half of the rows makes the reader skip values and decode short runs inside ALP vectors.
     * Within each surviving row the ALP cell still equals the PLAIN cell.
     */
    @ParameterizedTest(name = "{0}")
    @MethodSource("alpColumns")
    void filteredReadsKeepAlpCellsEqualToTheirPlainReference(String alpColumn) {
        ColumnPath alp = ColumnPath.of(alpColumn);
        ColumnPath plain = plainReferenceOf(alp);
        Predicate positive = new Predicate.Gt(plain, positiveZeroOf(plain));

        List<Long> alpBits = new ArrayList<>();
        List<Long> plainBits = new ArrayList<>();
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture);
                Stream<ParquetRecord> records =
                        ParquetFileReader.open(source).read(positive, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(row -> {
                alpBits.add(bitsOf(row, alp));
                plainBits.add(bitsOf(row, plain));
            });
        }

        assertThat(alpBits).hasSizeBetween(1, ROW_COUNT - 1);
        assertBitwiseEqual(alpColumn, alpBits, plainBits);
    }

    private List<List<Encoding>> chunkEncodingsOf(String column) {
        FileMetaData footer;
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture)) {
            footer = ParquetFormat.readFooter(source);
        }
        List<List<Encoding>> encodings = new ArrayList<>();
        for (RowGroup rowGroup : footer.rowGroups()) {
            for (ColumnChunk chunk : rowGroup.columns()) {
                List<String> path = chunk.metaData().orElseThrow().pathInSchema();
                if (path.equals(List.of(column))) {
                    encodings.add(chunk.metaData().orElseThrow().encodings());
                }
            }
        }
        return encodings;
    }

    private List<Long> readBitsViaRowApi(ColumnPath column) {
        List<Long> cells = new ArrayList<>(ROW_COUNT);
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture);
                Stream<ParquetRecord> records = ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(row -> cells.add(bitsOf(row, column)));
        }
        return cells;
    }

    private List<Long> readBitsViaBatchApi(ColumnPath column) {
        List<Long> cells = new ArrayList<>(ROW_COUNT);
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixture);
                Stream<ParquetRecordBatch> batches = ParquetFileReader.open(source)
                        .readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                try (ParquetRecordBatch owned = batch) {
                    ColumnVector vector = owned.columns().get(column);
                    for (int row = 0; row < owned.rowCount(); row++) {
                        cells.add(bitsOf(vector, row));
                    }
                }
            });
        }
        return cells;
    }

    /** The raw IEEE 754 bits of a cell, or {@code null} for a null cell. */
    private static Long bitsOf(ParquetRecord row, ColumnPath column) {
        if (row.isNull(column)) {
            return null;
        }
        if (isFloatColumn(column)) {
            return Integer.toUnsignedLong(Float.floatToRawIntBits(row.getFloat(column)));
        }
        return Double.doubleToRawLongBits(row.getDouble(column));
    }

    private static Long bitsOf(ColumnVector vector, int row) {
        if (vector.isNull(row)) {
            return null;
        }
        return switch (vector) {
            case FloatVector floats -> Integer.toUnsignedLong(Float.floatToRawIntBits(floats.getFloat(row)));
            case DoubleVector doubles -> Double.doubleToRawLongBits(doubles.getDouble(row));
            default -> throw new IllegalStateException("not a floating-point vector: " + vector);
        };
    }

    private static boolean isFloatColumn(ColumnPath column) {
        return column.dot().startsWith("float_");
    }

    private static ColumnPath plainReferenceOf(ColumnPath alpColumn) {
        String type = isFloatColumn(alpColumn) ? "float" : "double";
        return ColumnPath.of(type + "_plain");
    }

    private static Value positiveZeroOf(ColumnPath column) {
        if (isFloatColumn(column)) {
            return new Value.FloatVal(0.0f);
        }
        return new Value.DoubleVal(0.0);
    }

    /** Compares the cells row by row, reporting the differing rows only rather than the whole columns. */
    private static void assertBitwiseEqual(String alpColumn, List<Long> alpBits, List<Long> plainBits) {
        assertThat(alpBits).as("%s row count", alpColumn).hasSameSizeAs(plainBits);
        List<String> mismatches = new ArrayList<>();
        for (int row = 0; row < alpBits.size(); row++) {
            if (!Objects.equals(alpBits.get(row), plainBits.get(row))) {
                mismatches.add("row " + row + ": " + hex(alpBits.get(row)) + " != " + hex(plainBits.get(row)));
            }
        }
        assertThat(mismatches)
                .as("%s cells differing from the plain reference", alpColumn)
                .isEmpty();
    }

    private static List<Integer> nullRowsOf(List<Long> cells) {
        List<Integer> nullRows = new ArrayList<>();
        for (int row = 0; row < cells.size(); row++) {
            if (cells.get(row) == null) {
                nullRows.add(row);
            }
        }
        return nullRows;
    }

    private static String hex(Long bits) {
        if (bits == null) {
            return "null";
        }
        return "0x" + HexFormat.of().toHexDigits(bits);
    }
}
