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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.SinglePageParquetFile.Column;
import io.tileverse.parquetry.data.SinglePageParquetFile.PageVersion;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.internal.read.page.AlpPageBuilder;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Reads optional FLOAT and DOUBLE columns stored as ALP-encoded V1 and V2 data pages, through the row and the batch
 * APIs. The values include exceptions with NaN payloads, and nulls interleave with them.
 */
class AlpDataPageReadTest {

    private static final int ROW_COUNT = 3000;
    private static final ColumnPath COLUMN = ColumnPath.of("measure");

    @TempDir
    Path tempDir;

    static Stream<Arguments> pageShapes() {
        return Stream.of(
                Arguments.of(PageVersion.V1, PhysicalType.FLOAT),
                Arguments.of(PageVersion.V2, PhysicalType.FLOAT),
                Arguments.of(PageVersion.V1, PhysicalType.DOUBLE),
                Arguments.of(PageVersion.V2, PhysicalType.DOUBLE));
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("pageShapes")
    void rowApiReadsAlpValuesAndNulls(PageVersion version, PhysicalType type) throws IOException {
        BitSet nulls = everyThirteenthRow();
        List<Long> expected = expectedCells(type, nulls);
        Path file = writeAlpFile(version, type, nulls, expected);

        assertThat(readViaRowApi(file, type)).containsExactlyElementsOf(expected);
    }

    @ParameterizedTest(name = "{0} {1}")
    @MethodSource("pageShapes")
    void batchApiReadsAlpValuesAndNulls(PageVersion version, PhysicalType type) throws IOException {
        BitSet nulls = everyThirteenthRow();
        List<Long> expected = expectedCells(type, nulls);
        Path file = writeAlpFile(version, type, nulls, expected);

        assertThat(readViaBatchApi(file)).containsExactlyElementsOf(expected);
    }

    private static BitSet everyThirteenthRow() {
        BitSet nulls = new BitSet(ROW_COUNT);
        for (int row = 0; row < ROW_COUNT; row += 13) {
            nulls.set(row);
        }
        return nulls;
    }

    /**
     * Per row, the raw IEEE 754 bits of the cell or {@code null}. Values are two-decimal numbers; rows divisible by 97
     * hold a NaN payload, stored by the encoder as an exception.
     */
    private static List<Long> expectedCells(PhysicalType type, BitSet nulls) {
        Random random = new Random(type.ordinal());
        List<Long> cells = new ArrayList<>(ROW_COUNT);
        for (int row = 0; row < ROW_COUNT; row++) {
            if (nulls.get(row)) {
                cells.add(null);
                continue;
            }
            boolean nanWithPayload = row % 97 == 0;
            long bits = type == PhysicalType.DOUBLE
                    ? doubleBits(random, nanWithPayload)
                    : floatBits(random, nanWithPayload);
            cells.add(bits);
        }
        return cells;
    }

    private static long doubleBits(Random random, boolean nanWithPayload) {
        if (nanWithPayload) {
            return 0x7FF800DEADBEEF00L;
        }
        return Double.doubleToRawLongBits(random.nextInt(-999, 1000) / 100.0);
    }

    private static long floatBits(Random random, boolean nanWithPayload) {
        if (nanWithPayload) {
            return Integer.toUnsignedLong(0x7FC0DEAD);
        }
        return Integer.toUnsignedLong(Float.floatToRawIntBits(random.nextInt(-999, 1000) / 100.0f));
    }

    private Path writeAlpFile(PageVersion version, PhysicalType type, BitSet nulls, List<Long> cells)
            throws IOException {
        byte[] alpValues = alpPage(type, cells);
        Column column = new Column(COLUMN.dot(), type, true, nulls, Encoding.ALP.value(), alpValues);
        byte[] bytes = SinglePageParquetFile.build(version, ROW_COUNT, List.of(column));
        Path file = tempDir.resolve("alp-" + version + "-" + type + ".parquet");
        Files.write(file, bytes);
        return file;
    }

    /** The non-null cells ALP-encoded in 1024-value vectors with exponent 2 and factor 0. */
    private static byte[] alpPage(PhysicalType type, List<Long> cells) {
        List<Long> nonNull = cells.stream().filter(cell -> cell != null).toList();
        if (type == PhysicalType.DOUBLE) {
            double[] values =
                    nonNull.stream().mapToDouble(Double::longBitsToDouble).toArray();
            return AlpPageBuilder.doublePage(10, 2, 0, values);
        }
        float[] values = new float[nonNull.size()];
        for (int i = 0; i < values.length; i++) {
            values[i] = Float.intBitsToFloat(nonNull.get(i).intValue());
        }
        return AlpPageBuilder.floatPage(10, 2, 0, values);
    }

    private static List<Long> readViaRowApi(Path file, PhysicalType type) {
        List<Long> cells = new ArrayList<>(ROW_COUNT);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecord> records = ParquetFileReader.open(source)
                        .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            records.forEach(row -> cells.add(bitsOf(row, type)));
        }
        return cells;
    }

    private static Long bitsOf(ParquetRecord row, PhysicalType type) {
        if (row.isNull(COLUMN)) {
            return null;
        }
        if (type == PhysicalType.DOUBLE) {
            return Double.doubleToRawLongBits(row.getDouble(COLUMN));
        }
        return Integer.toUnsignedLong(Float.floatToRawIntBits(row.getFloat(COLUMN)));
    }

    private static List<Long> readViaBatchApi(Path file) {
        List<Long> cells = new ArrayList<>(ROW_COUNT);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file);
                Stream<ParquetRecordBatch> batches = ParquetFileReader.open(source)
                        .readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            batches.forEach(batch -> {
                try (ParquetRecordBatch owned = batch) {
                    ColumnVector vector = owned.columns().get(COLUMN);
                    for (int row = 0; row < owned.rowCount(); row++) {
                        cells.add(bitsOf(vector, row));
                    }
                }
            });
        }
        return cells;
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
}
