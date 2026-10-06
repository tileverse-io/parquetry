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
package io.tileverse.parquetry.internal.write.conformance;

import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOATING;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ROWS_PER_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.column.Encoding;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.Row;

/**
 * parquet-java reads back the bit pattern of each FLOAT, DOUBLE and FLOAT16 cell written by parquetry: the sign of a
 * zero and the sign and payload of a NaN survive the write, in plain pages and in dictionary pages alike.
 */
@Tag("conformance")
class FloatCellConformanceIT {

    private static final float[] FLOAT_PATTERNS = {
        1.0f, Float.intBitsToFloat(0x7FC00001), Float.NaN, Float.intBitsToFloat(0xFFC00000), -0.0f
    };
    private static final double[] DOUBLE_PATTERNS = {
        1.0,
        Double.longBitsToDouble(0x7FF8000000000001L),
        Double.NaN,
        Double.longBitsToDouble(0xFFF8000000000000L),
        -0.0
    };
    private static final short[] HALF_PATTERNS = {0x3C00, 0x7E01, 0x7E00, (short) 0xFE00, (short) 0x8000};

    @TempDir
    Path tempDir;

    @Test
    void parquetJavaReadsBackTheBitPatternOfEachCell() throws IOException {
        List<Row> rows = FloatConformanceFixture.generateRows();
        Path file = FloatConformanceFixture.writeWithParquetry(tempDir.resolve("floats.parquet"), tempDir, rows);

        assertBitPatternsReadBack(file, rows);
    }

    @Test
    void dictionaryEncodedCellsReadBackWithTheirBitPatterns() throws IOException {
        List<Row> fewPatterns = new ArrayList<>();
        for (int id = 0; id < ROWS_PER_GROUP; id++) {
            int pattern = id % FLOAT_PATTERNS.length;
            fewPatterns.add(Row.of(id, FLOAT_PATTERNS[pattern], DOUBLE_PATTERNS[pattern], HALF_PATTERNS[pattern]));
        }
        Path file =
                FloatConformanceFixture.writeWithParquetry(tempDir.resolve("dictionary.parquet"), tempDir, fewPatterns);

        BlockMetaData rowGroup =
                WriteConformanceSupport.rowGroupsViaParquetJava(file).getFirst();
        for (String column : FLOATING) {
            assertThat(WriteConformanceSupport.chunkOf(rowGroup, column).getEncodings())
                    .as("encodings of %s", column)
                    .contains(Encoding.RLE_DICTIONARY);
        }
        assertBitPatternsReadBack(file, fewPatterns);
    }

    private static void assertBitPatternsReadBack(Path file, List<Row> written) throws IOException {
        List<Row> read = readWithParquetJava(file);

        assertThat(read).hasSameSizeAs(written);
        for (int i = 0; i < written.size(); i++) {
            Row expected = written.get(i);
            Row actual = read.get(i);
            assertThat(actual.isNull()).as("row %d is null", i).isEqualTo(expected.isNull());
            assertThat(Float.floatToRawIntBits(actual.f()))
                    .as("f of row %d", i)
                    .isEqualTo(Float.floatToRawIntBits(expected.f()));
            assertThat(Double.doubleToRawLongBits(actual.d()))
                    .as("d of row %d", i)
                    .isEqualTo(Double.doubleToRawLongBits(expected.d()));
            assertThat(actual.h()).as("h of row %d", i).isEqualTo(expected.h());
        }
    }

    private static List<Row> readWithParquetJava(Path file) throws IOException {
        List<Row> read = new ArrayList<>();
        try (ParquetReader<Group> reader =
                WriteConformanceSupport.groupReader(file).build()) {
            Group group = reader.read();
            while (group != null) {
                read.add(FloatConformanceFixture.rowOf(group));
                group = reader.read();
            }
        }
        return read;
    }
}
