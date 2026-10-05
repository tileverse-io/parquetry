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

import static io.tileverse.parquetry.filter.Pred.col;
import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.FooterRewrite;

/**
 * Reads a file with statistics bounding a chunk by its NaN cells although the chunk holds a number, as left by a writer
 * taking the minimum and the maximum over the NaN cells too. The NaN count of the chunk tells that a number is there,
 * and pruning must not take the chunk for one holding nothing but NaN.
 */
class NaNBoundsOverNumbersReadTest {

    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final double NEGATIVE_NAN = Double.longBitsToDouble(0xFFF8000000000000L);
    private static final List<Double> CELLS = List.of(NEGATIVE_NAN, 1.0, Double.NaN);
    private static final long NUMBERS = 1L;
    private static final long NANS = 2L;

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFileBoundedByItsNaNs() throws IOException {
        file = FooterRewrite.rewrite(
                writeCells(),
                tempDir.resolve("nan-bounds.parquet"),
                FooterRewrite.statisticsBounds(VALUE, 0, plain(NEGATIVE_NAN), plain(Double.NaN)));
    }

    @Test
    void comparisonsMatchedByTheNumberReadItsRow() {
        assertCount(col("value").eq(1.0), NUMBERS);
        assertCount(col("value").gt(0.0), NUMBERS);
        assertCount(col("value").ltEq(1.0), NUMBERS);
    }

    @Test
    void inequalityWithTheNumberReadsTheNaNCellsAlone() {
        assertCount(col("value").notEq(1.0), NANS);
    }

    @Test
    void nanLiteralReadsTheNaNCellsAlone() {
        assertCount(col("value").eq(Double.NaN), NANS);
    }

    @Test
    void inequalityWithNaNReadsTheNumberAlone() {
        assertCount(col("value").notEq(Double.NaN), NUMBERS);
    }

    private void assertCount(Predicate predicate, long expected) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(predicate, ReadOptions.DEFAULTS))
                    .as("rows matching %s", predicate)
                    .isEqualTo(expected);
        }
    }

    private Path writeCells() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(WriteFixtures.requiredLeaf("value", PrimitiveKind.DOUBLE));
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        List<Map<ColumnPath, Object>> rows = CELLS.stream()
                .map(cell -> Map.<ColumnPath, Object>of(VALUE, cell))
                .toList();
        return WriteFixtures.writeRows(tempDir.resolve("cells.parquet"), schema, options, rows);
    }

    private static MemorySegment plain(double value) {
        MemorySegment bytes = MemorySegment.ofArray(new byte[Double.BYTES]);
        bytes.set(DOUBLE, 0, value);
        return bytes;
    }
}
