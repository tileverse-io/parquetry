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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * {@code NOT IN} follows SQL for null cells: a null cell is in no list, and outside no list holding a value. The count,
 * the row read and the batch read agree on it.
 *
 * <p>The file has 100 rows of two optional columns: an INT32 column holding its row number, with a null cell in one row
 * out of four, and a FLOAT column cycling through a null cell, {@code -0.0}, {@code +0.0}, {@code 1.5} and NaN.
 */
class NotInNullCellsTest {

    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath RATIO = ColumnPath.of("ratio");
    private static final int ROWS = 100;
    private static final int ROWS_PER_NULL = 4;
    private static final long CELLS = ROWS - ROWS / ROWS_PER_NULL;

    /** The cells of the FLOAT column by row, in a cycle; {@code null} is a null cell. */
    private static final List<Float> RATIOS = Arrays.asList(null, -0.0f, 0.0f, 1.5f, Float.NaN);

    private static final long ROWS_PER_RATIO = ROWS / RATIOS.size();

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFile() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(
                WriteFixtures.optionalLeaf("value", PrimitiveKind.INT32),
                WriteFixtures.optionalLeaf("ratio", PrimitiveKind.FLOAT));
        WriteOptions options = WriteOptions.builder().tempDir(tempDir).build();
        file = WriteFixtures.writeRows(tempDir.resolve("values.parquet"), schema, options, rows());
    }

    @Test
    void notInMatchesTheOtherCellsAndNoNullCell() {
        Predicate neitherOneNorTwo = notIn(VALUE, new Value.IntVal(1), new Value.IntVal(2));
        long listedCells = 2L;

        assertEachReadSelects(neitherOneNorTwo, CELLS - listedCells);
    }

    @Test
    void notInOfAValueHeldByNoCellMatchesEachCellAndNoNullCell() {
        Predicate notAThousand = notIn(VALUE, new Value.IntVal(1000));

        assertEachReadSelects(notAThousand, CELLS);
    }

    @Test
    void notInWithoutValuesMatchesEachRow() {
        Predicate notInNothing = notIn(VALUE);

        assertEachReadSelects(notInNothing, ROWS);
    }

    @Test
    void notInComparesAnIntegerColumnWithLiteralsOfBothIntegerWidths() {
        Predicate neitherOneNorTwo = notIn(VALUE, new Value.IntVal(1), new Value.LongVal(2L));
        long listedCells = 2L;

        assertEachReadSelects(neitherOneNorTwo, CELLS - listedCells);
    }

    @Test
    void notInAZeroMatchesNeitherZeroNorANullCell() {
        Predicate notZero = notIn(RATIO, new Value.FloatVal(0.0f));
        long numbersAndNaNCells = 2 * ROWS_PER_RATIO;

        assertEachReadSelects(notZero, numbersAndNaNCells);
    }

    @Test
    void notInAZeroAndNaNMatchesTheOtherNumbersAlone() {
        Predicate neitherZeroNorNaN = notIn(RATIO, new Value.FloatVal(-0.0f), new Value.FloatVal(Float.NaN));

        assertEachReadSelects(neitherZeroNorNaN, ROWS_PER_RATIO);
    }

    private static Predicate notIn(ColumnPath column, Value... values) {
        return new Predicate.Not(new Predicate.In(column, List.of(values)));
    }

    /** Asserts that the count, with and without pruning, the row read and the batch read select {@code expected}. */
    private void assertEachReadSelects(Predicate predicate, long expected) {
        ReadFixtures.assertCountWithAndWithoutPruning(file, predicate, expected);
        assertThat(rowsRead(predicate)).as("rows read").isEqualTo(expected);
        assertThat(rowsReadInBatches(predicate)).as("rows read in batches").isEqualTo(expected);
    }

    private long rowsRead(Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                return rows.count();
            }
        }
    }

    private long rowsReadInBatches(Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            try (Stream<ParquetRecordBatch> batches =
                    reader.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
                return batches.mapToLong(ParquetRecordBatch::rowCount).sum();
            }
        }
    }

    /** A row names its cells unless they are null. */
    private static List<Map<ColumnPath, Object>> rows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int row = 0; row < ROWS; row++) {
            Map<ColumnPath, Object> cells = new HashMap<>();
            if (row % ROWS_PER_NULL != 0) {
                cells.put(VALUE, row);
            }
            Float ratio = RATIOS.get(row % RATIOS.size());
            if (ratio != null) {
                cells.put(RATIO, ratio);
            }
            rows.add(cells);
        }
        return rows;
    }
}
