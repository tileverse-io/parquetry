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
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * A negated {@code IN} over a FLOAT column reads as the conjunction of its inequalities: it matches no null cell, and
 * each row group is decided from its NaN count as an inequality is.
 *
 * <p>The file has three row groups of 100 rows: numbers with a null cell in one row out of four, NaN cells with a null
 * cell in one row out of four, and numbers without a null.
 */
class NotInNaNStatisticsTest {

    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final int ROWS_PER_GROUP = 100;
    private static final int ROWS_PER_NULL = 4;
    private static final long NULLS_PER_GROUP = ROWS_PER_GROUP / ROWS_PER_NULL;
    private static final long CELLS_PER_GROUP_WITH_NULLS = ROWS_PER_GROUP - NULLS_PER_GROUP;
    private static final int NAN_AND_NULLS_ROW_GROUP = 1;
    private static final int NUMBERS_ROW_GROUP = 2;
    private static final int ROW_GROUPS = 3;

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFile() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(WriteFixtures.optionalLeaf("value", PrimitiveKind.FLOAT));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .build();
        file = WriteFixtures.writeRows(tempDir.resolve("values.parquet"), schema, options, rows());
    }

    @Test
    void notInNaNMatchesTheNumbersAndNoNullCell() {
        Predicate notNaN = notIn(Float.NaN);

        List<RowGroupPlan> rowGroups = explain(notNaN);

        assertThat(rowGroups.get(NAN_AND_NULLS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertThat(rowGroups.get(NUMBERS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.MATCHED);
        ReadFixtures.assertCountWithAndWithoutPruning(file, notNaN, CELLS_PER_GROUP_WITH_NULLS + ROWS_PER_GROUP);
    }

    @Test
    void notInANumberMatchesTheOtherNumbersTheNaNCellsAndNoNullCell() {
        Predicate notOne = notIn(1.0f);

        List<RowGroupPlan> rowGroups = explain(notOne);

        assertThat(rowGroups.get(NUMBERS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.MATCHED);
        long cellsEqualToOne = 1L;
        long matches = 2 * CELLS_PER_GROUP_WITH_NULLS - cellsEqualToOne + ROWS_PER_GROUP;
        ReadFixtures.assertCountWithAndWithoutPruning(file, notOne, matches);
    }

    @Test
    void notInANumberAndNaNMatchesTheOtherNumbersAlone() {
        Predicate neitherOneNorNaN = notIn(1.0f, Float.NaN);

        List<RowGroupPlan> rowGroups = explain(neitherOneNorNaN);

        assertThat(rowGroups.get(NAN_AND_NULLS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        long cellsEqualToOne = 1L;
        long matches = CELLS_PER_GROUP_WITH_NULLS - cellsEqualToOne + ROWS_PER_GROUP;
        ReadFixtures.assertCountWithAndWithoutPruning(file, neitherOneNorNaN, matches);
    }

    private static Predicate notIn(float... values) {
        List<Value> list = new ArrayList<>();
        for (float value : values) {
            list.add(new Value.FloatVal(value));
        }
        return new Predicate.Not(new Predicate.In(VALUE, list));
    }

    private List<RowGroupPlan> explain(Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ExplainPlan plan = ParquetFileReader.open(source).explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
            return plan.rowGroups();
        }
    }

    /** A row names its cell unless the cell is null. */
    private static List<Map<ColumnPath, Object>> rows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int row = 0; row < ROW_GROUPS * ROWS_PER_GROUP; row++) {
            rows.add(isNull(row) ? Map.of() : Map.of(VALUE, cellOf(row)));
        }
        return rows;
    }

    private static boolean isNull(int row) {
        boolean inAGroupWithNulls = row / ROWS_PER_GROUP != NUMBERS_ROW_GROUP;
        return inAGroupWithNulls && row % ROWS_PER_NULL == 0;
    }

    private static float cellOf(int row) {
        boolean nan = row / ROWS_PER_GROUP == NAN_AND_NULLS_ROW_GROUP;
        return nan ? Float.NaN : row;
    }
}
