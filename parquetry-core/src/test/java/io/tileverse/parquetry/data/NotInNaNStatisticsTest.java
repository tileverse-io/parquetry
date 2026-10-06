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
 * A negated {@code IN} over a FLOAT column is decided for a whole row group from its NaN count: when no cell can match
 * the list, each row matches the negation, a row with a null cell included.
 *
 * <p>The file has two row groups of 100 rows with a null cell in one row out of four: numbers and nulls, then NaN cells
 * and nulls.
 */
class NotInNaNStatisticsTest {

    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final int ROWS_PER_GROUP = 100;
    private static final int ROWS_PER_NULL = 4;
    private static final long NULLS_PER_GROUP = ROWS_PER_GROUP / ROWS_PER_NULL;
    private static final int NUMBERS_ROW_GROUP = 0;
    private static final int ONLY_NAN_ROW_GROUP = 1;

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
    void notInNaNMatchesEachRowOfARowGroupCountingNoNaN() {
        Predicate notNaN = notIn(Float.NaN);

        RowGroupOutcome outcome = explain(notNaN).get(NUMBERS_ROW_GROUP).outcome();

        assertThat(outcome).isEqualTo(RowGroupOutcome.MATCHED);
        assertCount(notNaN, ROWS_PER_GROUP + NULLS_PER_GROUP);
    }

    @Test
    void notInANumberMatchesEachRowOfARowGroupOfOnlyNaN() {
        Predicate notOne = notIn(1.0f);

        RowGroupOutcome outcome = explain(notOne).get(ONLY_NAN_ROW_GROUP).outcome();

        assertThat(outcome).isEqualTo(RowGroupOutcome.MATCHED);
        long cellsEqualToOne = 1L;
        assertCount(notOne, 2 * ROWS_PER_GROUP - cellsEqualToOne);
    }

    private static Predicate notIn(float value) {
        Predicate in = new Predicate.In(VALUE, List.of(new Value.FloatVal(value)));
        return new Predicate.Not(in);
    }

    private List<RowGroupPlan> explain(Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ExplainPlan plan = ParquetFileReader.open(source).explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
            return plan.rowGroups();
        }
    }

    private void assertCount(Predicate predicate, long expected) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.count(predicate, ReadOptions.DEFAULTS))
                    .as("rows matching %s", predicate)
                    .isEqualTo(reader.count(predicate, ReadFixtures.METADATA_PRUNING_OFF))
                    .isEqualTo(expected);
        }
    }

    /** A row names its cell unless the cell is null. */
    private static List<Map<ColumnPath, Object>> rows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int row = 0; row < 2 * ROWS_PER_GROUP; row++) {
            boolean isNull = row % ROWS_PER_NULL == 0;
            rows.add(isNull ? Map.of() : Map.of(VALUE, cellOf(row)));
        }
        return rows;
    }

    private static float cellOf(int row) {
        boolean number = row < ROWS_PER_GROUP;
        return number ? row : Float.NaN;
    }
}
