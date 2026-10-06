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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
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
 * A read prunes a FLOAT column by the NaN counts of a file written by parquetry: a NaN literal skips the row groups
 * counting no NaN, a comparison with a number skips those holding nothing but NaN, and a row group proven to match as a
 * whole is counted without a scan.
 *
 * <p>The file has three row groups of two pages of 100 rows: numbers from 1 to 200; a page of numbers from 201 to 300
 * and a page of NaN; NaN only.
 */
class NaNStatisticsPruningTest {

    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final int ROW_GROUPS = 3;
    private static final int ROWS_PER_GROUP = 200;
    private static final int ROWS_PER_PAGE = 100;
    private static final long NUMBERS_IN_THE_FILE = ROWS_PER_GROUP + ROWS_PER_PAGE;
    private static final long NANS_IN_THE_FILE = ROWS_PER_PAGE + ROWS_PER_GROUP;
    private static final int NUMBERS_ROW_GROUP = 0;
    private static final int MIXED_ROW_GROUP = 1;
    private static final int ONLY_NAN_ROW_GROUP = 2;
    /** The outcomes leaving the rows of a row group to the scan. */
    private static final Set<RowGroupOutcome> SCANNED = Set.of(RowGroupOutcome.FULL, RowGroupOutcome.PARTIAL);

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFile() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(WriteFixtures.requiredLeaf("value", PrimitiveKind.FLOAT));
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .pageValueLimit(ROWS_PER_PAGE)
                .build();
        file = WriteFixtures.writeRows(tempDir.resolve("values.parquet"), schema, options, rows());
    }

    @Test
    void nanLiteralSkipsWhatCountsNoNaNAndMatchesWhatHoldsOnlyNaN() {
        Predicate isNaN = col("value").eq(Float.NaN);

        List<RowGroupPlan> plan = explain(isNaN);

        assertThat(plan.get(NUMBERS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertThat(plan.get(MIXED_ROW_GROUP).outcome()).isIn(SCANNED);
        assertThat(plan.get(ONLY_NAN_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.MATCHED);
        assertCount(isNaN, NANS_IN_THE_FILE);
    }

    @Test
    void comparisonWithANumberSkipsWhatHoldsOnlyNaNAndMatchesWhatCountsNoNaN() {
        Predicate positive = col("value").gt(0.0);

        List<RowGroupPlan> plan = explain(positive);

        assertThat(plan.get(NUMBERS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.MATCHED);
        assertThat(plan.get(MIXED_ROW_GROUP).outcome()).isIn(SCANNED);
        assertThat(plan.get(ONLY_NAN_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertCount(positive, NUMBERS_IN_THE_FILE);
    }

    @Test
    void notEqualToNaNSkipsWhatHoldsOnlyNaN() {
        Predicate isNumber = col("value").notEq(Float.NaN);

        List<RowGroupPlan> plan = explain(isNumber);

        assertThat(plan.get(NUMBERS_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.MATCHED);
        assertThat(plan.get(MIXED_ROW_GROUP).outcome()).isIn(SCANNED);
        assertThat(plan.get(ONLY_NAN_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertCount(isNumber, NUMBERS_IN_THE_FILE);
    }

    @Test
    void notEqualToANumberMatchesWhatHoldsOnlyNaN() {
        Predicate notOne = col("value").notEq(1.0);

        List<RowGroupPlan> plan = explain(notOne);

        assertThat(plan.get(ONLY_NAN_ROW_GROUP).outcome()).isEqualTo(RowGroupOutcome.MATCHED);
        long cellsEqualToOne = 1L;
        assertCount(notOne, NUMBERS_IN_THE_FILE + NANS_IN_THE_FILE - cellsEqualToOne);
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

    private static List<Map<ColumnPath, Object>> rows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int row = 0; row < ROW_GROUPS * ROWS_PER_GROUP; row++) {
            rows.add(Map.of(VALUE, valueOf(row)));
        }
        return rows;
    }

    private static float valueOf(int row) {
        boolean number = row < NUMBERS_IN_THE_FILE;
        return number ? row + 1 : Float.NaN;
    }
}
