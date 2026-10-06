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
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * A read prunes the pages of a FLOAT column written in IEEE 754 total order: a page holding only NaN is bounded by NaN
 * in the column index, a comparison with a number skips it, an equality with NaN reads it alone, an inequality with NaN
 * skips it, and an inequality with a number reads it.
 *
 * <p>The file has one row group of three pages of 100 rows: numbers from 1 to 100; NaN cells of either sign and payload
 * with a null in one row out of four; numbers from 201 to 300. A second file holds the same cells in the leaf of an
 * optional struct, with null structs for the nulls.
 */
class TotalOrderPagePruningTest {

    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath SAMPLE = ColumnPath.of("sample");
    private static final ColumnPath SAMPLE_VALUE = ColumnPath.of("sample", "value");
    private static final int ROWS_PER_PAGE = 100;
    private static final int PAGES = 3;
    private static final int NAN_PAGE = 1;
    private static final int ROWS_PER_NULL = 4;
    private static final long NUMBERS = 2L * ROWS_PER_PAGE;
    private static final long NULLS = ROWS_PER_PAGE / ROWS_PER_NULL;
    private static final long NANS = ROWS_PER_PAGE - NULLS;
    private static final List<Float> NAN_PATTERNS =
            List.of(Float.NaN, Float.intBitsToFloat(0xFFC00000), Float.intBitsToFloat(0x7FC00123));

    @TempDir
    Path tempDir;

    private Path file;

    @BeforeEach
    void writeFile() throws IOException {
        ParquetSchema schema = WriteFixtures.schemaOf(WriteFixtures.optionalLeaf("value", PrimitiveKind.FLOAT));
        file = WriteFixtures.writeRows(tempDir.resolve("values.parquet"), schema, totalOrderPages(), rows());
    }

    @Test
    void comparisonWithANumberSkipsThePageOfNaN() {
        Predicate positive = col("value").gt(0.0);

        RowRanges kept = rowsKeptByTheColumnIndex(positive);

        assertThat(kept.totalRows()).isEqualTo(NUMBERS);
        ReadFixtures.assertCountWithAndWithoutPruning(file, positive, NUMBERS);
    }

    @Test
    void nanLiteralReadsThePageOfNaNAlone() {
        Predicate isNaN = col("value").eq(Float.NaN);

        RowRanges kept = rowsKeptByTheColumnIndex(isNaN);

        assertThat(kept.totalRows()).as("the rows of the page of NaN").isEqualTo(ROWS_PER_PAGE);
        ReadFixtures.assertCountWithAndWithoutPruning(file, isNaN, NANS);
    }

    @Test
    void inequalityWithNaNSkipsThePageOfNaN() {
        Predicate isNumber = col("value").notEq(Float.NaN);

        RowRanges kept = rowsKeptByTheColumnIndex(isNumber);

        assertThat(kept.totalRows()).isEqualTo(NUMBERS);
        ReadFixtures.assertCountWithAndWithoutPruning(file, isNumber, NUMBERS);
    }

    @Test
    void inequalityWithANumberKeepsThePageOfNaN() {
        Predicate notOne = col("value").notEq(1.0);

        long cellsEqualToOne = 1L;
        ReadFixtures.assertCountWithAndWithoutPruning(file, notOne, NUMBERS + NANS - cellsEqualToOne);
    }

    @Test
    void nullFilterReadsThePageOfNaNAndNulls() {
        Predicate isNull = col("value").isNull();

        RowRanges kept = rowsKeptByTheColumnIndex(isNull);

        assertThat(kept.totalRows()).as("the rows of the page of NaN").isEqualTo(ROWS_PER_PAGE);
        ReadFixtures.assertCountWithAndWithoutPruning(file, isNull, NULLS);
    }

    @Test
    void comparisonWithANumberSkipsAPageOfNaNAndNullStructs() throws IOException {
        Path structs = writeFileWithAStruct();
        Predicate positive = col(SAMPLE_VALUE).gt(0.0);

        RowRanges kept = rowsKeptByTheColumnIndex(structs, positive);

        assertThat(kept.totalRows()).isEqualTo(NUMBERS);
        ReadFixtures.assertCountWithAndWithoutPruning(structs, positive, NUMBERS);
    }

    private WriteOptions totalOrderPages() {
        return WriteOptions.builder()
                .tempDir(tempDir)
                .pageValueLimit(ROWS_PER_PAGE)
                .floatColumnOrder(FloatColumnOrder.IEEE_754_TOTAL_ORDER)
                .build();
    }

    /**
     * Writes the cells of the flat file as the leaf of an optional struct, with a null struct for each null cell: the
     * null count of the leaf counts the null structs.
     */
    private Path writeFileWithAStruct() throws IOException {
        SchemaNode.Primitive value = WriteFixtures.optionalLeaf("value", PrimitiveKind.FLOAT);
        SchemaNode sample = new SchemaNode.Group("sample", Repetition.OPTIONAL, List.of(value), Optional.empty(), -1);
        ParquetSchema schema = WriteFixtures.schemaOf(sample);
        Path target = tempDir.resolve("structs.parquet");
        try (OutputStream out = Files.newOutputStream(target);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, totalOrderPages())) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (Map<ColumnPath, Object> row : rows()) {
                appendStruct(appender, (Float) row.get(VALUE));
            }
        }
        return target;
    }

    private static void appendStruct(ParquetRecordBatchBuilder appender, Float cell) {
        if (cell == null) {
            appender.setNull(SAMPLE);
        } else {
            appender.setFloat(SAMPLE_VALUE, cell);
        }
        appender.endRow();
    }

    /** The row ranges left to the scan by the column index of the single row group of the flat file. */
    private RowRanges rowsKeptByTheColumnIndex(Predicate predicate) {
        return rowsKeptByTheColumnIndex(file, predicate);
    }

    /** The row ranges left to the scan by the column index of the single row group of {@code file}. */
    private static RowRanges rowsKeptByTheColumnIndex(Path file, Predicate predicate) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ExplainPlan plan = ParquetFileReader.open(source).explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
            List<PruningDecision> tiers = plan.rowGroups().getFirst().tiers();
            PruningDecision columnIndex = tiers.stream()
                    .filter(decision -> decision.tier() == Tier.COLUMN_INDEX)
                    .findFirst()
                    .orElseThrow();
            assertThat(columnIndex).isInstanceOf(PruningDecision.NarrowedTo.class);
            return ((PruningDecision.NarrowedTo) columnIndex).ranges();
        }
    }

    private static List<Map<ColumnPath, Object>> rows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int row = 0; row < PAGES * ROWS_PER_PAGE; row++) {
            boolean inNaNPage = row / ROWS_PER_PAGE == NAN_PAGE;
            rows.add(inNaNPage ? nanOrNullRow(row) : Map.of(VALUE, (float) (row + 1)));
        }
        return rows;
    }

    /** A row of the page of NaN: a null in one row out of four, else a NaN of a sign and payload changing by row. */
    private static Map<ColumnPath, Object> nanOrNullRow(int row) {
        if (row % ROWS_PER_NULL == 0) {
            return Map.of();
        }
        return Map.of(VALUE, NAN_PATTERNS.get(row % NAN_PATTERNS.size()));
    }
}
