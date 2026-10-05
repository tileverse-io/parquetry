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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Rewrites {@code floating_orders_nan_count.parquet} of the {@code apache/parquet-testing} corpus and compares the
 * statistics of the copy with those of the reference writer. The file holds the same cells in a total order column and
 * a type-defined order column per floating-point type, over five row groups: numbers, numbers mixed with NaN, NaN only,
 * a zero minimum and a zero maximum. A copy records its columns in one order: the type-defined order, or IEEE 754 total
 * order on request.
 *
 * <p>The copy matches the reference type-defined columns: their NaN and null counts, their bounds with a zero written
 * as {@code -0.0} for a minimum and {@code +0.0} for a maximum, and no bounds for the row group of NaN. In the row
 * group mixing numbers and NaN the reference writer leaves the type-defined bounds out, as allowed by the format.
 * parquetry records the bounds of the numbers there, the ones held by the reference total order column.
 *
 * <p>The copy in total order matches the reference total order columns row group by row group: counts and bounds, with
 * zero bounds keeping their sign and NaN bounds for the row group of NaN.
 */
@Tag("conformance")
class FloatingOrdersCorpusRewriteIT {

    private static final String FIXTURE = "parquet-testing/data/floating_orders_nan_count.parquet";
    private static final int ROWS_PER_GROUP = 10;
    private static final int MIXED_GROUP = 1;
    private static final List<String> TYPES = List.of("float", "double", "float16");
    private static final String TOTAL_ORDER_SUFFIX = "_ieee754";
    private static final String TYPE_DEFINED_SUFFIX = "_typedef";

    @TempDir
    static Path tempDir;

    private static List<BlockMetaData> referenceGroups;
    private static List<BlockMetaData> copyGroups;
    private static List<BlockMetaData> totalOrderCopyGroups;

    @BeforeAll
    static void rewriteTheCorpusFile() throws IOException {
        Path reference = TestCorpus.extractFile(FIXTURE, tempDir);
        Path copy = rewrite(reference, FloatColumnOrder.TYPE_DEFINED);
        Path totalOrderCopy = rewrite(reference, FloatColumnOrder.IEEE_754_TOTAL_ORDER);
        referenceGroups = WriteConformanceSupport.rowGroupsViaParquetJava(reference);
        copyGroups = WriteConformanceSupport.rowGroupsViaParquetJava(copy);
        totalOrderCopyGroups = WriteConformanceSupport.rowGroupsViaParquetJava(totalOrderCopy);
    }

    @Test
    void copyHasTheRowGroupsOfTheReference() {
        assertThat(copyGroups).hasSameSizeAs(referenceGroups);
        assertThat(totalOrderCopyGroups).hasSameSizeAs(referenceGroups);
    }

    @Test
    void copyRecordsTheCountsOfTheReferenceTypeDefinedColumns() {
        for (int group = 0; group < referenceGroups.size(); group++) {
            for (String type : TYPES) {
                Statistics<?> expected =
                        StatisticsAssertions.statisticsOf(referenceGroups.get(group), type + TYPE_DEFINED_SUFFIX);
                for (String suffix : List.of(TYPE_DEFINED_SUFFIX, TOTAL_ORDER_SUFFIX)) {
                    Statistics<?> actual = StatisticsAssertions.statisticsOf(copyGroups.get(group), type + suffix);
                    String where = type + suffix + " in row group " + group;

                    assertThat(actual.getNanCount())
                            .as("NaN count of %s", where)
                            .isEqualTo(expected.getNanCount());
                    assertThat(actual.getNumNulls())
                            .as("null count of %s", where)
                            .isEqualTo(expected.getNumNulls());
                }
            }
        }
    }

    @Test
    void copyRecordsTheBoundsOfTheReferenceWriter() {
        for (int group = 0; group < referenceGroups.size(); group++) {
            for (String type : TYPES) {
                Statistics<?> expected = referenceBounds(referenceGroups.get(group), type, group);
                for (String suffix : List.of(TYPE_DEFINED_SUFFIX, TOTAL_ORDER_SUFFIX)) {
                    Statistics<?> actual = StatisticsAssertions.statisticsOf(copyGroups.get(group), type + suffix);
                    String where = type + suffix + " in row group " + group;

                    assertThat(actual.hasNonNullValue())
                            .as("bounds of %s", where)
                            .isEqualTo(expected.hasNonNullValue());
                    assertThat(actual.getMinBytes()).as("min of %s", where).isEqualTo(expected.getMinBytes());
                    assertThat(actual.getMaxBytes()).as("max of %s", where).isEqualTo(expected.getMaxBytes());
                }
            }
        }
    }

    @Test
    void totalOrderCopyRecordsTheStatisticsOfTheReferenceTotalOrderColumns() {
        for (int group = 0; group < referenceGroups.size(); group++) {
            for (String type : TYPES) {
                Statistics<?> expected =
                        StatisticsAssertions.statisticsOf(referenceGroups.get(group), type + TOTAL_ORDER_SUFFIX);
                for (String suffix : List.of(TYPE_DEFINED_SUFFIX, TOTAL_ORDER_SUFFIX)) {
                    Statistics<?> actual =
                            StatisticsAssertions.statisticsOf(totalOrderCopyGroups.get(group), type + suffix);
                    String where = type + suffix + " in row group " + group;

                    assertThat(expected.hasNonNullValue())
                            .as("reference bounds of %s", where)
                            .isTrue();
                    StatisticsAssertions.assertSameChunkStatistics(actual, expected, where);
                }
            }
        }
    }

    /**
     * The statistics holding the bounds expected for {@code type} in a row group: those of the reference type-defined
     * column, except in the row group mixing numbers and NaN, where the reference writer records the bounds of the
     * numbers for the total order column alone.
     */
    private static Statistics<?> referenceBounds(BlockMetaData rowGroup, String type, int group) {
        String suffix = group == MIXED_GROUP ? TOTAL_ORDER_SUFFIX : TYPE_DEFINED_SUFFIX;
        return StatisticsAssertions.statisticsOf(rowGroup, type + suffix);
    }

    /**
     * Reads the rows of {@code source} and writes them to a new file with the row groups of the source and the
     * statistics of its floating-point columns in {@code floatOrder}.
     */
    private static Path rewrite(Path source, FloatColumnOrder floatOrder) throws IOException {
        ParquetSchema schema = WriteConformanceSupport.schemaViaParquetry(source);
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_GROUP))
                .floatColumnOrder(floatOrder)
                .build();
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (ParquetRecord row : WriteConformanceSupport.readAll(source)) {
            rows.add(cells(row, schema));
        }
        return WriteFixtures.writeRows(tempDir.resolve("copy-" + floatOrder + ".parquet"), schema, options, rows);
    }

    private static Map<ColumnPath, Object> cells(ParquetRecord row, ParquetSchema schema) {
        Map<ColumnPath, Object> cells = new HashMap<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            cells.put(leaf, row.get(leaf));
        }
        return cells;
    }
}
