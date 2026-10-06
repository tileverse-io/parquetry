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

import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.D;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.F;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOATING;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOAT_NEGATIVE_ZERO;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOAT_POSITIVE_ZERO;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.GROUPS;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ID;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.MIXED_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NAN_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NAN_PAGE_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NEGATIVE_ZERO_PAGE_OF_ZEROS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NULL_PAGE_OF_NUMBERS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NUMBERS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.POSITIVE_ZERO_PAGE_OF_ZEROS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ROWS_PER_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ROWS_PER_PAGE;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ZEROS_AND_NEGATIVE_NUMBERS_PAGE_OF_ZEROS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ZEROS_AND_POSITIVE_NUMBERS_PAGE_OF_ZEROS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ZEROS_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.Row;

/**
 * The statistics of FLOAT, DOUBLE and FLOAT16 columns follow the rules of the format as applied by the parquet-java
 * writer, in the type-defined order and in IEEE 754 total order: for the same cells both writers declare the same
 * column order and record the same bounds, NaN counts, null counts and deprecated bounds per row group.
 *
 * <p>In the type-defined order the bounds leave the NaN cells out, a zero bound is {@code -0.0} as a minimum and
 * {@code +0.0} as a maximum, and a chunk of only NaN has no bounds. The two writers differ there on the column index of
 * a chunk holding NaN cells. parquet-java writes none. parquetry leaves it out only when a page holds nothing but NaN,
 * as required by the format, and otherwise bounds each page by its numbers and counts its NaN cells.
 * {@link FloatTotalOrderConformanceIT} covers the bounds and the column index of total order.
 */
@Tag("conformance")
class FloatStatisticsConformanceIT {

    @TempDir
    static Path tempDir;

    private static List<Row> rows;

    /** The files written by parquetry, keyed by the order of their float columns. */
    private static Map<FloatColumnOrder, Path> filesByOrder;

    /**
     * The files written by parquet-java, one per row group of the fixture, keyed by the order of their float columns.
     */
    private static Map<FloatColumnOrder, List<Path>> referencesByOrder;

    @BeforeAll
    static void writeFiles() throws IOException {
        rows = FloatConformanceFixture.generateRows();
        filesByOrder = new EnumMap<>(FloatColumnOrder.class);
        referencesByOrder = new EnumMap<>(FloatColumnOrder.class);
        for (FloatColumnOrder order : FloatColumnOrder.values()) {
            Path target = tempDir.resolve("floats-" + order + ".parquet");
            filesByOrder.put(order, FloatConformanceFixture.writeWithParquetry(target, tempDir, rows, order));
            String prefix = "reference-" + order;
            referencesByOrder.put(order, FloatConformanceFixture.writeReferences(tempDir, prefix, rows, order));
        }
    }

    /** The parquetry file of the type-defined order. */
    private static Path typeDefinedFile() {
        return filesByOrder.get(FloatColumnOrder.TYPE_DEFINED);
    }

    /** The parquet-java files of the type-defined order, one per row group of the fixture. */
    private static List<Path> typeDefinedReferences() {
        return referencesByOrder.get(FloatColumnOrder.TYPE_DEFINED);
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void floatingPointColumnsDeclareTheirOrderToParquetJava(FloatColumnOrder order) throws IOException {
        MessageType schema = WriteConformanceSupport.readFooterViaParquetJava(filesByOrder.get(order))
                .getFileMetaData()
                .getSchema();

        for (String column : FLOATING) {
            assertThat(schema.getType(column).asPrimitiveType().columnOrder())
                    .as("column order of %s", column)
                    .isEqualTo(FloatConformanceFixture.parquetJavaOrder(order));
        }
        assertThat(schema.getType(ID).asPrimitiveType().columnOrder())
                .as("column order of the INT32 column")
                .isEqualTo(ColumnOrder.typeDefined());
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void chunkStatisticsEqualThoseOfTheParquetJavaWriter(FloatColumnOrder order) throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(filesByOrder.get(order));
        assertThat(written).hasSize(GROUPS);

        for (int group = 0; group < GROUPS; group++) {
            BlockMetaData reference = WriteConformanceSupport.soleRowGroupOf(
                    referencesByOrder.get(order).get(group));
            for (String column : FLOATING) {
                Statistics<?> actual = StatisticsAssertions.statisticsOf(written.get(group), column);
                Statistics<?> expected = StatisticsAssertions.statisticsOf(reference, column);

                StatisticsAssertions.assertSameChunkStatistics(actual, expected, column + " in row group " + group);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void boundsHeldByTheFooterEqualThoseOfTheParquetJavaWriter(FloatColumnOrder order) {
        // parquet-java drops NaN bounds and rewrites zero bounds as it reads type-defined statistics: the two footers
        // are compared byte for byte, as read by parquetry.
        Path written = filesByOrder.get(order);
        for (int group = 0; group < GROUPS; group++) {
            Path reference = referencesByOrder.get(order).get(group);
            for (String column : FLOATING) {
                String where = column + " in row group " + group;

                assertThat(WriteConformanceSupport.footerMin(written, group, column))
                        .as("min_value of %s", where)
                        .isEqualTo(WriteConformanceSupport.footerMin(reference, 0, column));
                assertThat(WriteConformanceSupport.footerMax(written, group, column))
                        .as("max_value of %s", where)
                        .isEqualTo(WriteConformanceSupport.footerMax(reference, 0, column));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void deprecatedBoundsOfFloatAndDoubleEqualThoseOfTheParquetJavaWriter(FloatColumnOrder order) {
        Path written = filesByOrder.get(order);
        for (int group = 0; group < GROUPS; group++) {
            Path reference = referencesByOrder.get(order).get(group);
            for (String column : List.of(F, D)) {
                String where = column + " in row group " + group;

                assertThat(WriteConformanceSupport.deprecatedMin(written, group, column))
                        .as("deprecated min of %s", where)
                        .isEqualTo(WriteConformanceSupport.deprecatedMin(reference, 0, column));
                assertThat(WriteConformanceSupport.deprecatedMax(written, group, column))
                        .as("deprecated max of %s", where)
                        .isEqualTo(WriteConformanceSupport.deprecatedMax(reference, 0, column));
            }
        }
    }

    @ParameterizedTest
    @EnumSource(FloatColumnOrder.class)
    void nanCountsMatchTheCells(FloatColumnOrder order) throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(filesByOrder.get(order));

        for (String column : FLOATING) {
            assertThat(StatisticsAssertions.statisticsOf(written.get(NUMBERS_GROUP), column)
                            .getNanCount())
                    .as("NaN count of %s in the group of numbers", column)
                    .isZero();
            assertThat(StatisticsAssertions.statisticsOf(written.get(NAN_GROUP), column)
                            .getNanCount())
                    .as("NaN count of %s in the group of NaN", column)
                    .isEqualTo(ROWS_PER_GROUP);
        }
        long mixedNaNs = FloatConformanceFixture.rowsOf(rows, MIXED_GROUP).stream()
                .filter(row -> !row.isNull() && Float.isNaN(row.f()))
                .count();
        Statistics<?> mixed = StatisticsAssertions.statisticsOf(written.get(MIXED_GROUP), F);
        assertThat(mixed.getNanCount()).isEqualTo(mixedNaNs);
    }

    @Test
    void chunkOfOnlyNaNHasNoBounds() throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(typeDefinedFile());

        for (String column : FLOATING) {
            assertThat(StatisticsAssertions.statisticsOf(written.get(NAN_GROUP), column)
                            .hasNonNullValue())
                    .as("bounds of %s in the group of NaN", column)
                    .isFalse();
        }
    }

    @Test
    void pageOfZerosIsBoundedByNegativeZeroAndPositiveZero() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(typeDefinedFile(), ZEROS_GROUP, F);
        List<byte[]> minValues = WriteConformanceSupport.bytesOf(index.getMinValues());
        List<byte[]> maxValues = WriteConformanceSupport.bytesOf(index.getMaxValues());

        for (int page : List.of(POSITIVE_ZERO_PAGE_OF_ZEROS_GROUP, NEGATIVE_ZERO_PAGE_OF_ZEROS_GROUP)) {
            assertThat(minValues.get(page)).as("min of page %d", page).isEqualTo(FLOAT_NEGATIVE_ZERO);
            assertThat(maxValues.get(page)).as("max of page %d", page).isEqualTo(FLOAT_POSITIVE_ZERO);
        }
        assertThat(minValues.get(ZEROS_AND_POSITIVE_NUMBERS_PAGE_OF_ZEROS_GROUP))
                .as("min of the page of zeros and positive numbers")
                .isEqualTo(FLOAT_NEGATIVE_ZERO);
        assertThat(maxValues.get(ZEROS_AND_NEGATIVE_NUMBERS_PAGE_OF_ZEROS_GROUP))
                .as("max of the page of zeros and negative numbers")
                .isEqualTo(FLOAT_POSITIVE_ZERO);
    }

    @Test
    void columnIndexOfAChunkWithoutNaNEqualsThatOfTheParquetJavaWriter() throws IOException {
        for (int group : List.of(NUMBERS_GROUP, ZEROS_GROUP)) {
            for (String column : FLOATING) {
                ColumnIndex actual =
                        WriteConformanceSupport.columnIndexViaParquetJava(typeDefinedFile(), group, column);
                ColumnIndex expected = WriteConformanceSupport.columnIndexViaParquetJava(
                        typeDefinedReferences().get(group), 0, column);
                String where = column + " in row group " + group;

                assertThat(actual).as("column index of %s", where).isNotNull();
                assertThat(expected).as("reference column index of %s", where).isNotNull();
                StatisticsAssertions.assertSameColumnIndex(actual, expected, where);
            }
        }
    }

    @Test
    void parquetJavaWritesNoColumnIndexForAChunkHoldingNaN() throws IOException {
        for (int group : List.of(MIXED_GROUP, NAN_PAGE_GROUP, NAN_GROUP)) {
            assertThat(WriteConformanceSupport.columnIndexViaParquetJava(
                            typeDefinedReferences().get(group), 0, F))
                    .as("reference column index of row group %d", group)
                    .isNull();
        }
    }

    @Test
    void chunkWithAPageOfOnlyNaNHasNoColumnIndex() throws IOException {
        for (String column : FLOATING) {
            assertThat(WriteConformanceSupport.columnIndexViaParquetJava(typeDefinedFile(), NAN_PAGE_GROUP, column))
                    .as("column index of %s in the group with a page of NaN", column)
                    .isNull();
        }
    }

    @Test
    void pagesMixingNumbersAndNaNAreBoundedByTheirNumbers() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(typeDefinedFile(), MIXED_GROUP, F);
        assertThat(index).as("column index of the mixed group").isNotNull();
        List<Row> mixedGroup = FloatConformanceFixture.rowsOf(rows, MIXED_GROUP);

        for (int page = 0; page < index.getMinValues().size(); page++) {
            List<Float> cells = presentCellsOfPage(mixedGroup, page);
            List<Float> numbers =
                    cells.stream().filter(value -> !Float.isNaN(value)).toList();
            long nans = cells.size() - numbers.size();
            assertThat(numbers).as("numbers drawn into page %d", page).isNotEmpty();
            assertThat(nans).as("NaN cells drawn into page %d", page).isPositive();

            assertThat(WriteConformanceSupport.floatOf(index.getMinValues().get(page)))
                    .as("min of page %d", page)
                    .isEqualTo(Collections.min(numbers));
            assertThat(WriteConformanceSupport.floatOf(index.getMaxValues().get(page)))
                    .as("max of page %d", page)
                    .isEqualTo(Collections.max(numbers));
            assertThat(index.getNanCounts().get(page))
                    .as("NaN count of page %d", page)
                    .isEqualTo(nans);
        }
    }

    @Test
    void pageOfNullsIsANullPageOfTheColumnIndex() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(typeDefinedFile(), NUMBERS_GROUP, F);

        assertThat(index.getNullPages().get(NULL_PAGE_OF_NUMBERS_GROUP)).isTrue();
        assertThat(index.getNanCounts().get(NULL_PAGE_OF_NUMBERS_GROUP)).isZero();
    }

    /** The non-null FLOAT cells of a page of a row group of the fixture. */
    private static List<Float> presentCellsOfPage(List<Row> groupRows, int page) {
        List<Row> pageRows = groupRows.subList(page * ROWS_PER_PAGE, (page + 1) * ROWS_PER_PAGE);
        return pageRows.stream().filter(row -> !row.isNull()).map(Row::f).toList();
    }
}
