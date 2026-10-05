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
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.GROUPS;
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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.schema.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.Row;

/**
 * The statistics of FLOAT, DOUBLE and FLOAT16 columns follow the rules of the format for the type-defined order as
 * applied by the parquet-java writer: for the same cells both writers record the same bounds, NaN counts and null
 * counts per row group, and the same column index for a chunk without NaN cells. The bounds leave the NaN cells out, a
 * zero bound is {@code -0.0} as a minimum and {@code +0.0} as a maximum, and a chunk of only NaN has no bounds.
 *
 * <p>The two writers differ on the column index of a chunk holding NaN cells. parquet-java writes none. parquetry
 * leaves it out only when a page holds nothing but NaN, as required by the format, and otherwise bounds each page by
 * its numbers and counts its NaN cells.
 */
@Tag("conformance")
class FloatStatisticsConformanceIT {

    private static final byte[] FLOAT_NEGATIVE_ZERO = {0x00, 0x00, 0x00, (byte) 0x80};
    private static final byte[] FLOAT_POSITIVE_ZERO = {0x00, 0x00, 0x00, 0x00};

    @TempDir
    static Path tempDir;

    private static List<Row> rows;
    private static Path file;

    /** The files written by parquet-java, one per row group of the fixture. */
    private static List<Path> references;

    @BeforeAll
    static void writeFiles() throws IOException {
        rows = FloatConformanceFixture.generateRows();
        file = FloatConformanceFixture.writeWithParquetry(tempDir.resolve("floats.parquet"), tempDir, rows);
        references = new ArrayList<>();
        for (int group = 0; group < GROUPS; group++) {
            Path reference = tempDir.resolve("reference-" + group + ".parquet");
            List<Row> groupRows = FloatConformanceFixture.rowsOf(rows, group);
            references.add(FloatConformanceFixture.writeWithParquetJava(reference, groupRows));
        }
    }

    @Test
    void floatingPointColumnsDeclareTheTypeDefinedOrderToParquetJava() throws IOException {
        MessageType schema = WriteConformanceSupport.readFooterViaParquetJava(file)
                .getFileMetaData()
                .getSchema();

        for (String column : FLOATING) {
            assertThat(schema.getType(column).asPrimitiveType().columnOrder())
                    .as("column order of %s", column)
                    .isEqualTo(ColumnOrder.typeDefined());
        }
    }

    @Test
    void chunkStatisticsEqualThoseOfTheParquetJavaWriter() throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(file);
        assertThat(written).hasSize(GROUPS);

        for (int group = 0; group < GROUPS; group++) {
            BlockMetaData reference = soleRowGroupOf(references.get(group));
            for (String column : FLOATING) {
                Statistics<?> actual = statistics(written.get(group), column);
                Statistics<?> expected = statistics(reference, column);

                assertSameChunkStatistics(actual, expected, column + " in row group " + group);
            }
        }
    }

    @Test
    void deprecatedBoundsOfFloatAndDoubleEqualThoseOfTheParquetJavaWriter() {
        for (int group = 0; group < GROUPS; group++) {
            for (String column : List.of(F, D)) {
                String where = column + " in row group " + group;

                assertThat(WriteConformanceSupport.deprecatedMin(file, group, column))
                        .as("deprecated min of %s", where)
                        .isEqualTo(WriteConformanceSupport.deprecatedMin(references.get(group), 0, column));
                assertThat(WriteConformanceSupport.deprecatedMax(file, group, column))
                        .as("deprecated max of %s", where)
                        .isEqualTo(WriteConformanceSupport.deprecatedMax(references.get(group), 0, column));
            }
        }
    }

    @Test
    void nanCountsMatchTheCells() throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(file);

        for (String column : FLOATING) {
            assertThat(statistics(written.get(NUMBERS_GROUP), column).getNanCount())
                    .as("NaN count of %s in the group of numbers", column)
                    .isZero();
            assertThat(statistics(written.get(NAN_GROUP), column).getNanCount())
                    .as("NaN count of %s in the group of NaN", column)
                    .isEqualTo(ROWS_PER_GROUP);
        }
        long mixedNaNs = FloatConformanceFixture.rowsOf(rows, MIXED_GROUP).stream()
                .filter(row -> !row.isNull() && Float.isNaN(row.f()))
                .count();
        assertThat(statistics(written.get(MIXED_GROUP), F).getNanCount()).isEqualTo(mixedNaNs);
    }

    @Test
    void chunkOfOnlyNaNHasNoBounds() throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(file);

        for (String column : FLOATING) {
            assertThat(statistics(written.get(NAN_GROUP), column).hasNonNullValue())
                    .as("bounds of %s in the group of NaN", column)
                    .isFalse();
        }
    }

    @Test
    void pageOfZerosIsBoundedByNegativeZeroAndPositiveZero() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(file, ZEROS_GROUP, F);
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
                ColumnIndex actual = WriteConformanceSupport.columnIndexViaParquetJava(file, group, column);
                ColumnIndex expected =
                        WriteConformanceSupport.columnIndexViaParquetJava(references.get(group), 0, column);
                String where = column + " in row group " + group;

                assertThat(actual).as("column index of %s", where).isNotNull();
                assertThat(expected).as("reference column index of %s", where).isNotNull();
                assertSameColumnIndex(actual, expected, where);
            }
        }
    }

    @Test
    void parquetJavaWritesNoColumnIndexForAChunkHoldingNaN() throws IOException {
        for (int group : List.of(MIXED_GROUP, NAN_PAGE_GROUP, NAN_GROUP)) {
            assertThat(WriteConformanceSupport.columnIndexViaParquetJava(references.get(group), 0, F))
                    .as("reference column index of row group %d", group)
                    .isNull();
        }
    }

    @Test
    void chunkWithAPageOfOnlyNaNHasNoColumnIndex() throws IOException {
        for (String column : FLOATING) {
            assertThat(WriteConformanceSupport.columnIndexViaParquetJava(file, NAN_PAGE_GROUP, column))
                    .as("column index of %s in the group with a page of NaN", column)
                    .isNull();
        }
    }

    @Test
    void pagesMixingNumbersAndNaNAreBoundedByTheirNumbers() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(file, MIXED_GROUP, F);
        assertThat(index).as("column index of the mixed group").isNotNull();
        List<Row> mixedGroup = FloatConformanceFixture.rowsOf(rows, MIXED_GROUP);

        for (int page = 0; page < index.getMinValues().size(); page++) {
            List<Float> cells = presentCellsOfPage(mixedGroup, page);
            List<Float> numbers =
                    cells.stream().filter(value -> !Float.isNaN(value)).toList();
            long nans = cells.size() - numbers.size();
            assertThat(numbers).as("numbers drawn into page %d", page).isNotEmpty();
            assertThat(nans).as("NaN cells drawn into page %d", page).isPositive();

            assertThat(floatOf(index.getMinValues().get(page)))
                    .as("min of page %d", page)
                    .isEqualTo(Collections.min(numbers));
            assertThat(floatOf(index.getMaxValues().get(page)))
                    .as("max of page %d", page)
                    .isEqualTo(Collections.max(numbers));
            assertThat(index.getNanCounts().get(page))
                    .as("NaN count of page %d", page)
                    .isEqualTo(nans);
        }
    }

    @Test
    void pageOfNullsIsANullPageOfTheColumnIndex() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(file, NUMBERS_GROUP, F);

        assertThat(index.getNullPages().get(NULL_PAGE_OF_NUMBERS_GROUP)).isTrue();
        assertThat(index.getNanCounts().get(NULL_PAGE_OF_NUMBERS_GROUP)).isZero();
    }

    /** The non-null FLOAT cells of a page of a row group of the fixture. */
    private static List<Float> presentCellsOfPage(List<Row> groupRows, int page) {
        List<Row> pageRows = groupRows.subList(page * ROWS_PER_PAGE, (page + 1) * ROWS_PER_PAGE);
        return pageRows.stream().filter(row -> !row.isNull()).map(Row::f).toList();
    }

    private static float floatOf(ByteBuffer bound) {
        return bound.duplicate().order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    private static void assertSameChunkStatistics(Statistics<?> actual, Statistics<?> expected, String where) {
        assertThat(actual.hasNonNullValue()).as("bounds of %s", where).isEqualTo(expected.hasNonNullValue());
        assertThat(actual.getMinBytes()).as("min of %s", where).isEqualTo(expected.getMinBytes());
        assertThat(actual.getMaxBytes()).as("max of %s", where).isEqualTo(expected.getMaxBytes());
        assertThat(actual.isNanCountSet()).as("NaN count of %s is set", where).isTrue();
        assertThat(actual.getNanCount()).as("NaN count of %s", where).isEqualTo(expected.getNanCount());
        assertThat(actual.getNumNulls()).as("null count of %s", where).isEqualTo(expected.getNumNulls());
    }

    private static void assertSameColumnIndex(ColumnIndex actual, ColumnIndex expected, String where) {
        assertThat(actual.getNullPages()).as("null pages of %s", where).isEqualTo(expected.getNullPages());
        assertThat(WriteConformanceSupport.bytesOf(actual.getMinValues()))
                .as("page minima of %s", where)
                .containsExactlyElementsOf(WriteConformanceSupport.bytesOf(expected.getMinValues()));
        assertThat(WriteConformanceSupport.bytesOf(actual.getMaxValues()))
                .as("page maxima of %s", where)
                .containsExactlyElementsOf(WriteConformanceSupport.bytesOf(expected.getMaxValues()));
        assertThat(actual.getNullCounts()).as("null counts of %s", where).isEqualTo(expected.getNullCounts());
        assertThat(actual.getNanCounts()).as("NaN counts of %s", where).isEqualTo(expected.getNanCounts());
        assertThat(actual.getBoundaryOrder()).as("boundary order of %s", where).isEqualTo(expected.getBoundaryOrder());
    }

    private static BlockMetaData soleRowGroupOf(Path reference) throws IOException {
        List<BlockMetaData> rowGroups = WriteConformanceSupport.rowGroupsViaParquetJava(reference);
        assertThat(rowGroups).as("row groups of the reference").hasSize(1);
        return rowGroups.getFirst();
    }

    private static Statistics<?> statistics(BlockMetaData rowGroup, String column) {
        return WriteConformanceSupport.chunkOf(rowGroup, column).getStatistics();
    }
}
