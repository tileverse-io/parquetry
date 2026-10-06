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

import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.F;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOATING;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOAT_NEGATIVE_ZERO;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.FLOAT_POSITIVE_ZERO;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.GROUPS;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NAN_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NAN_PAGE_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NAN_PAGE_OF_NAN_PAGE_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.NEGATIVE_ZERO_PAGE_OF_ZEROS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.POSITIVE_ZERO_PAGE_OF_ZEROS_GROUP;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ROWS_PER_PAGE;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ZEROS_GROUP;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.Row;

/**
 * A file written with IEEE 754 total order for its FLOAT, DOUBLE and FLOAT16 columns follows the rules of the format
 * for that order as applied by the parquet-java writer: for the same cells both writers record the same column index
 * page by page, a zero bound keeps its sign, and a chunk or page of only NaN is bounded by NaN and keeps its column
 * index. {@link FloatStatisticsConformanceIT} compares the chunk statistics of the two writers in both orders.
 */
@Tag("conformance")
class FloatTotalOrderConformanceIT {

    @TempDir
    static Path tempDir;

    private static Path file;

    /** The files written by parquet-java in total order, one per row group of the fixture. */
    private static List<Path> references;

    @BeforeAll
    static void writeFiles() throws IOException {
        List<Row> rows = FloatConformanceFixture.generateRows();
        FloatColumnOrder totalOrder = FloatColumnOrder.IEEE_754_TOTAL_ORDER;
        file = FloatConformanceFixture.writeWithParquetry(tempDir.resolve("floats.parquet"), tempDir, rows, totalOrder);
        references = FloatConformanceFixture.writeReferences(tempDir, "reference", rows, totalOrder);
    }

    @Test
    void columnIndexEqualsThatOfTheParquetJavaWriter() throws IOException {
        for (int group = 0; group < GROUPS; group++) {
            for (String column : FLOATING) {
                ColumnIndex actual = WriteConformanceSupport.columnIndexViaParquetJava(file, group, column);
                ColumnIndex expected =
                        WriteConformanceSupport.columnIndexViaParquetJava(references.get(group), 0, column);
                String where = column + " in row group " + group;

                assertThat(actual).as("column index of %s", where).isNotNull();
                assertThat(expected).as("reference column index of %s", where).isNotNull();
                StatisticsAssertions.assertSameColumnIndex(actual, expected, where);
            }
        }
    }

    @Test
    void chunkOfOnlyNaNIsBoundedByNaN() throws IOException {
        List<BlockMetaData> written = WriteConformanceSupport.rowGroupsViaParquetJava(file);
        Statistics<?> statistics = StatisticsAssertions.statisticsOf(written.get(NAN_GROUP), F);

        assertThat(statistics.hasNonNullValue()).isTrue();
        assertThat(WriteConformanceSupport.floatOf(statistics.getMinBytes())).isNaN();
        assertThat(WriteConformanceSupport.floatOf(statistics.getMaxBytes())).isNaN();
    }

    @Test
    void pageOfOnlyNaNIsBoundedByNaNInTheColumnIndexOfItsChunk() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(file, NAN_PAGE_GROUP, F);

        assertThat(index).isNotNull();
        ByteBuffer min = index.getMinValues().get(NAN_PAGE_OF_NAN_PAGE_GROUP);
        ByteBuffer max = index.getMaxValues().get(NAN_PAGE_OF_NAN_PAGE_GROUP);
        assertThat(index.getNanCounts().get(NAN_PAGE_OF_NAN_PAGE_GROUP)).isEqualTo(ROWS_PER_PAGE);
        assertThat(WriteConformanceSupport.floatOf(min)).isNaN();
        assertThat(WriteConformanceSupport.floatOf(max)).isNaN();
    }

    @Test
    void zeroBoundsKeepTheSignOfTheirCells() throws IOException {
        ColumnIndex index = WriteConformanceSupport.columnIndexViaParquetJava(file, ZEROS_GROUP, F);
        List<byte[]> minValues = WriteConformanceSupport.bytesOf(index.getMinValues());
        List<byte[]> maxValues = WriteConformanceSupport.bytesOf(index.getMaxValues());

        assertThat(minValues.get(POSITIVE_ZERO_PAGE_OF_ZEROS_GROUP))
                .as("min of the page of +0.0")
                .isEqualTo(FLOAT_POSITIVE_ZERO);
        assertThat(maxValues.get(NEGATIVE_ZERO_PAGE_OF_ZEROS_GROUP))
                .as("max of the page of -0.0")
                .isEqualTo(FLOAT_NEGATIVE_ZERO);
    }
}
