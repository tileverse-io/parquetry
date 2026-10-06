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

import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;

/**
 * The chunk statistics read by parquet-java from a written file, and the comparisons of the chunk statistics and column
 * indexes of two files.
 */
final class StatisticsAssertions {

    private StatisticsAssertions() {}

    /** The statistics of the chunk of {@code column} in {@code rowGroup}. */
    static Statistics<?> statisticsOf(BlockMetaData rowGroup, String column) {
        return WriteConformanceSupport.chunkOf(rowGroup, column).getStatistics();
    }

    static void assertSameChunkStatistics(Statistics<?> actual, Statistics<?> expected, String where) {
        assertThat(actual.hasNonNullValue()).as("bounds of %s", where).isEqualTo(expected.hasNonNullValue());
        assertThat(actual.getMinBytes()).as("min of %s", where).isEqualTo(expected.getMinBytes());
        assertThat(actual.getMaxBytes()).as("max of %s", where).isEqualTo(expected.getMaxBytes());
        assertThat(actual.isNanCountSet()).as("NaN count of %s is set", where).isTrue();
        assertThat(actual.getNanCount()).as("NaN count of %s", where).isEqualTo(expected.getNanCount());
        assertThat(actual.getNumNulls()).as("null count of %s", where).isEqualTo(expected.getNumNulls());
    }

    static void assertSameColumnIndex(ColumnIndex actual, ColumnIndex expected, String where) {
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
}
