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

import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SortedLongPositionSet;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * A row group declaring no rows holds nothing to read. pyarrow writes one for an empty table: its column chunks declare
 * no bytes, or a dictionary page where a data page is stored. Fetching those chunks fails, and a read must skip them.
 *
 * <p>The fixtures under {@code src/test/resources/pyarrow-fixtures/} were written by pyarrow 26.0.0:
 *
 * <ul>
 *   <li>{@code zero-rows-plain} and {@code zero-rows-dictionary}: an empty table of an {@code int32} column {@code i}
 *       and a string column {@code s}, written with {@code use_dictionary} off and on.
 *   <li>{@code zero-rows-geometry}: an empty GeoParquet table, without a bbox in its metadata.
 *   <li>{@code zero-rows-between}: row groups of 3, 0 and 4 rows. Column {@code i} counts from 0 to 6 and the geometry
 *       is the point {@code (i, i)}.
 * </ul>
 */
class EmptyRowGroupReadTest {

    private static final ColumnPath I = ColumnPath.of("i");
    private static final Predicate ABOVE_THREE = col("i").gt(3);
    private static final ReadOptions METADATA_PRUNING_OFF = ReadOptions.builder()
            .useStatsFilter(false)
            .useDictionaryFilter(false)
            .useColumnIndexFilter(false)
            .useBloomFilter(false)
            .build();

    @ParameterizedTest(name = "{0}, {1}")
    @MethodSource("emptyFilesAndOptions")
    void filterSelectsNoRows(String fixture, String pruning, ReadOptions options) throws URISyntaxException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixtureFile(fixture))) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.rowGroups()).as("row groups of the file").hasSize(1);
            assertThat(reader.count(ABOVE_THREE, options)).as("count").isZero();
            assertThat(valuesRead(reader, ABOVE_THREE, options)).as("rows read").isEmpty();
            assertThat(rowsReadInBatches(reader, ABOVE_THREE, options))
                    .as("rows read in batches")
                    .isZero();
        }
    }

    static Stream<Arguments> emptyFilesAndOptions() {
        return Stream.of("zero-rows-plain.parquet", "zero-rows-dictionary.parquet")
                .flatMap(fixture -> Stream.of(
                        Arguments.of(fixture, "pruning on", ReadOptions.DEFAULTS),
                        Arguments.of(fixture, "pruning off", METADATA_PRUNING_OFF)));
    }

    // An unfiltered bounds query scans the row groups without a box of their own.
    @Test
    void boundsOfAFileWithoutRows() throws URISyntaxException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixtureFile("zero-rows-geometry.parquet"))) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            assertThat(reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .as("unfiltered bounds")
                    .isEmpty();
            assertThat(reader.bounds(ABOVE_THREE, ReadOptions.DEFAULTS))
                    .as("filtered bounds")
                    .isEmpty();
        }
    }

    // The rows after the empty row group keep their values and their positions in the file.
    @Test
    void emptyGroupBetweenTwoWithRows() throws URISyntaxException {
        try (ByteRangeSource source = ByteRangeSource.ofFile(fixtureFile("zero-rows-between.parquet"))) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            SortedLongPositionSet rowsOneAndFive = SortedLongPositionSet.of(new long[] {1L, 5L});
            Predicate withoutRowsOneAndFive = new Predicate.RowIndexExcluded(ColumnPath.of("_pos"), rowsOneAndFive);

            assertThat(reader.count(ABOVE_THREE, ReadOptions.DEFAULTS)).isEqualTo(3L);
            assertThat(valuesRead(reader, ABOVE_THREE, ReadOptions.DEFAULTS)).containsExactly(4, 5, 6);
            assertThat(valuesRead(reader, withoutRowsOneAndFive, ReadOptions.DEFAULTS))
                    .containsExactly(0, 2, 3, 4, 6);
            BoundingBox bounds =
                    reader.bounds(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS).orElseThrow();
            assertThat(bounds.xmin()).isZero();
            assertThat(bounds.xmax()).isEqualTo(6.0);
        }
    }

    private static Path fixtureFile(String name) throws URISyntaxException {
        URL resource = EmptyRowGroupReadTest.class.getResource("/pyarrow-fixtures/" + name);
        return Path.of(resource.toURI());
    }

    private static List<Integer> valuesRead(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        try (Stream<ParquetRecord> rows = reader.read(predicate, Projection.ALL, options)) {
            return rows.map(row -> row.getInt(I)).toList();
        }
    }

    private static long rowsReadInBatches(ParquetFileReader reader, Predicate predicate, ReadOptions options) {
        try (Stream<ParquetRecordBatch> batches = reader.readBatches(predicate, Projection.ALL, options)) {
            return batches.mapToLong(ParquetRecordBatch::rowCount).sum();
        }
    }
}
