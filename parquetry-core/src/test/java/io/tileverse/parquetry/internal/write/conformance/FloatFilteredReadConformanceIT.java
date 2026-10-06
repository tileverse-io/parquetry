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
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.H;
import static io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.ID;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.internal.write.conformance.FloatConformanceFixture.Row;
import io.tileverse.parquetry.testsupport.ReadFixtures;

/**
 * Filtered reads of FLOAT, DOUBLE and FLOAT16 columns written by parquetry return the same rows with the metadata
 * pruning tiers on and off, in the parquet-java reader and in the parquetry reader: the statistics, NaN counts and
 * column indexes of the file prune no matching row.
 */
@Tag("conformance")
class FloatFilteredReadConformanceIT {

    @TempDir
    static Path tempDir;

    private static List<Row> rows;
    private static Path file;

    @BeforeAll
    static void writeFile() throws IOException {
        rows = FloatConformanceFixture.generateRows();
        file = FloatConformanceFixture.writeWithParquetry(tempDir.resolve("floats.parquet"), tempDir, rows);
    }

    @Test
    void parquetJavaFilteredReadsAgreeWithAndWithoutMetadataPruning() throws IOException {
        for (FilterPredicate filter : parquetJavaFilters()) {
            List<Integer> pruned = WriteConformanceSupport.idsReadByParquetJava(file, ID, filter, true);
            List<Integer> scanned = WriteConformanceSupport.idsReadByParquetJava(file, ID, filter, false);

            assertThat(scanned).as("rows matching %s", filter).isNotEmpty();
            assertThat(pruned).as("ids read by parquet-java for %s", filter).containsExactlyElementsOf(scanned);
        }
    }

    @Test
    void parquetryFilteredReadsAgreeWithAndWithoutMetadataPruning() {
        for (FilterCase filter : parquetryFilters()) {
            long expected = rows.stream()
                    .filter(row -> !row.isNull())
                    .filter(filter.matching()::test)
                    .count();
            long pruned = WriteConformanceSupport.rowsReadByParquetry(file, filter.predicate(), ReadOptions.DEFAULTS);
            long scanned = WriteConformanceSupport.rowsReadByParquetry(
                    file, filter.predicate(), ReadFixtures.METADATA_PRUNING_OFF);

            assertThat(expected).as("rows matching %s", filter.predicate()).isPositive();
            assertThat(pruned)
                    .as("rows read for %s", filter.predicate())
                    .isEqualTo(scanned)
                    .isEqualTo(expected);
        }
    }

    /** A predicate and the test telling the non-null rows expected from it under IEEE 754 comparison. */
    private record FilterCase(Predicate predicate, Matching matching) {}

    @FunctionalInterface
    private interface Matching {
        boolean test(Row row);
    }

    private static List<FilterCase> parquetryFilters() {
        return List.of(
                new FilterCase(Pred.col(F).gt(1.0f), row -> row.f() > 1.0f),
                new FilterCase(Pred.col(F).gtEq(0.0f), row -> row.f() >= 0.0f),
                new FilterCase(Pred.col(F).lt(-1.0f), row -> row.f() < -1.0f),
                new FilterCase(Pred.col(F).ltEq(-0.0f), row -> row.f() <= -0.0f),
                new FilterCase(Pred.col(F).eq(0.0f), row -> row.f() == 0.0f),
                new FilterCase(Pred.col(F).eq(Float.NaN), row -> Float.isNaN(row.f())),
                new FilterCase(Pred.col(F).notEq(Float.NaN), row -> !Float.isNaN(row.f())),
                new FilterCase(Pred.col(F).notEq(0.0f), row -> row.f() != 0.0f),
                new FilterCase(Pred.col(D).gt(1.0), row -> row.d() > 1.0),
                new FilterCase(Pred.col(D).lt(-1.0), row -> row.d() < -1.0),
                new FilterCase(Pred.col(D).eq(0.0), row -> row.d() == 0.0),
                new FilterCase(Pred.col(D).eq(Double.NaN), row -> Double.isNaN(row.d())),
                new FilterCase(Pred.col(D).notEq(Double.NaN), row -> !Double.isNaN(row.d())));
    }

    private static List<FilterPredicate> parquetJavaFilters() {
        Binary halfOne = half(Float.floatToFloat16(1.0f));
        Binary halfNaN = half(aHalfNaNOfTheFile());
        return List.of(
                FilterApi.gt(FilterApi.floatColumn(F), 1.0f),
                FilterApi.gtEq(FilterApi.floatColumn(F), 0.0f),
                FilterApi.lt(FilterApi.floatColumn(F), -1.0f),
                FilterApi.ltEq(FilterApi.floatColumn(F), -0.0f),
                FilterApi.eq(FilterApi.floatColumn(F), 0.0f),
                FilterApi.eq(FilterApi.floatColumn(F), -0.0f),
                FilterApi.eq(FilterApi.floatColumn(F), Float.NaN),
                FilterApi.notEq(FilterApi.floatColumn(F), Float.NaN),
                FilterApi.notEq(FilterApi.floatColumn(F), 0.0f),
                FilterApi.gt(FilterApi.doubleColumn(D), 1.0),
                FilterApi.lt(FilterApi.doubleColumn(D), -1.0),
                FilterApi.eq(FilterApi.doubleColumn(D), 0.0),
                FilterApi.eq(FilterApi.doubleColumn(D), Double.NaN),
                FilterApi.notEq(FilterApi.doubleColumn(D), Double.NaN),
                FilterApi.gt(FilterApi.binaryColumn(H), halfOne),
                FilterApi.lt(FilterApi.binaryColumn(H), halfOne),
                FilterApi.eq(FilterApi.binaryColumn(H), halfNaN),
                FilterApi.notEq(FilterApi.binaryColumn(H), halfNaN));
    }

    /** The bits of a FLOAT16 NaN cell of the file: parquet-java compares the cells of the column as bytes. */
    private static short aHalfNaNOfTheFile() {
        return rows.stream()
                .filter(row -> !row.isNull() && Float.isNaN(row.f()))
                .map(Row::h)
                .findFirst()
                .orElseThrow();
    }

    private static Binary half(short bits) {
        return Binary.fromConstantByteArray(WriteFixtures.halfFloatBytes(bits));
    }
}
