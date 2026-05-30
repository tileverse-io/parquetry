/*
 * Copyright (c) 2026 Multivers.io
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

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

class ParquetDatasetCountTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    void unfilteredCountEqualsSumOfRowGroupRowCounts() {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset ds = ParquetDataset.open(src);
            long expected =
                    ds.rowGroups().stream().mapToLong(RowGroupSummary::rowCount).sum();
            assertThat(ds.count()).isEqualTo(expected);
        }
    }

    @Test
    void unfilteredCountEqualsStreamedRecordCount() {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset ds = ParquetDataset.open(src);
            long streamed;
            try (Stream<ParquetRecord> rows = ds.read()) {
                streamed = rows.count();
            }
            assertThat(ds.count(Predicate.ALWAYS_TRUE)).isEqualTo(streamed);
        }
    }

    // A filtered count must agree with streaming the same predicate and counting records, now that the dataset
    // delegates to the optimized per-reader count instead of traversing the read stream itself.
    @Test
    void filteredCountEqualsStreamedRecordCount() {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset ds = ParquetDataset.open(src);
            Predicate residual = Pred.col("id").gt(4);
            long streamed;
            try (Stream<ParquetRecord> rows = ds.read(residual, Projection.ALL, ReadOptions.DEFAULTS)) {
                streamed = rows.count();
            }
            long total =
                    ds.rowGroups().stream().mapToLong(RowGroupSummary::rowCount).sum();
            assertThat(streamed).isPositive().isLessThan(total); // a real residual: some rows match, some do not
            assertThat(ds.count(residual)).isEqualTo(streamed);
        }
    }

    @Test
    void alwaysFalsePredicateCountsZero() {
        try (ByteRangeSource src = ByteRangeSource.ofFile(FILE)) {
            ParquetDataset ds = ParquetDataset.open(src);
            assertThat(ds.count(Predicate.ALWAYS_FALSE)).isZero();
        }
    }
}
