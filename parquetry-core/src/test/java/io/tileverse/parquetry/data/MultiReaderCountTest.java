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
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

class MultiReaderCountTest {

    private static final Path FILE = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    @Test
    void countSumsAcrossReaders() {
        try (ByteRangeSource a = ByteRangeSource.ofFile(FILE);
                ByteRangeSource b = ByteRangeSource.ofFile(FILE)) {
            ParquetReader ra = ParquetReader.open(a);
            ParquetReader rb = ParquetReader.open(b);
            long single =
                    ra.rowGroups().stream().mapToLong(RowGroupSummary::rowCount).sum();

            DefaultParquetDataset ds = new DefaultParquetDataset(List.of(ra, rb));
            assertThat(ds.count()).isEqualTo(2 * single);
        }
    }

    // A filtered multi-reader count drives the concurrent residual fan-out: each file decodes its own surviving rows
    // on its own virtual thread, and the per-file counts are summed.
    @Test
    void filteredCountSumsResidualAcrossReaders() {
        Predicate residual = Pred.col("id").gt(4);
        try (ByteRangeSource single = ByteRangeSource.ofFile(FILE);
                ByteRangeSource a = ByteRangeSource.ofFile(FILE);
                ByteRangeSource b = ByteRangeSource.ofFile(FILE)) {
            long perFile = ParquetReader.open(single).count(residual, ReadOptions.DEFAULTS);
            assertThat(perFile).isPositive();

            DefaultParquetDataset ds = new DefaultParquetDataset(List.of(ParquetReader.open(a), ParquetReader.open(b)));
            assertThat(ds.count(residual)).isEqualTo(2 * perFile);
        }
    }
}
