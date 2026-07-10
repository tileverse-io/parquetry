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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Verifies that the {@code explainAnalyze} drain tallies bytes from both byte-I/O paths: the index sections the filter
 * pipeline reads (column index, offset index, bloom filter) and the column-chunk pages the decode fetches.
 *
 * <p>The fixture is {@code data_index_bloom_encoding_stats.parquet} from the {@code apache/parquet-testing} corpus: one
 * row group, one BYTE_ARRAY column {@code String} with a column index, an offset index, and a bloom filter. A predicate
 * matching a value present in the column ({@code "Hello"}, the recorded min) passes the column-index and bloom tiers,
 * then leaves the row group to decode. The drain therefore reads all four section kinds, and every asserted byte field
 * is strictly positive.
 */
class QueryFetchAccountingIT {

    private static final Path FILE =
            CorpusFixtures.parquetTestingData().resolve("data_index_bloom_encoding_stats.parquet");

    @Test
    void analyzeTalliesIndexBloomAndPageBytes() {
        Predicate present = Pred.col("String").eq("Hello");
        try (ByteRangeSource source = ByteRangeSource.ofFile(FILE)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            ExplainPlan plan = reader.explainAnalyze(present, Projection.ALL, ReadOptions.DEFAULTS);
            FetchStats stats = plan.execution().orElseThrow().totalFetch();

            assertThat(stats.pageBytes())
                    .as("the surviving row group decodes its predicate column, fetching page bytes")
                    .isPositive();
            assertThat(stats.columnIndexBytes())
                    .as("the column-index tier reads the column index")
                    .isPositive();
            assertThat(stats.offsetIndexBytes())
                    .as("the column-index tier reads the offset index alongside the column index")
                    .isPositive();
            assertThat(stats.bloomFilterBytes())
                    .as("the bloom tier reads the bloom filter")
                    .isPositive();
            assertThat(stats.totalBytes()).isPositive();
            assertThat(stats.fetchCount())
                    .as("at least the residual page fetch increments the fetch count")
                    .isPositive();
        }
    }
}
