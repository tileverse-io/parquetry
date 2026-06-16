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

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * Bloom-tier end-to-end coverage against the {@code data_index_bloom_encoding_stats.parquet} fixture from the
 * {@code apache/parquet-testing} corpus. The fixture has one row group, one BYTE_ARRAY column {@code String} with 14
 * values (min {@code "Hello"}, max {@code "today"}), and a {@code BloomFilterOffset=192, BloomFilterLength=1024} entry
 * in the column metadata.
 *
 * <p>What this test pins:
 *
 * <ul>
 *   <li>A predicate value the writer never inserted should be eliminated at the bloom tier (the whole point of the
 *       tier).
 *   <li>A predicate value that <em>is</em> in the column should pass through every a-priori tier without being
 *       eliminated.
 *   <li>Turning the tier off via {@link ReadOptions#useBloomFilter()} disables both fetching and elimination.
 * </ul>
 */
class BloomFilterFixtureTest {

    private static final Path FIXTURE =
            CorpusFixtures.parquetTestingData().resolve("data_index_bloom_encoding_stats.parquet");

    @Test
    void eqAbsentValueIsEliminatedByBloom() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FIXTURE)) {
            ParquetReader dataset = ParquetReader.open(source);
            Predicate p = Pred.col("String").eq("absent_value_that_is_definitely_not_in_the_file");
            ExplainPlan plan = dataset.explain(p, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.rowGroups()).hasSize(1);
            assertThat(plan.rowGroups().get(0).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
            PruningDecision bloom = decisionAt(plan, 0, Tier.BLOOM_FILTER);
            assertThat(bloom)
                    .as("bloom tier should eliminate a known-absent value")
                    .isInstanceOf(PruningDecision.Eliminated.class);
        }
    }

    @Test
    void eqPresentValueSurvivesAllTiers() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FIXTURE)) {
            ParquetReader dataset = ParquetReader.open(source);
            // "Hello" is the recorded min of the column; it must be in both the stats range and the bloom bitset.
            Predicate p = Pred.col("String").eq("Hello");
            ExplainPlan plan = dataset.explain(p, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.rowGroups()).hasSize(1);
            assertThat(plan.rowGroups().get(0).outcome())
                    .as("no tier should eliminate a value that the fixture actually contains")
                    .isNotEqualTo(RowGroupOutcome.ELIMINATED);
            PruningDecision bloom = decisionAt(plan, 0, Tier.BLOOM_FILTER);
            assertThat(bloom)
                    .as("a probed value that may be present is Inconclusive, not NotApplied")
                    .isInstanceOf(PruningDecision.Inconclusive.class);
        }
    }

    @Test
    void disablingBloomTierStopsEliminations() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(FIXTURE)) {
            ParquetReader dataset = ParquetReader.open(source);
            Predicate p = Pred.col("String").eq("absent_value_that_is_definitely_not_in_the_file");

            ReadOptions noBloom = ReadOptions.builder().useBloomFilter(false).build();
            ExplainPlan plan = dataset.explain(p, Projection.ALL, noBloom);

            assertThat(plan.rowGroups()).hasSize(1);
            assertThat(plan.rowGroups().get(0).outcome())
                    .as("disabling the bloom tier should hand the row group through to the data path")
                    .isNotEqualTo(RowGroupOutcome.ELIMINATED);
            PruningDecision bloom = decisionAt(plan, 0, Tier.BLOOM_FILTER);
            assertThat(bloom)
                    .as("bloom tier should be NotApplied when disabled in ReadOptions")
                    .isInstanceOf(PruningDecision.NotApplied.class);
        }
    }

    private static PruningDecision decisionAt(ExplainPlan plan, int rowGroupIndex, Tier tier) {
        return plan.rowGroups().get(rowGroupIndex).tiers().stream()
                .filter(d -> d.tier() == tier)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No decision for tier " + tier));
    }
}
