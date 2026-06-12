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
package io.tileverse.parquetry.filter.explain;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.observe.PhaseTimings;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class RendererExecutionTest {

    @Test
    void plainExplainOmitsExecutionColumnsAndKey() {
        ExplainPlan plan = planWithAllTiersActive();

        String table = plan.toAsciiTable();
        assertThat(table).doesNotContain("actual").doesNotContain("Total");

        String json = plan.toJson();
        assertThat(json).doesNotContain("\"execution\"");
    }

    @Test
    void analyzeAddsExecutionColumnsAndTotals() {
        ExplainPlan plan = planWithAllTiersActive().withExecution(stats(), Map.of(0, rowGroupRead(0)));

        String table = plan.toAsciiTable();
        assertThat(table).contains("actual").contains("bytes").contains("time").contains("Total");
        assertThat(table).contains("17"); // rows matched
        assertThat(table).contains("4096"); // bytes read

        String json = plan.toJson();
        assertThat(json).contains("\"execution\"");
        assertThat(json).contains("\"rowsMatched\":42");
        assertThat(json).contains("\"rowGroupsRead\":1");
    }

    @Test
    void timeColumnAbsentWhenNoTimings() {
        RowGroupRead noTimings =
                new RowGroupRead(0, 100, 17, 3, 1, new FetchStats(4096, 0, 0, 0, 0, 1), Optional.empty());
        QueryStats noCpu = new QueryStats(
                5_000_000L, 100, 42, Map.of(), 1, 1, 3, 1, new FetchStats(4096, 0, 0, 0, 0, 1), Optional.empty());
        ExplainPlan plan = planWithAllTiersActive().withExecution(noCpu, Map.of(0, noTimings));

        String table = plan.toAsciiTable();
        assertThat(table).contains("actual").doesNotContain("time");
    }

    @Test
    void tierHeaderMarksOffTiers() {
        ExplainPlan disabled = planWithStatsDisabled();
        assertThat(disabled.toAsciiTable()).contains("Tiers:").contains("STATS(off)");
        assertThat(disabled.toJson()).contains("\"tiers\"").contains("STATS").contains("\"off\":true");

        ExplainPlan active = planWithAllTiersActive();
        assertThat(active.toAsciiTable()).contains("Tiers:").doesNotContain("(off)");
        assertThat(active.toJson()).doesNotContain("\"off\":true");
    }

    @Test
    void outputIsAsciiOnly() {
        ExplainPlan plan = planWithAllTiersActive().withExecution(stats(), Map.of(0, rowGroupRead(0)));
        assertAsciiOnly(plan.toAsciiTable());
        assertAsciiOnly(plan.toJson());
        assertAsciiOnly(planWithStatsDisabled().toAsciiTable());
        assertAsciiOnly(planWithStatsDisabled().toJson());
    }

    private static void assertAsciiOnly(String text) {
        for (int i = 0; i < text.length(); i++) {
            assertThat((int) text.charAt(i)).isLessThanOrEqualTo(127);
        }
    }

    private static ExplainPlan planWithAllTiersActive() {
        List<PruningDecision> tiers = List.of(
                new PruningDecision.PassedAll(Tier.STATS, "min/max admit the value"),
                new PruningDecision.NotApplied(Tier.DICTIONARY, "dictionary pruning not wired into reads"));
        return planWithRowGroupTiers(tiers);
    }

    private static ExplainPlan planWithStatsDisabled() {
        List<PruningDecision> tiers =
                List.of(new PruningDecision.NotApplied(Tier.STATS, TierReasons.STATS_FILTER_DISABLED));
        return planWithRowGroupTiers(tiers);
    }

    private static ExplainPlan planWithRowGroupTiers(List<PruningDecision> tiers) {
        RowGroupPlan rowGroup =
                new RowGroupPlan(0, 100, tiers, RowGroupOutcome.FULL, Optional.empty(), Optional.empty());
        Predicate predicate = col("year").eq(2020);
        return new ExplainPlan(
                schema(), schema(), predicate, predicate, List.of(rowGroup), 100, 4096, Optional.empty());
    }

    private static QueryStats stats() {
        return new QueryStats(
                5_000_000L,
                100,
                42,
                Map.of(),
                1,
                1,
                3,
                1,
                new FetchStats(4096, 0, 0, 0, 0, 1),
                Optional.of(new PhaseTimings(5_000_000L, 1_000_000L, 3_000_000L, 500_000L)));
    }

    private static RowGroupRead rowGroupRead(int index) {
        return new RowGroupRead(
                index,
                100,
                17,
                3,
                1,
                new FetchStats(4096, 0, 0, 0, 0, 1),
                Optional.of(new PhaseTimings(5_000_000L, 1_000_000L, 3_000_000L, 500_000L)));
    }

    private static ParquetSchema schema() {
        SchemaNode.Primitive year = new SchemaNode.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(year), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
