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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.explain.ExplainPlan;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class FilterPipelineToggleTest {

    @Test
    void statsFilterDisabledSkipsEvaluatorAndReportsNotApplied() {
        ExplainPlan plan = evaluate(eliminatingInputs(), new FilterPipeline.FilterToggles(false, true));

        assertThat(plan.rowGroups().get(0).outcome()).isNotEqualTo(RowGroupOutcome.ELIMINATED);
        PruningDecision stats = tierDecision(plan, Tier.STATS);
        assertThat(stats).isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(stats.reason()).isEqualTo("stats filter disabled");
    }

    @Test
    void statsFilterEnabledStillEliminates() {
        ExplainPlan plan = evaluate(eliminatingInputs(), new FilterPipeline.FilterToggles(true, true));

        assertThat(plan.rowGroups().get(0).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        PruningDecision stats = tierDecision(plan, Tier.STATS);
        assertThat(stats).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void dictionaryFilterDisabledReportsDisabledReason() {
        ExplainPlan plan = evaluate(survivingInputs(), new FilterPipeline.FilterToggles(true, false));

        PruningDecision dict = tierDecision(plan, Tier.DICTIONARY);
        assertThat(dict).isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(dict.reason()).isEqualTo("dictionary filter disabled");
    }

    @Test
    void dictionaryFilterEnabledButUnwiredReportsHonestReason() {
        ExplainPlan plan = evaluate(survivingInputs(), new FilterPipeline.FilterToggles(true, true));

        PruningDecision dict = tierDecision(plan, Tier.DICTIONARY);
        assertThat(dict).isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(dict.reason()).contains("not wired");
    }

    private static ExplainPlan evaluate(FilterPipeline.RowGroupInputs rg, FilterPipeline.FilterToggles toggles) {
        return FilterPipeline.evaluate(flatSchema(), Projection.ALL, col("year").eq(2030), List.of(rg), toggles);
    }

    private static PruningDecision tierDecision(ExplainPlan plan, Tier tier) {
        return plan.rowGroups().get(0).tiers().stream()
                .filter(decision -> decision.tier() == tier)
                .findFirst()
                .orElseThrow(() -> new AssertionError("no decision for tier " + tier));
    }

    /** Stats prove min/max excludes the predicate value (year in [2010,2015] vs year = 2030): STATS eliminates. */
    private static FilterPipeline.RowGroupInputs eliminatingInputs() {
        return inputs(intStats(2010, 2015));
    }

    /** Stats straddle the predicate value (year in [2010,2030]): STATS cannot eliminate; later tiers run. */
    private static FilterPipeline.RowGroupInputs survivingInputs() {
        return inputs(intStats(2010, 2030));
    }

    private static FilterPipeline.RowGroupInputs inputs(Statistics yearStats) {
        return new FilterPipeline.RowGroupInputs(
                1000,
                statsOf("year", PrimitiveKind.INT32, yearStats),
                FilterPipeline.noDictionaryLookup(),
                noPageIndex());
    }

    private static FilterPipeline.ColumnStatsLookup statsOf(String name, PrimitiveKind kind, Statistics stats) {
        Map<ColumnPath, FilterPipeline.ColumnStats> map = new HashMap<>();
        map.put(ColumnPath.of(name), new FilterPipeline.ColumnStats(kind, stats));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.ColumnPageStatsLookup noPageIndex() {
        return path -> Optional.empty();
    }

    private static Statistics intStats(int min, int max) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(0))
                .maxValue(encodeInt(max))
                .minValue(encodeInt(min))
                .build();
    }

    private static MemorySegment encodeInt(int v) {
        MemorySegment segment = MemorySegment.ofArray(new byte[4]);
        segment.set(INT32, 0, v);
        return segment.asReadOnly();
    }

    private static ParquetSchema flatSchema() {
        SchemaNode.Primitive year = new SchemaNode.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root = new SchemaNode.Group("root", Repetition.REQUIRED, List.of(year), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
