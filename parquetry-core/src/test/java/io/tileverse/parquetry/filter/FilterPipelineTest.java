/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

class FilterPipelineTest {

    @Test
    void statsElimSkipsLaterTiers() {
        ParquetSchema schema = flatSchema();
        FilterPipeline.RowGroupInputs rg = inputs(
                /* rows */ 1000,
                statsOf("year", PrimitiveKind.INT32, intStats(2010, 2015, 0)),
                noDicts(),
                noPageIndex());
        Predicate p = col("year").eq(2030);

        ExplainPlan plan = FilterPipeline.evaluate(schema, Projection.ALL, p, List.of(rg));

        assertThat(plan.rowGroups()).hasSize(1);
        assertThat(plan.rowGroups().get(0).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertThat(plan.rowGroups().get(0).tiers()).hasSize(1); // stats short-circuited
        assertThat(plan.estimatedRowsScanned()).isZero();
    }

    @Test
    void fullPassageWhenAllTiersPass() {
        ParquetSchema schema = flatSchema();
        FilterPipeline.RowGroupInputs rg = inputs(
                /* rows */ 1000,
                statsOf("year", PrimitiveKind.INT32, intStats(2010, 2030, 0)),
                noDicts(),
                noPageIndex());
        Predicate p = col("year").lt(2050);

        ExplainPlan plan = FilterPipeline.evaluate(schema, Projection.ALL, p, List.of(rg));

        assertThat(plan.rowGroups().get(0).outcome()).isEqualTo(RowGroupOutcome.FULL);
        assertThat(plan.estimatedRowsScanned()).isEqualTo(1000);
    }

    @Test
    void allRowGroupsEvaluatedAndAggregated() {
        ParquetSchema schema = flatSchema();
        List<FilterPipeline.RowGroupInputs> rgs = List.of(
                inputs(1000, statsOf("year", PrimitiveKind.INT32, intStats(2010, 2015, 0)), noDicts(), noPageIndex()),
                inputs(1000, statsOf("year", PrimitiveKind.INT32, intStats(2018, 2022, 0)), noDicts(), noPageIndex()),
                inputs(1000, statsOf("year", PrimitiveKind.INT32, intStats(2025, 2030, 0)), noDicts(), noPageIndex()));
        Predicate p = col("year").eq(2020);

        ExplainPlan plan = FilterPipeline.evaluate(schema, Projection.ALL, p, rgs);

        assertThat(plan.rowGroups().get(0).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertThat(plan.rowGroups().get(1).outcome()).isIn(RowGroupOutcome.PARTIAL, RowGroupOutcome.FULL);
        assertThat(plan.rowGroups().get(2).outcome()).isEqualTo(RowGroupOutcome.ELIMINATED);
        assertThat(plan.estimatedRowsScanned()).isEqualTo(1000);
    }

    @Test
    void asciiTableRendersHeaderAndOneRowPerRowGroup() {
        ParquetSchema schema = flatSchema();
        FilterPipeline.RowGroupInputs rg =
                inputs(100, statsOf("year", PrimitiveKind.INT32, intStats(2010, 2015, 0)), noDicts(), noPageIndex());
        ExplainPlan plan =
                FilterPipeline.evaluate(schema, Projection.ALL, col("year").eq(2030), List.of(rg));
        String table = plan.toAsciiTable();
        assertThat(table).contains("RG").contains("Stats").contains("Outcome").contains("skip");
    }

    @Test
    void jsonRenderIncludesRowGroupsAndDecisions() {
        ParquetSchema schema = flatSchema();
        FilterPipeline.RowGroupInputs rg =
                inputs(100, statsOf("year", PrimitiveKind.INT32, intStats(2010, 2015, 0)), noDicts(), noPageIndex());
        ExplainPlan plan =
                FilterPipeline.evaluate(schema, Projection.ALL, col("year").eq(2030), List.of(rg));
        String json = plan.toJson();
        assertThat(json)
                .startsWith("{")
                .endsWith("}")
                .contains("\"rowGroups\"")
                .contains("\"outcome\":\"ELIMINATED\"")
                .contains("\"tier\":\"STATS\"");
    }

    private static FilterPipeline.RowGroupInputs inputs(
            long rows,
            FilterPipeline.ColumnStatsLookup stats,
            FilterPipeline.DictionaryLookup dicts,
            FilterPipeline.ColumnPageStatsLookup pageIndexes) {
        return new FilterPipeline.RowGroupInputs(rows, stats, dicts, pageIndexes);
    }

    private static FilterPipeline.ColumnStatsLookup statsOf(String name, PrimitiveKind kind, Statistics stats) {
        Map<ColumnPath, FilterPipeline.ColumnStats> map = new HashMap<>();
        map.put(ColumnPath.of(name), new FilterPipeline.ColumnStats(kind, stats));
        return path -> Optional.ofNullable(map.get(path));
    }

    private static FilterPipeline.DictionaryLookup noDicts() {
        return path -> Optional.empty();
    }

    private static FilterPipeline.ColumnPageStatsLookup noPageIndex() {
        return path -> Optional.empty();
    }

    private static Statistics intStats(int min, int max, long nullCount) {
        return Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .maxValue(Optional.of(encodeInt(max)))
                .minValue(Optional.of(encodeInt(min)))
                .build();
    }

    private static ByteBuffer encodeInt(int v) {
        return (ByteBuffer)
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(v).flip();
    }

    private static ParquetSchema flatSchema() {
        Field.Primitive year = new Field.Primitive(
                "year", Repetition.OPTIONAL, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
        Field.Group root = new Field.Group("root", Repetition.REQUIRED, List.of(year), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
