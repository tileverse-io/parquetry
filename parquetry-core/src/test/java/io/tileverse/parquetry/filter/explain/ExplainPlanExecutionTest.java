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
package io.tileverse.parquetry.filter.explain;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.observe.FetchStats;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.observe.SpillStats;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

class ExplainPlanExecutionTest {

    private static final ParquetSchema SCHEMA = new ParquetSchema(new SchemaNode.Group(
            "root",
            Repetition.REQUIRED,
            List.of(new SchemaNode.Primitive(
                    "id", Repetition.OPTIONAL, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1)),
            Optional.empty(),
            -1));

    @Test
    void plainPlanHasNoExecution() {
        ExplainPlan plan = planWithRowGroups(rowGroup(0), rowGroup(1));

        assertThat(plan.execution()).isEmpty();
        assertThat(plan.rowGroups())
                .allSatisfy(group -> assertThat(group.execution()).isEmpty());
    }

    @Test
    void withExecutionAttachesStatsAndPerRowGroupReads() {
        ExplainPlan plan = planWithRowGroups(rowGroup(0), rowGroup(1));
        QueryStats stats = stats();
        RowGroupRead readForGroupZero = rowGroupRead(0);
        Map<Integer, RowGroupRead> perRowGroup = Map.of(0, readForGroupZero);

        ExplainPlan annotated = plan.withExecution(stats, perRowGroup);

        assertThat(annotated.execution()).contains(stats);
        assertThat(annotated.rowGroups().get(0).execution()).contains(readForGroupZero);
        assertThat(annotated.rowGroups().get(1).execution()).isEmpty();
    }

    @Test
    void withExecutionDoesNotMutateOriginal() {
        ExplainPlan plan = planWithRowGroups(rowGroup(0));

        plan.withExecution(stats(), Map.of(0, rowGroupRead(0)));

        assertThat(plan.execution()).isEmpty();
        assertThat(plan.rowGroups().get(0).execution()).isEmpty();
    }

    private static ExplainPlan planWithRowGroups(RowGroupPlan... rowGroups) {
        return new ExplainPlan(
                SCHEMA,
                SCHEMA,
                Predicate.ALWAYS_TRUE,
                Predicate.ALWAYS_TRUE,
                List.of(rowGroups),
                0L,
                0L,
                Optional.empty());
    }

    private static RowGroupPlan rowGroup(int index) {
        return new RowGroupPlan(index, 100L, List.of(), RowGroupOutcome.FULL, Optional.empty(), Optional.empty());
    }

    private static QueryStats stats() {
        return new QueryStats(
                123L, 100L, 10L, Map.of(), 1, 2, 5L, 3L, FetchStats.EMPTY, SpillStats.EMPTY, Optional.empty());
    }

    private static RowGroupRead rowGroupRead(int index) {
        return new RowGroupRead(index, 100L, 10L, 5, 3, FetchStats.EMPTY, Optional.empty());
    }
}
