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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.observe.QueryStats;
import io.tileverse.parquetry.observe.RowGroupRead;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The result of running {@code FilterPipeline} without actually reading column data: which row groups were eliminated,
 * narrowed, or kept whole, and what each pruning tier decided. Renderable as a compact ASCII table or as JSON for
 * tooling.
 *
 * @param fileSchema the file's full schema
 * @param projectedSchema the schema after projection (subset of {@code fileSchema})
 * @param originalPredicate the predicate as the caller wrote it
 * @param normalizedPredicate the predicate after structural normalization
 * @param rowGroups one {@link RowGroupPlan} per row group in the file
 * @param estimatedRowsScanned sum of surviving rows across all row groups
 * @param estimatedBytesRead sum, over the non-eliminated row groups, of the total compressed size of the projected leaf
 *     column chunks; an upper bound on the bytes a read would fetch (whole-chunk granularity, no page-level refinement)
 * @param execution the whole-query execution stats when this plan was annotated with what a read actually did; empty
 *     for a plain explain that never read column data
 */
public record ExplainPlan(
        ParquetSchema fileSchema,
        ParquetSchema projectedSchema,
        Predicate originalPredicate,
        Predicate normalizedPredicate,
        List<RowGroupPlan> rowGroups,
        long estimatedRowsScanned,
        long estimatedBytesRead,
        Optional<QueryStats> execution) {

    public ExplainPlan {
        rowGroups = List.copyOf(rowGroups);
        Objects.requireNonNull(execution, "execution");
    }

    /**
     * Returns a copy of this plan annotated with execution results: the whole-query {@code stats} and, on each row
     * group, the {@link RowGroupRead} keyed by its {@link RowGroupPlan#index()} in {@code perRowGroup}. A row group
     * with no matching entry keeps an empty execution. The original plan is left unchanged.
     */
    public ExplainPlan withExecution(QueryStats stats, Map<Integer, RowGroupRead> perRowGroup) {
        List<RowGroupPlan> annotated = new ArrayList<>(rowGroups.size());
        for (RowGroupPlan rowGroup : rowGroups) {
            Optional<RowGroupRead> read = Optional.ofNullable(perRowGroup.get(rowGroup.index()));
            annotated.add(rowGroup.withExecution(read));
        }
        return new ExplainPlan(
                fileSchema,
                projectedSchema,
                originalPredicate,
                normalizedPredicate,
                annotated,
                estimatedRowsScanned,
                estimatedBytesRead,
                Optional.of(stats));
    }

    /**
     * Renders the plan as a compact ASCII table. Columns: row group index, row count, one column per tier with "passed"
     * / "ELIM" / "NARROW n/m" / "-", and the final outcome.
     */
    public String toAsciiTable() {
        AsciiTableRenderer renderer = new AsciiTableRenderer(this);
        return renderer.render();
    }

    /**
     * Renders the plan as a single JSON object. Hand-rolled emitter to avoid pulling Jackson into the core module's
     * runtime classpath solely for ExplainPlan.
     */
    public String toJson() {
        return JsonRenderer.render(this);
    }
}
