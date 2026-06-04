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

import java.util.List;

import io.tileverse.parquetry.filter.Predicate;
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
 * @param estimatedBytesRead 0 in the v1 explain (no per-page byte tracking yet)
 */
public record ExplainPlan(
        ParquetSchema fileSchema,
        ParquetSchema projectedSchema,
        Predicate originalPredicate,
        Predicate normalizedPredicate,
        List<RowGroupPlan> rowGroups,
        long estimatedRowsScanned,
        long estimatedBytesRead) {

    public ExplainPlan {
        rowGroups = List.copyOf(rowGroups);
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
