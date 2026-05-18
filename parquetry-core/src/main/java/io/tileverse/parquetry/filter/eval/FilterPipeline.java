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
package io.tileverse.parquetry.filter.eval;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.ExplainPlan;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.PredicateNormalizer;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.PruningDecision;
import io.tileverse.parquetry.filter.RowGroupOutcome;
import io.tileverse.parquetry.filter.RowGroupPlan;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.schema.Schema;

/**
 * The a-priori filter pipeline: given a normalized predicate and per-row-group lookups for stats, dictionaries, and
 * page indexes, produces an {@link ExplainPlan} describing what each tier decided. No actual column data is read; the
 * pipeline only consumes metadata that callers have already loaded.
 */
public final class FilterPipeline {

    private FilterPipeline() {}

    /**
     * Per-row-group inputs the pipeline needs to evaluate the a-priori tiers. Any lookup may return empty entries for a
     * given column, in which case the corresponding tier degrades to {@link PruningDecision.NotApplied}.
     */
    public record RowGroupInputs(
            long rowCount, ColumnStatsLookup stats, DictionaryLookup dictionaries, ColumnPageStatsLookup pageIndexes) {}

    /**
     * Runs the pipeline.
     *
     * @param fileSchema the file's full schema (used for predicate validation and the explain plan)
     * @param projection projected columns (used only to record the projected schema in the plan)
     * @param originalPredicate the predicate as the caller wrote it; will be normalized before evaluation
     * @param rowGroups one {@link RowGroupInputs} per row group in the file, in file order
     */
    public static ExplainPlan evaluate(
            Schema fileSchema, Projection projection, Predicate originalPredicate, List<RowGroupInputs> rowGroups) {
        Predicate normalized = PredicateNormalizer.normalizeAndValidate(originalPredicate, fileSchema);
        Schema projectedSchema = projectionOf(fileSchema, projection);
        List<RowGroupPlan> plans = new ArrayList<>(rowGroups.size());
        long rowsScanned = 0;
        for (int i = 0; i < rowGroups.size(); i++) {
            RowGroupPlan plan = evaluateRowGroup(i, rowGroups.get(i), normalized);
            plans.add(plan);
            rowsScanned += survivingRowsForOutcome(plan);
        }
        return new ExplainPlan(fileSchema, projectedSchema, originalPredicate, normalized, plans, rowsScanned, 0L);
    }

    private static RowGroupPlan evaluateRowGroup(int index, RowGroupInputs inputs, Predicate normalized) {
        List<PruningDecision> tierDecisions = new ArrayList<>(4);
        Optional<RowRanges> surviving = Optional.empty();

        PruningDecision statsDecision = StatsEvaluator.evaluate(normalized, inputs.stats(), inputs.rowCount());
        tierDecisions.add(statsDecision);
        if (statsDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }

        PruningDecision dictDecision = DictionaryEvaluator.evaluate(normalized, inputs.dictionaries());
        tierDecisions.add(dictDecision);
        if (dictDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }

        PruningDecision indexDecision =
                ColumnIndexEvaluator.evaluate(normalized, inputs.pageIndexes(), inputs.rowCount());
        tierDecisions.add(indexDecision);
        if (indexDecision instanceof PruningDecision.Eliminated) {
            return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, RowGroupOutcome.ELIMINATED, surviving);
        }
        if (indexDecision instanceof PruningDecision.NarrowedTo n) {
            surviving = Optional.of(n.ranges());
        }

        // Bloom filter tier deferred to a future release. Record a NotApplied entry so consumers can see all 4 a-priori
        // tiers.
        tierDecisions.add(new PruningDecision.NotApplied(
                io.tileverse.parquetry.filter.Tier.BLOOM_FILTER, "deferred to a future release"));

        RowGroupOutcome outcome = surviving.isPresent() ? RowGroupOutcome.PARTIAL : RowGroupOutcome.FULL;
        return new RowGroupPlan(index, inputs.rowCount(), tierDecisions, outcome, surviving);
    }

    private static long survivingRowsForOutcome(RowGroupPlan plan) {
        return switch (plan.outcome()) {
            case ELIMINATED -> 0L;
            case PARTIAL -> plan.survivingRows().map(RowRanges::totalRows).orElse(plan.rowCount());
            case FULL -> plan.rowCount();
        };
    }

    private static Schema projectionOf(Schema fileSchema, Projection projection) {
        return switch (projection) {
            case Projection.All _ -> fileSchema;
            case Projection.Columns(var kept) -> fileSchema.project(kept);
        };
    }

    /** Defensive copy of an empty list as the no-row-groups input. */
    @SuppressWarnings("unused")
    public static List<RowGroupInputs> empty() {
        return Collections.emptyList();
    }
}
