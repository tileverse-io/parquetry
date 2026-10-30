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

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.MatchAction;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Tier-1 (STATS) evaluator. Inspects a row group's per-column statistics (min, max, null count, NaN count) to decide
 * whether the row group can be eliminated, kept whole, or left for downstream tiers to refine.
 *
 * <p>The evaluator assumes the predicate has already been run through {@code PredicateNormalizer}: {@code Not} is
 * pushed to leaves, {@code Always} is folded, and nested {@code And}/{@code Or} are flattened. It only ever returns
 * {@link PruningDecision.Eliminated}, {@link PruningDecision.PassedAll}, {@link PruningDecision.Inconclusive} (the
 * statistics leave some rows undecided), or {@link PruningDecision.NotApplied} (no statistics to consult) - the stats
 * tier cannot narrow to a subset of rows. Each leaf is judged by {@link LeafVerdicts}, with the rules of the page tier.
 */
public final class StatsEvaluator {

    private static final Tier TIER = Tier.STATS;

    private StatsEvaluator() {}

    /** Name-keyed lookup of typed per-column statistics. */
    @FunctionalInterface
    public interface TypedColumns {
        Optional<ColumnSummary> get(ColumnPath path);
    }

    /**
     * Evaluates {@code predicate} against already-decoded typed column statistics.
     *
     * @param predicate the normalized predicate
     * @param columns lookup from column path to its typed stats (kind, decoded min/max, null count, NaN cells)
     * @param rowCount total rows in the row group; a row group without rows is never proven to match each row
     */
    public static PruningDecision evaluate(Predicate predicate, TypedColumns columns, long rowCount) {
        return switch (predicate) {
            case Predicate.Always(boolean value) ->
                value
                        ? new PruningDecision.PassedAll(TIER, "predicate is ALWAYS_TRUE")
                        : new PruningDecision.Eliminated(TIER, "predicate is ALWAYS_FALSE");
            case Predicate.And(List<Predicate> children) -> evaluateAnd(children, columns, rowCount);
            case Predicate.Or(List<Predicate> children) -> evaluateOr(children, columns, rowCount);
            case Predicate.Not(Predicate child) -> evaluateNotLeaf(child, columns, rowCount);
            case Predicate.Eq(ColumnPath col, Value _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.NotEq(ColumnPath col, Value _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.Lt(ColumnPath col, Value _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.LtEq(ColumnPath col, Value _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.Gt(ColumnPath col, Value _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.GtEq(ColumnPath col, Value _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.In(ColumnPath col, List<Value> _) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.IsNull(ColumnPath col) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.IsNotNull(ColumnPath col) -> evaluateLeaf(predicate, col, columns, rowCount);
            case Predicate.Spatial _ ->
                new PruningDecision.NotApplied(TIER, "spatial predicate handled by the bounds source");
            case Predicate.GeometryFilterPredicate _ ->
                new PruningDecision.NotApplied(TIER, "GeometryFilter not handled at STATS tier");
            case Predicate.RowIndexExcluded _ ->
                new PruningDecision.NotApplied(TIER, "row-position deletes not handled at STATS tier");
            case Predicate.Quantified(MatchAction match, Predicate leaf) ->
                evaluateQuantified(match, leaf, columns, rowCount);
        };
    }

    /**
     * Evaluates {@code predicate} against the given row group's column statistics, decoding each column's PLAIN min/max
     * bytes before delegating to the typed evaluator.
     *
     * @param predicate the normalized predicate
     * @param columns lookup from column path to its stats (kind + PLAIN-encoded bounds + null count)
     * @param rowCount total rows in the row group
     */
    public static PruningDecision evaluate(
            Predicate predicate, FilterPipeline.ColumnStatsLookup columns, long rowCount) {
        return evaluate(predicate, decoding(columns), rowCount);
    }

    private static TypedColumns decoding(FilterPipeline.ColumnStatsLookup columns) {
        return path -> columns.get(path).map(StatsEvaluator::summarize);
    }

    /**
     * Decodes one column's PLAIN-encoded min/max statistic bytes into a typed {@link ColumnSummary}. A minimum above
     * its maximum leaves both out; see {@link ValueComparison#inverted}.
     */
    public static ColumnSummary summarize(FilterPipeline.ColumnStats cs) {
        PrimitiveKind kind = cs.kind();
        Optional<LogicalType> logicalType = cs.logicalType();
        Optional<Value> min = decode(kind, logicalType, cs.minValue());
        Optional<Value> max = decode(kind, logicalType, cs.maxValue());
        OptionalLong valueCount = OptionalLong.of(cs.valueCount());
        return ColumnSummary.of(kind, min, max, cs.nullCount(), valueCount, cs.nans());
    }

    /** A bound not recorded by the writer, or of a type unknown to the decoder, decodes to nothing. */
    private static Optional<Value> decode(
            PrimitiveKind kind, Optional<LogicalType> logicalType, Optional<MemorySegment> bound) {
        return bound.flatMap(bytes -> StatisticsValueDecoder.decode(kind, logicalType, bytes));
    }

    /**
     * Decides a comparison, a set membership or a null test from the statistics of its column. A verdict proving each
     * row to match needs a row group with rows. Callers count the rows of such a row group without reading them.
     */
    private static PruningDecision evaluateLeaf(Predicate leaf, ColumnPath col, TypedColumns columns, long rowCount) {
        Optional<ColumnSummary> unit = columns.get(col);
        LeafVerdict verdict =
                unit.isPresent() ? LeafVerdicts.of(leaf, unit.orElseThrow()) : LeafVerdicts.withoutStatistics(leaf);
        String reason = leaf.getClass().getSimpleName() + " " + col.dot() + ": " + verdict.reason();
        return switch (verdict.matches()) {
            case NONE -> new PruningDecision.Eliminated(TIER, reason);
            case ALL ->
                rowCount > 0
                        ? new PruningDecision.PassedAll(TIER, reason)
                        : new PruningDecision.Inconclusive(TIER, reason);
            case SOME -> new PruningDecision.Inconclusive(TIER, reason);
            case UNKNOWN -> new PruningDecision.NotApplied(TIER, reason);
        };
    }

    /**
     * An existential {@code ANY} over a repeated leaf may only eliminate the whole row group: if no element anywhere in
     * the chunk matches the inner comparison then no row matches. It must never report {@code PassedAll}: a row with an
     * empty list has no element and fails {@code ANY}, and a null element matches no comparison. Such rows still need
     * record-level confirmation. {@code ALL} and {@code ONE} aggregate across rows and cannot be decided from chunk
     * statistics at all.
     */
    private static PruningDecision evaluateQuantified(
            MatchAction match, Predicate leaf, TypedColumns columns, long rowCount) {
        if (match != MatchAction.ANY) {
            return new PruningDecision.NotApplied(TIER, match + " over a repeated leaf is not pruned by stats");
        }
        PruningDecision inner = evaluate(leaf, columns, rowCount);
        if (inner instanceof PruningDecision.Eliminated) {
            return inner;
        }
        return new PruningDecision.NotApplied(
                TIER,
                "ANY over a repeated leaf: row-group elimination only; per-row lists need record-level confirmation");
    }

    private static PruningDecision evaluateAnd(List<Predicate> children, TypedColumns cols, long rowCount) {
        boolean allPassed = true;
        for (Predicate child : children) {
            PruningDecision d = evaluate(child, cols, rowCount);
            if (d instanceof PruningDecision.Eliminated) {
                return new PruningDecision.Eliminated(TIER, "AND child eliminated: " + d.reason());
            }
            if (!(d instanceof PruningDecision.PassedAll)) {
                allPassed = false;
            }
        }
        return allPassed
                ? new PruningDecision.PassedAll(TIER, "all AND children passed")
                : new PruningDecision.NotApplied(TIER, "AND children mixed");
    }

    private static PruningDecision evaluateOr(List<Predicate> children, TypedColumns cols, long rowCount) {
        boolean anyPassed = false;
        boolean allEliminated = true;
        for (Predicate child : children) {
            PruningDecision d = evaluate(child, cols, rowCount);
            if (d instanceof PruningDecision.PassedAll) {
                anyPassed = true;
            }
            if (!(d instanceof PruningDecision.Eliminated)) {
                allEliminated = false;
            }
        }
        if (allEliminated) {
            return new PruningDecision.Eliminated(TIER, "all OR children eliminated");
        }
        return anyPassed
                ? new PruningDecision.PassedAll(TIER, "an OR child passed")
                : new PruningDecision.NotApplied(TIER, "OR children inconclusive");
    }

    /**
     * Complements the decision of a negation left by normalization: a negated spatial relation, geometry filter,
     * row-position exclusion or quantifier, and a negated IN without values or under a quantifier. A child decided for
     * the whole row group is decided the other way, and an undecided child stays undecided.
     */
    private static PruningDecision evaluateNotLeaf(Predicate child, TypedColumns cols, long rowCount) {
        PruningDecision inner = evaluate(child, cols, rowCount);
        return switch (inner) {
            case PruningDecision.Eliminated _ -> new PruningDecision.PassedAll(TIER, "NOT of eliminated child");
            case PruningDecision.PassedAll _ -> new PruningDecision.Eliminated(TIER, "NOT of passed-all child");
            default -> new PruningDecision.NotApplied(TIER, "NOT of inconclusive child");
        };
    }
}
