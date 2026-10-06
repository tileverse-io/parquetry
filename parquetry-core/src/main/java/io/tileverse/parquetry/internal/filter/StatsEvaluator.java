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
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;

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
 * tier cannot narrow to a subset of rows.
 */
public final class StatsEvaluator {

    private static final Tier TIER = Tier.STATS;

    private static final String OP_EQ = "Eq ";
    private static final String OP_NOT_EQ = "NotEq ";
    private static final String OP_LT = "Lt ";
    private static final String OP_LT_EQ = "LtEq ";
    private static final String OP_GT = "Gt ";
    private static final String OP_GT_EQ = "GtEq ";
    private static final String OP_IN = "In ";
    private static final String OP_IS_NULL = "IsNull ";
    private static final String OP_IS_NOT_NULL = "IsNotNull ";
    private static final String NAN_CELLS_OUTSIDE_BOUNDS = ": NaN cells lie outside [min, max]";
    private static final String NOTHING_ORDERED_AGAINST_NAN = ": no value is ordered against NaN";
    private static final String NO_NAN_CELL = ": no cell is NaN";
    private static final String EMPTY_LIST = ": no value listed";
    private static final String ONLY_NAN_CELLS = ": the non-null cells are NaN";

    private StatsEvaluator() {}

    /**
     * A typed view of one column's statistics: kind plus already-decoded min/max, null count, and what is known of its
     * NaN cells.
     */
    public record ColumnSummary(
            PrimitiveKind kind, Optional<Value> min, Optional<Value> max, OptionalLong nullCount, NaNCells nans) {
        public ColumnSummary {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(min, "min");
            Objects.requireNonNull(max, "max");
            Objects.requireNonNull(nullCount, "nullCount");
            Objects.requireNonNull(nans, "nans");
        }

        /** A summary telling nothing about NaN cells. */
        public ColumnSummary(PrimitiveKind kind, Optional<Value> min, Optional<Value> max, OptionalLong nullCount) {
            this(kind, min, max, nullCount, NaNCells.POSSIBLE);
        }
    }

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
     * @param rowCount total rows in the row group
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
            case Predicate.Eq(ColumnPath col, Value v) -> evalEq(col, v, columns.get(col), rowCount);
            case Predicate.NotEq(ColumnPath col, Value v) -> evalNotEq(col, v, columns.get(col), rowCount);
            case Predicate.Lt(ColumnPath col, Value v) -> evalLt(col, v, columns.get(col));
            case Predicate.LtEq(ColumnPath col, Value v) -> evalLtEq(col, v, columns.get(col));
            case Predicate.Gt(ColumnPath col, Value v) -> evalGt(col, v, columns.get(col));
            case Predicate.GtEq(ColumnPath col, Value v) -> evalGtEq(col, v, columns.get(col));
            case Predicate.In(ColumnPath col, List<Value> values) -> evalIn(col, values, columns.get(col), rowCount);
            case Predicate.IsNull(ColumnPath col) -> evalIsNull(col, columns.get(col), rowCount);
            case Predicate.IsNotNull(ColumnPath col) -> evalIsNotNull(col, columns.get(col), rowCount);
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
        Optional<Value> decodedMin = decode(kind, logicalType, cs.minValue());
        Optional<Value> decodedMax = decode(kind, logicalType, cs.maxValue());
        NaNCells nans = nanCells(cs, decodedMin, decodedMax);
        Optional<Value> min = usable(decodedMin);
        Optional<Value> max = usable(decodedMax);
        if (inverted(min, max)) {
            return new ColumnSummary(kind, Optional.empty(), Optional.empty(), cs.nullCount(), nans);
        }
        return new ColumnSummary(kind, min, max, cs.nullCount(), nans);
    }

    /**
     * What the NaN count recorded for the chunk tells, checked against its bounds. The count is read for FLOAT and
     * DOUBLE columns alone: the cells of a FLOAT16 column compare as bytes, and a byte literal matches a NaN cell.
     */
    private static NaNCells nanCells(FilterPipeline.ColumnStats cs, Optional<Value> min, Optional<Value> max) {
        if (!NaNCells.comparesAsNumbers(cs.kind())) {
            return NaNCells.POSSIBLE;
        }
        return cs.nans().checkedAgainst(min, max);
    }

    private static boolean inverted(Optional<Value> min, Optional<Value> max) {
        if (min.isEmpty() || max.isEmpty()) {
            return false;
        }
        return ValueComparison.inverted(min.orElseThrow(), max.orElseThrow());
    }

    /** A bound not recorded by the writer, or of a type unknown to the decoder, decodes to nothing. */
    private static Optional<Value> decode(
            PrimitiveKind kind, Optional<LogicalType> logicalType, Optional<MemorySegment> bound) {
        return bound.flatMap(bytes -> StatisticsValueDecoder.decode(kind, logicalType, bytes));
    }

    /** A NaN bound orders no number and leaves its end of the range open. */
    private static Optional<Value> usable(Optional<Value> bound) {
        return bound.filter(value -> !ValueComparison.isNaN(value));
    }

    /**
     * An existential {@code ANY} over a repeated leaf may only eliminate the whole row group: if no element anywhere in
     * the chunk matches the inner comparison then no row matches. It must never report {@code PassedAll}, because a row
     * with an empty or all-null list has zero elements and therefore fails {@code ANY} even when every present element
     * matches; such rows still need record-level confirmation. {@code ALL} and {@code ONE} aggregate across rows and
     * cannot be decided from chunk statistics at all.
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
     * Handles {@code Not} wrappers that survived normalization (only In and the spatial relations do). Both turn into
     * NotApplied at the stats tier.
     */
    private static PruningDecision evaluateNotLeaf(Predicate child, TypedColumns cols, long rowCount) {
        PruningDecision inner = evaluate(child, cols, rowCount);
        return switch (inner) {
            case PruningDecision.Eliminated _ -> new PruningDecision.PassedAll(TIER, "NOT of eliminated child");
            case PruningDecision.PassedAll _ -> new PruningDecision.Eliminated(TIER, "NOT of passed-all child");
            default -> new PruningDecision.NotApplied(TIER, "NOT of inconclusive child");
        };
    }

    private static PruningDecision evalEq(ColumnPath col, Value v, Optional<ColumnSummary> stats, long rowCount) {
        if (ValueComparison.isNaN(v)) {
            return evalEqNaN(OP_EQ, col, stats, rowCount);
        }
        return withNumbers(OP_EQ, col, stats, range -> {
            int cmpMin = ValueComparison.compareValues(v, range.min());
            int cmpMax = ValueComparison.compareValues(v, range.max());
            if (cmpMin < 0 || cmpMax > 0) {
                return new PruningDecision.Eliminated(TIER, OP_EQ + col.dot() + ": value outside [min, max]");
            }
            if (cmpMin == 0 && cmpMax == 0 && rowCount > 0 && canProveAllMatch(range)) {
                return new PruningDecision.PassedAll(TIER, OP_EQ + col.dot() + ": single distinct value matches");
            }
            return new PruningDecision.Inconclusive(TIER, OP_EQ + col.dot() + ": value within [min, max]");
        });
    }

    /**
     * Decides a NaN literal matched by the NaN cells alone, as in {@code Eq} and {@code In}. The bounds tell nothing
     * about those cells; the NaN count of the chunk does.
     */
    private static PruningDecision evalEqNaN(String op, ColumnPath col, Optional<ColumnSummary> stats, long rowCount) {
        return switch (nansOf(stats)) {
            case ABSENT -> new PruningDecision.Eliminated(TIER, op + col.dot() + NO_NAN_CELL);
            case ALL -> onlyNaNCellsMatch(op, col, stats, rowCount);
            case POSSIBLE -> undecidedForNaNLiteral(op, col, stats);
        };
    }

    /**
     * NaN cells lie outside the bounds, and a chunk with possible NaN cells stays undecided for a NaN literal. A chunk
     * without bounds reports the tier as not applied, as for the other comparisons.
     */
    private static PruningDecision undecidedForNaNLiteral(String op, ColumnPath col, Optional<ColumnSummary> stats) {
        Function<DecodedRange, PruningDecision> undecided =
                range -> new PruningDecision.Inconclusive(TIER, op + col.dot() + NAN_CELLS_OUTSIDE_BOUNDS);
        return withRange(col, stats, undecided);
    }

    /** A predicate matched by NaN cells passes a row group holding nothing else: no number, and no null. */
    private static PruningDecision onlyNaNCellsMatch(
            String op, ColumnPath col, Optional<ColumnSummary> stats, long rowCount) {
        OptionalLong nullCount = nullCountOf(stats);
        boolean noNulls = nullCount.isPresent() && nullCount.getAsLong() == 0;
        if (noNulls && rowCount > 0) {
            return new PruningDecision.PassedAll(TIER, op + col.dot() + ONLY_NAN_CELLS + ", no nulls");
        }
        return new PruningDecision.Inconclusive(TIER, op + col.dot() + ONLY_NAN_CELLS);
    }

    private static PruningDecision evalNotEq(ColumnPath col, Value v, Optional<ColumnSummary> stats, long rowCount) {
        if (nansOf(stats) == NaNCells.ALL) {
            return evalNotEqOnOnlyNaN(col, v, stats, rowCount);
        }
        return withRange(col, stats, range -> {
            int cmpMin = ValueComparison.compareValues(v, range.min());
            int cmpMax = ValueComparison.compareValues(v, range.max());
            if ((cmpMin < 0 || cmpMax > 0) && canProveAllMatch(range)) {
                return new PruningDecision.PassedAll(
                        TIER, OP_NOT_EQ + col.dot() + ": value outside [min, max], no nulls");
            }
            if (cmpMin == 0 && cmpMax == 0 && range.nullCount() == 0 && rowCount > 0 && !mayHoldNaN(range)) {
                return new PruningDecision.Eliminated(
                        TIER, OP_NOT_EQ + col.dot() + ": column is single value equal to operand");
            }
            return new PruningDecision.Inconclusive(TIER, OP_NOT_EQ + col.dot() + ": value within [min, max]");
        });
    }

    /**
     * On a chunk holding only NaN, {@code NotEq} with a number matches each non-null cell and {@code NotEq(NaN)}
     * matches none.
     */
    private static PruningDecision evalNotEqOnOnlyNaN(
            ColumnPath col, Value v, Optional<ColumnSummary> stats, long rowCount) {
        if (ValueComparison.isNaN(v)) {
            return new PruningDecision.Eliminated(TIER, OP_NOT_EQ + col.dot() + ONLY_NAN_CELLS);
        }
        return onlyNaNCellsMatch(OP_NOT_EQ, col, stats, rowCount);
    }

    private static PruningDecision evalLt(ColumnPath col, Value v, Optional<ColumnSummary> stats) {
        return withNumbers(OP_LT, col, stats, range -> {
            if (ValueComparison.isNaN(v)) {
                return new PruningDecision.Eliminated(TIER, OP_LT + col.dot() + NOTHING_ORDERED_AGAINST_NAN);
            }
            if (ValueComparison.compareValues(v, range.min()) <= 0) {
                return new PruningDecision.Eliminated(TIER, OP_LT + col.dot() + ": value <= min");
            }
            if (ValueComparison.compareValues(v, range.max()) > 0 && canProveAllMatch(range)) {
                return new PruningDecision.PassedAll(TIER, OP_LT + col.dot() + ": value > max, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_LT + col.dot() + ": value within (min, max]");
        });
    }

    private static PruningDecision evalLtEq(ColumnPath col, Value v, Optional<ColumnSummary> stats) {
        return withNumbers(OP_LT_EQ, col, stats, range -> {
            if (ValueComparison.isNaN(v)) {
                return new PruningDecision.Eliminated(TIER, OP_LT_EQ + col.dot() + NOTHING_ORDERED_AGAINST_NAN);
            }
            if (ValueComparison.compareValues(v, range.min()) < 0) {
                return new PruningDecision.Eliminated(TIER, OP_LT_EQ + col.dot() + ": value < min");
            }
            if (ValueComparison.compareValues(v, range.max()) >= 0 && canProveAllMatch(range)) {
                return new PruningDecision.PassedAll(TIER, OP_LT_EQ + col.dot() + ": value >= max, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_LT_EQ + col.dot() + ": value within [min, max)");
        });
    }

    private static PruningDecision evalGt(ColumnPath col, Value v, Optional<ColumnSummary> stats) {
        return withNumbers(OP_GT, col, stats, range -> {
            if (ValueComparison.isNaN(v)) {
                return new PruningDecision.Eliminated(TIER, OP_GT + col.dot() + NOTHING_ORDERED_AGAINST_NAN);
            }
            if (ValueComparison.compareValues(v, range.max()) >= 0) {
                return new PruningDecision.Eliminated(TIER, OP_GT + col.dot() + ": value >= max");
            }
            if (ValueComparison.compareValues(v, range.min()) < 0 && canProveAllMatch(range)) {
                return new PruningDecision.PassedAll(TIER, OP_GT + col.dot() + ": value < min, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_GT + col.dot() + ": value within [min, max)");
        });
    }

    private static PruningDecision evalGtEq(ColumnPath col, Value v, Optional<ColumnSummary> stats) {
        return withNumbers(OP_GT_EQ, col, stats, range -> {
            if (ValueComparison.isNaN(v)) {
                return new PruningDecision.Eliminated(TIER, OP_GT_EQ + col.dot() + NOTHING_ORDERED_AGAINST_NAN);
            }
            if (ValueComparison.compareValues(v, range.max()) > 0) {
                return new PruningDecision.Eliminated(TIER, OP_GT_EQ + col.dot() + ": value > max");
            }
            if (ValueComparison.compareValues(v, range.min()) <= 0 && canProveAllMatch(range)) {
                return new PruningDecision.PassedAll(TIER, OP_GT_EQ + col.dot() + ": value <= min, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_GT_EQ + col.dot() + ": value within (min, max]");
        });
    }

    /**
     * Decides the NaN literals of the list from what is known of the NaN cells, and its numbers from the bounds. A NaN
     * literal keeps a row group with possible NaN cells regardless of its bounds.
     */
    private static PruningDecision evalIn(
            ColumnPath col, List<Value> values, Optional<ColumnSummary> stats, long rowCount) {
        if (values.isEmpty()) {
            return new PruningDecision.Eliminated(TIER, OP_IN + col.dot() + EMPTY_LIST);
        }
        boolean namesNaN = values.stream().anyMatch(ValueComparison::isNaN);
        if (namesNaN && nansOf(stats) != NaNCells.ABSENT) {
            return evalEqNaN(OP_IN, col, stats, rowCount);
        }
        List<Value> numbers =
                values.stream().filter(value -> !ValueComparison.isNaN(value)).toList();
        if (numbers.isEmpty()) {
            return new PruningDecision.Eliminated(TIER, OP_IN + col.dot() + NO_NAN_CELL);
        }
        return withNumbers(OP_IN, col, stats, range -> {
            for (Value v : numbers) {
                int cmpMin = ValueComparison.compareValues(v, range.min());
                int cmpMax = ValueComparison.compareValues(v, range.max());
                if (cmpMin >= 0 && cmpMax <= 0) {
                    return new PruningDecision.Inconclusive(
                            TIER, OP_IN + col.dot() + ": at least one value within [min, max]");
                }
            }
            return new PruningDecision.Eliminated(TIER, OP_IN + col.dot() + ": no value within [min, max]");
        });
    }

    private static PruningDecision evalIsNull(ColumnPath col, Optional<ColumnSummary> stats, long rowCount) {
        OptionalLong nullCountOpt = nullCountOf(stats);
        if (nullCountOpt.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, OP_IS_NULL + col.dot() + ": nullCount missing");
        }
        long nullCount = nullCountOpt.getAsLong();
        if (nullCount == 0) {
            return new PruningDecision.Eliminated(TIER, OP_IS_NULL + col.dot() + ": no nulls");
        }
        if (nullCount == rowCount) {
            return new PruningDecision.PassedAll(TIER, OP_IS_NULL + col.dot() + ": all rows null");
        }
        return new PruningDecision.Inconclusive(TIER, OP_IS_NULL + col.dot() + ": some nulls");
    }

    private static PruningDecision evalIsNotNull(ColumnPath col, Optional<ColumnSummary> stats, long rowCount) {
        OptionalLong nullCountOpt = nullCountOf(stats);
        if (nullCountOpt.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, OP_IS_NOT_NULL + col.dot() + ": nullCount missing");
        }
        long nullCount = nullCountOpt.getAsLong();
        if (nullCount == rowCount) {
            return new PruningDecision.Eliminated(TIER, OP_IS_NOT_NULL + col.dot() + ": all rows null");
        }
        if (nullCount == 0) {
            return new PruningDecision.PassedAll(TIER, OP_IS_NOT_NULL + col.dot() + ": no nulls");
        }
        return new PruningDecision.Inconclusive(TIER, OP_IS_NOT_NULL + col.dot() + ": some nulls");
    }

    /**
     * A min/max comparison can prove all-match only when the bounds cover the cells of a column without nulls. Binary
     * bounds may be truncated, and FLOAT/DOUBLE bounds leave the NaN cells out: only a column known to hold no NaN is
     * covered. INT96 has no stats path.
     */
    private static boolean canProveAllMatch(DecodedRange range) {
        if (range.nullCount() != 0) {
            return false;
        }
        return switch (range.kind()) {
            case BOOLEAN, INT32, INT64 -> true;
            case FLOAT, DOUBLE -> range.nans() == NaNCells.ABSENT;
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> false;
        };
    }

    /** A FLOAT or DOUBLE chunk may hold NaN cells outside its min/max, unless its statistics rule them out. */
    private static boolean mayHoldNaN(DecodedRange range) {
        return NaNCells.comparesAsNumbers(range.kind()) && range.nans() != NaNCells.ABSENT;
    }

    /** What is known of the NaN cells of a column; nothing for a column without statistics. */
    private static NaNCells nansOf(Optional<ColumnSummary> stats) {
        return stats.map(ColumnSummary::nans).orElse(NaNCells.POSSIBLE);
    }

    private static OptionalLong nullCountOf(Optional<ColumnSummary> stats) {
        return stats.map(ColumnSummary::nullCount).orElseGet(OptionalLong::empty);
    }

    /**
     * Decides a comparison matched by numbers alone: a column holding nothing but NaN cells has no number to match, and
     * any other column is decided from its range as {@link #withRange} does.
     */
    private static PruningDecision withNumbers(
            String op, ColumnPath col, Optional<ColumnSummary> stats, Function<DecodedRange, PruningDecision> f) {
        if (nansOf(stats) == NaNCells.ALL) {
            return new PruningDecision.Eliminated(TIER, op + col.dot() + ONLY_NAN_CELLS);
        }
        return withRange(col, stats, f);
    }

    /**
     * Looks up the column's min/max range and dispatches to the supplied evaluator. Returns NotApplied if the stats
     * lack the data we need (no entry, or no usable decoded min/max value).
     */
    private static PruningDecision withRange(
            ColumnPath col, Optional<ColumnSummary> stats, Function<DecodedRange, PruningDecision> f) {
        if (stats.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "no stats for " + col.dot());
        }
        ColumnSummary cs = stats.get();
        Optional<Value> min = usable(cs.min());
        Optional<Value> max = usable(cs.max());
        if (min.isEmpty() || max.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "min/max missing for " + col.dot());
        }
        long nullCount = cs.nullCount().orElse(-1L);
        return f.apply(new DecodedRange(cs.kind(), min.orElseThrow(), max.orElseThrow(), nullCount, cs.nans()));
    }

    /**
     * Aggregate of a column's decoded min/max, its null count (or {@code -1} if absent) and what is known of its NaN
     * cells.
     */
    private record DecodedRange(PrimitiveKind kind, Value min, Value max, long nullCount, NaNCells nans) {}
}
