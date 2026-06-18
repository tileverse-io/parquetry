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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

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
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Tier-1 (STATS) evaluator. Inspects a row group's per-column {@link Statistics} (min, max, nullCount) to decide
 * whether the row group can be eliminated, kept whole, or left for downstream tiers to refine.
 *
 * <p>The evaluator assumes the predicate has already been run through {@code PredicateNormalizer}: {@code Not} is
 * pushed to leaves, {@code Always} is folded, and nested {@code And}/{@code Or} are flattened. It only ever returns
 * {@link PruningDecision.Eliminated}, {@link PruningDecision.PassedAll}, {@link PruningDecision.Inconclusive} (the
 * value fell within the column's min/max), or {@link PruningDecision.NotApplied} (no statistics to consult) - the stats
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

    private StatsEvaluator() {}

    /** A typed view of one column's statistics: kind plus already-decoded min/max and null count. */
    public record ColumnSummary(PrimitiveKind kind, Optional<Value> min, Optional<Value> max, OptionalLong nullCount) {
        public ColumnSummary {
            Objects.requireNonNull(kind, "kind");
            Objects.requireNonNull(min, "min");
            Objects.requireNonNull(max, "max");
            Objects.requireNonNull(nullCount, "nullCount");
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
     * @param columns lookup from column path to its typed stats (kind + decoded min/max + nullCount)
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
            case Predicate.Eq(ColumnPath col, Value v) -> evalEq(col, v, columns, rowCount);
            case Predicate.NotEq(ColumnPath col, Value v) -> evalNotEq(col, v, columns, rowCount);
            case Predicate.Lt(ColumnPath col, Value v) -> evalLt(col, v, columns);
            case Predicate.LtEq(ColumnPath col, Value v) -> evalLtEq(col, v, columns);
            case Predicate.Gt(ColumnPath col, Value v) -> evalGt(col, v, columns);
            case Predicate.GtEq(ColumnPath col, Value v) -> evalGtEq(col, v, columns);
            case Predicate.In(ColumnPath col, List<Value> values) -> evalIn(col, values, columns);
            case Predicate.IsNull(ColumnPath col) -> evalIsNull(col, columns, rowCount);
            case Predicate.IsNotNull(ColumnPath col) -> evalIsNotNull(col, columns, rowCount);
            case Predicate.Spatial _ ->
                new PruningDecision.NotApplied(TIER, "spatial predicate handled by the bounds source");
            case Predicate.GeometryFilterPredicate _ ->
                new PruningDecision.NotApplied(TIER, "GeometryFilter not handled at STATS tier");
            case Predicate.Quantified(MatchAction match, Predicate leaf) ->
                evaluateQuantified(match, leaf, columns, rowCount);
        };
    }

    /**
     * Evaluates {@code predicate} against the given row group's column statistics, decoding each column's PLAIN min/max
     * bytes before delegating to the typed evaluator.
     *
     * @param predicate the normalized predicate
     * @param columns lookup from column path to its stats (kind + Statistics)
     * @param rowCount total rows in the row group
     */
    public static PruningDecision evaluate(
            Predicate predicate, FilterPipeline.ColumnStatsLookup columns, long rowCount) {
        return evaluate(predicate, decoding(columns), rowCount);
    }

    private static TypedColumns decoding(FilterPipeline.ColumnStatsLookup columns) {
        return path -> columns.get(path).map(StatsEvaluator::summarize);
    }

    private static ColumnSummary summarize(FilterPipeline.ColumnStats cs) {
        PrimitiveKind kind = cs.kind();
        MemorySegment minBytes =
                preferLatest(cs.statistics().minValue(), cs.statistics().min());
        MemorySegment maxBytes =
                preferLatest(cs.statistics().maxValue(), cs.statistics().max());
        Optional<Value> min = minBytes == MemorySegment.NULL ? Optional.empty() : decode(kind, minBytes);
        Optional<Value> max = maxBytes == MemorySegment.NULL ? Optional.empty() : decode(kind, maxBytes);
        return new ColumnSummary(kind, min, max, cs.statistics().nullCount());
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

    private static PruningDecision evalEq(ColumnPath col, Value v, TypedColumns cols, long rowCount) {
        return withRange(col, cols, range -> {
            int cmpMin = ValueComparison.compareValues(v, range.min());
            int cmpMax = ValueComparison.compareValues(v, range.max());
            if (cmpMin < 0 || cmpMax > 0) {
                return new PruningDecision.Eliminated(TIER, OP_EQ + col.dot() + ": value outside [min, max]");
            }
            if (cmpMin == 0 && cmpMax == 0 && rowCount > 0 && canProveAllMatch(range.kind(), range.nullCount())) {
                return new PruningDecision.PassedAll(TIER, OP_EQ + col.dot() + ": single distinct value matches");
            }
            return new PruningDecision.Inconclusive(TIER, OP_EQ + col.dot() + ": value within [min, max]");
        });
    }

    private static PruningDecision evalNotEq(ColumnPath col, Value v, TypedColumns cols, long rowCount) {
        return withRange(col, cols, range -> {
            int cmpMin = ValueComparison.compareValues(v, range.min());
            int cmpMax = ValueComparison.compareValues(v, range.max());
            if ((cmpMin < 0 || cmpMax > 0) && canProveAllMatch(range.kind(), range.nullCount())) {
                return new PruningDecision.PassedAll(
                        TIER, OP_NOT_EQ + col.dot() + ": value outside [min, max], no nulls");
            }
            if (cmpMin == 0 && cmpMax == 0 && range.nullCount() == 0 && rowCount > 0) {
                return new PruningDecision.Eliminated(
                        TIER, OP_NOT_EQ + col.dot() + ": column is single value equal to operand");
            }
            return new PruningDecision.Inconclusive(TIER, OP_NOT_EQ + col.dot() + ": value within [min, max]");
        });
    }

    private static PruningDecision evalLt(ColumnPath col, Value v, TypedColumns cols) {
        return withRange(col, cols, range -> {
            if (ValueComparison.compareValues(v, range.min()) <= 0) {
                return new PruningDecision.Eliminated(TIER, OP_LT + col.dot() + ": value <= min");
            }
            if (ValueComparison.compareValues(v, range.max()) > 0
                    && canProveAllMatch(range.kind(), range.nullCount())) {
                return new PruningDecision.PassedAll(TIER, OP_LT + col.dot() + ": value > max, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_LT + col.dot() + ": value within (min, max]");
        });
    }

    private static PruningDecision evalLtEq(ColumnPath col, Value v, TypedColumns cols) {
        return withRange(col, cols, range -> {
            if (ValueComparison.compareValues(v, range.min()) < 0) {
                return new PruningDecision.Eliminated(TIER, OP_LT_EQ + col.dot() + ": value < min");
            }
            if (ValueComparison.compareValues(v, range.max()) >= 0
                    && canProveAllMatch(range.kind(), range.nullCount())) {
                return new PruningDecision.PassedAll(TIER, OP_LT_EQ + col.dot() + ": value >= max, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_LT_EQ + col.dot() + ": value within [min, max)");
        });
    }

    private static PruningDecision evalGt(ColumnPath col, Value v, TypedColumns cols) {
        return withRange(col, cols, range -> {
            if (ValueComparison.compareValues(v, range.max()) >= 0) {
                return new PruningDecision.Eliminated(TIER, OP_GT + col.dot() + ": value >= max");
            }
            if (ValueComparison.compareValues(v, range.min()) < 0
                    && canProveAllMatch(range.kind(), range.nullCount())) {
                return new PruningDecision.PassedAll(TIER, OP_GT + col.dot() + ": value < min, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_GT + col.dot() + ": value within [min, max)");
        });
    }

    private static PruningDecision evalGtEq(ColumnPath col, Value v, TypedColumns cols) {
        return withRange(col, cols, range -> {
            if (ValueComparison.compareValues(v, range.max()) > 0) {
                return new PruningDecision.Eliminated(TIER, OP_GT_EQ + col.dot() + ": value > max");
            }
            if (ValueComparison.compareValues(v, range.min()) <= 0
                    && canProveAllMatch(range.kind(), range.nullCount())) {
                return new PruningDecision.PassedAll(TIER, OP_GT_EQ + col.dot() + ": value <= min, no nulls");
            }
            return new PruningDecision.Inconclusive(TIER, OP_GT_EQ + col.dot() + ": value within (min, max]");
        });
    }

    private static PruningDecision evalIn(ColumnPath col, List<Value> values, TypedColumns cols) {
        return withRange(col, cols, range -> {
            for (Value v : values) {
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

    private static PruningDecision evalIsNull(ColumnPath col, TypedColumns cols, long rowCount) {
        OptionalLong nullCountOpt = lookupNullCount(col, cols);
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

    private static PruningDecision evalIsNotNull(ColumnPath col, TypedColumns cols, long rowCount) {
        OptionalLong nullCountOpt = lookupNullCount(col, cols);
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
     * A min/max comparison can prove all-match only for exactly-bounded primitive kinds with no nulls. FLOAT/DOUBLE
     * (NaN) and binary (possibly truncated stats) are excluded; INT96 has no stats path.
     */
    private static boolean canProveAllMatch(PrimitiveKind kind, long nullCount) {
        if (nullCount != 0) {
            return false;
        }
        return switch (kind) {
            case BOOLEAN, INT32, INT64 -> true;
            case FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> false;
        };
    }

    private static OptionalLong lookupNullCount(ColumnPath col, TypedColumns cols) {
        return cols.get(col).map(ColumnSummary::nullCount).orElseGet(OptionalLong::empty);
    }

    /**
     * Looks up the column's min/max range and dispatches to the supplied evaluator. Returns NotApplied if the stats
     * lack the data we need (no entry, or no decoded min/max value).
     */
    private static PruningDecision withRange(
            ColumnPath col, TypedColumns cols, Function<DecodedRange, PruningDecision> f) {
        Optional<ColumnSummary> stats = cols.get(col);
        if (stats.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "no stats for " + col.dot());
        }
        ColumnSummary cs = stats.get();
        if (cs.min().isEmpty() || cs.max().isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "min/max missing for " + col.dot());
        }
        long nullCount = cs.nullCount().orElse(-1L);
        return f.apply(
                new DecodedRange(cs.kind(), cs.min().orElseThrow(), cs.max().orElseThrow(), nullCount));
    }

    /** Newer writers populate minValue/maxValue; older writers populate min/max. */
    private static MemorySegment preferLatest(MemorySegment latest, MemorySegment legacy) {
        return latest != MemorySegment.NULL ? latest : legacy;
    }

    /** Decodes a Statistics min/max byte payload into a {@link Value} matching the column's primitive kind. */
    private static Optional<Value> decode(PrimitiveKind kind, MemorySegment raw) {
        long size = raw.byteSize();
        return switch (kind) {
            case BOOLEAN -> size >= 1 ? Optional.of(new Value.BoolVal(raw.get(JAVA_BYTE, 0) != 0)) : Optional.empty();
            case INT32 -> size >= 4 ? Optional.of(new Value.IntVal(raw.get(INT32, 0))) : Optional.empty();
            case INT64 -> size >= 8 ? Optional.of(new Value.LongVal(raw.get(INT64, 0))) : Optional.empty();
            case FLOAT -> size >= 4 ? Optional.of(new Value.FloatVal(raw.get(FLOAT, 0))) : Optional.empty();
            case DOUBLE -> size >= 8 ? Optional.of(new Value.DoubleVal(raw.get(DOUBLE, 0))) : Optional.empty();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> Optional.of(new Value.BinaryVal(raw));
            case INT96 -> Optional.empty(); // legacy nanosecond timestamps; not handled at stats tier yet
        };
    }

    /** Aggregate of a column's decoded min/max plus its null count (or {@code -1} if absent). */
    private record DecodedRange(PrimitiveKind kind, Value min, Value max, long nullCount) {}
}
