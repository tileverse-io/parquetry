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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Tier-1 (STATS) evaluator. Inspects a row group's per-column {@link Statistics} (min, max, nullCount) to decide
 * whether the row group can be eliminated, kept whole, or left for downstream tiers to refine.
 *
 * <p>The evaluator assumes the predicate has already been run through {@code PredicateNormalizer}: {@code Not} is
 * pushed to leaves, {@code Always} is folded, and nested {@code And}/{@code Or} are flattened. It only ever returns
 * {@link PruningDecision.Eliminated}, {@link PruningDecision.PassedAll}, or {@link PruningDecision.NotApplied} - the
 * stats tier cannot narrow to a subset of rows.
 */
final class StatsEvaluator {

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

    /**
     * Evaluates {@code predicate} against the given row group's column statistics.
     *
     * @param predicate the normalized predicate
     * @param columns lookup from column path to its stats (kind + Statistics)
     * @param rowCount total rows in the row group
     */
    public static PruningDecision evaluate(
            Predicate predicate, FilterPipeline.ColumnStatsLookup columns, long rowCount) {
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
        };
    }

    private static PruningDecision evaluateAnd(
            List<Predicate> children, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
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

    private static PruningDecision evaluateOr(
            List<Predicate> children, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
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
    private static PruningDecision evaluateNotLeaf(
            Predicate child, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
        PruningDecision inner = evaluate(child, cols, rowCount);
        return switch (inner) {
            case PruningDecision.Eliminated _ -> new PruningDecision.PassedAll(TIER, "NOT of eliminated child");
            case PruningDecision.PassedAll _ -> new PruningDecision.Eliminated(TIER, "NOT of passed-all child");
            default -> new PruningDecision.NotApplied(TIER, "NOT of inconclusive child");
        };
    }

    private static PruningDecision evalEq(
            ColumnPath col, Value v, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
        return withRange(col, cols, range -> {
            int cmpMin = compare(range.kind(), v, range.min());
            int cmpMax = compare(range.kind(), v, range.max());
            if (cmpMin < 0 || cmpMax > 0) {
                return new PruningDecision.Eliminated(TIER, OP_EQ + col.dot() + ": value outside [min, max]");
            }
            if (cmpMin == 0 && cmpMax == 0 && range.nullCount() == 0 && rowCount > 0) {
                return new PruningDecision.PassedAll(TIER, OP_EQ + col.dot() + ": single distinct value matches");
            }
            return new PruningDecision.NotApplied(TIER, OP_EQ + col.dot() + ": value within [min, max]");
        });
    }

    private static PruningDecision evalNotEq(
            ColumnPath col, Value v, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
        return withRange(col, cols, range -> {
            int cmpMin = compare(range.kind(), v, range.min());
            int cmpMax = compare(range.kind(), v, range.max());
            if (cmpMin < 0 || cmpMax > 0) {
                return new PruningDecision.PassedAll(TIER, OP_NOT_EQ + col.dot() + ": value outside [min, max]");
            }
            if (cmpMin == 0 && cmpMax == 0 && range.nullCount() == 0 && rowCount > 0) {
                return new PruningDecision.Eliminated(
                        TIER, OP_NOT_EQ + col.dot() + ": column is single value equal to operand");
            }
            return new PruningDecision.NotApplied(TIER, OP_NOT_EQ + col.dot() + ": value within [min, max]");
        });
    }

    private static PruningDecision evalLt(ColumnPath col, Value v, FilterPipeline.ColumnStatsLookup cols) {
        return withRange(col, cols, range -> {
            if (compare(range.kind(), v, range.min()) <= 0) {
                return new PruningDecision.Eliminated(TIER, OP_LT + col.dot() + ": value <= min");
            }
            if (compare(range.kind(), v, range.max()) > 0) {
                return new PruningDecision.PassedAll(TIER, OP_LT + col.dot() + ": value > max");
            }
            return new PruningDecision.NotApplied(TIER, OP_LT + col.dot() + ": value within (min, max]");
        });
    }

    private static PruningDecision evalLtEq(ColumnPath col, Value v, FilterPipeline.ColumnStatsLookup cols) {
        return withRange(col, cols, range -> {
            if (compare(range.kind(), v, range.min()) < 0) {
                return new PruningDecision.Eliminated(TIER, OP_LT_EQ + col.dot() + ": value < min");
            }
            if (compare(range.kind(), v, range.max()) >= 0) {
                return new PruningDecision.PassedAll(TIER, OP_LT_EQ + col.dot() + ": value >= max");
            }
            return new PruningDecision.NotApplied(TIER, OP_LT_EQ + col.dot() + ": value within [min, max)");
        });
    }

    private static PruningDecision evalGt(ColumnPath col, Value v, FilterPipeline.ColumnStatsLookup cols) {
        return withRange(col, cols, range -> {
            if (compare(range.kind(), v, range.max()) >= 0) {
                return new PruningDecision.Eliminated(TIER, OP_GT + col.dot() + ": value >= max");
            }
            if (compare(range.kind(), v, range.min()) < 0) {
                return new PruningDecision.PassedAll(TIER, OP_GT + col.dot() + ": value < min");
            }
            return new PruningDecision.NotApplied(TIER, OP_GT + col.dot() + ": value within [min, max)");
        });
    }

    private static PruningDecision evalGtEq(ColumnPath col, Value v, FilterPipeline.ColumnStatsLookup cols) {
        return withRange(col, cols, range -> {
            if (compare(range.kind(), v, range.max()) > 0) {
                return new PruningDecision.Eliminated(TIER, OP_GT_EQ + col.dot() + ": value > max");
            }
            if (compare(range.kind(), v, range.min()) <= 0) {
                return new PruningDecision.PassedAll(TIER, OP_GT_EQ + col.dot() + ": value <= min");
            }
            return new PruningDecision.NotApplied(TIER, OP_GT_EQ + col.dot() + ": value within (min, max]");
        });
    }

    private static PruningDecision evalIn(ColumnPath col, List<Value> values, FilterPipeline.ColumnStatsLookup cols) {
        return withRange(col, cols, range -> {
            for (Value v : values) {
                int cmpMin = compare(range.kind(), v, range.min());
                int cmpMax = compare(range.kind(), v, range.max());
                if (cmpMin >= 0 && cmpMax <= 0) {
                    return new PruningDecision.NotApplied(
                            TIER, OP_IN + col.dot() + ": at least one value within [min, max]");
                }
            }
            return new PruningDecision.Eliminated(TIER, OP_IN + col.dot() + ": no value within [min, max]");
        });
    }

    private static PruningDecision evalIsNull(ColumnPath col, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
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
        return new PruningDecision.NotApplied(TIER, OP_IS_NULL + col.dot() + ": some nulls");
    }

    private static PruningDecision evalIsNotNull(ColumnPath col, FilterPipeline.ColumnStatsLookup cols, long rowCount) {
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
        return new PruningDecision.NotApplied(TIER, OP_IS_NOT_NULL + col.dot() + ": some nulls");
    }

    private static OptionalLong lookupNullCount(ColumnPath col, FilterPipeline.ColumnStatsLookup cols) {
        Optional<FilterPipeline.ColumnStats> stats = cols.get(col);
        return stats.isPresent() ? stats.get().statistics().nullCount() : OptionalLong.empty();
    }

    /**
     * Looks up the column's min/max range and dispatches to the supplied evaluator. Returns NotApplied if the stats
     * lack the data we need (no entry, no min/max bytes, or a kind we don't know how to decode).
     */
    private static PruningDecision withRange(
            ColumnPath col,
            FilterPipeline.ColumnStatsLookup cols,
            java.util.function.Function<DecodedRange, PruningDecision> f) {
        Optional<FilterPipeline.ColumnStats> stats = cols.get(col);
        if (stats.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "no stats for " + col.dot());
        }
        FilterPipeline.ColumnStats cs = stats.get();
        MemorySegment minBytes =
                preferLatest(cs.statistics().minValue(), cs.statistics().min());
        MemorySegment maxBytes =
                preferLatest(cs.statistics().maxValue(), cs.statistics().max());
        if (minBytes == MemorySegment.NULL || maxBytes == MemorySegment.NULL) {
            return new PruningDecision.NotApplied(TIER, "min/max missing for " + col.dot());
        }
        Optional<Value> minValue = decode(cs.kind(), minBytes);
        Optional<Value> maxValue = decode(cs.kind(), maxBytes);
        if (minValue.isEmpty() || maxValue.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "unsupported kind " + cs.kind() + " for " + col.dot());
        }
        long nullCount = cs.statistics().nullCount().orElse(-1L);
        return f.apply(new DecodedRange(cs.kind(), minValue.get(), maxValue.get(), nullCount));
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

    /**
     * Compares the predicate-side value to a decoded min or max value. Returns negative if {@code query < bound}, zero
     * if equal, positive if {@code query > bound}. Returns 0 for type mismatches (caller already had its chance via the
     * schema validator).
     */
    // S3776: dispatch table; cyclomatic complexity is inherent.
    // S7475: palantirJavaFormat 2.90 cannot parse bare _ in nested record patterns; see memory
    // feedback-palantir-unnamed-pattern.
    @SuppressWarnings({"java:S3776", "java:S7475"})
    private static int compare(PrimitiveKind kind, Value query, Value bound) {
        return switch (query) {
            case Value.BoolVal(boolean qv) when bound instanceof Value.BoolVal(boolean bv) -> Boolean.compare(qv, bv);
            case Value.IntVal(int qv) when bound instanceof Value.IntVal(int bv) -> Integer.compare(qv, bv);
            case Value.LongVal(long qv) when bound instanceof Value.LongVal(long bv) -> Long.compare(qv, bv);
            case Value.FloatVal(float qv) when bound instanceof Value.FloatVal(float bv) -> Float.compare(qv, bv);
            case Value.DoubleVal(double qv) when bound instanceof Value.DoubleVal(double bv) -> Double.compare(qv, bv);
            case Value.StringVal(String qv)
            when bound instanceof Value.BinaryVal(MemorySegment bv) ->
                compareBytes(MemorySegment.ofArray(qv.getBytes(java.nio.charset.StandardCharsets.UTF_8)), bv);
            case Value.BinaryVal(MemorySegment qv)
            when bound instanceof Value.BinaryVal(MemorySegment bv) -> compareBytes(qv, bv);
            case Value.DateVal(java.time.LocalDate qv)
            when bound instanceof Value.IntVal(int bv) -> Integer.compare((int) qv.toEpochDay(), bv);
            case Value.TimestampVal(java.time.LocalDateTime qv, boolean _)
            when bound instanceof Value.LongVal(long bv) ->
                Long.compare(qv.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L, bv);
            default -> 0;
        };
    }

    /** Lexicographic unsigned byte comparison, mirroring Parquet's default ColumnOrder for binary columns. */
    private static int compareBytes(MemorySegment a, MemorySegment b) {
        long aLen = a.byteSize();
        long bLen = b.byteSize();
        long common = Math.min(aLen, bLen);
        for (long i = 0; i < common; i++) {
            int diff = (a.get(JAVA_BYTE, i) & 0xff) - (b.get(JAVA_BYTE, i) & 0xff);
            if (diff != 0) {
                return diff;
            }
        }
        return Long.compare(aLen, bLen);
    }

    /** Aggregate of a column's decoded min/max plus its null count (or {@code -1} if absent). */
    private record DecodedRange(PrimitiveKind kind, Value min, Value max, long nullCount) {}
}
