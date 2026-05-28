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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Tier-3 (COLUMN_INDEX) evaluator. Uses per-page min/max + null markers from {@code ColumnIndex} together with the page
 * row offsets from {@code OffsetIndex} to produce a {@link RowRanges} of surviving pages.
 *
 * <p>Decision semantics:
 *
 * <ul>
 *   <li>{@link PruningDecision.Eliminated} when no page survives.
 *   <li>{@link PruningDecision.PassedAll} when every page survives - i.e. the predicate doesn't help here.
 *   <li>{@link PruningDecision.NarrowedTo} when a strict subset of pages survives.
 *   <li>{@link PruningDecision.NotApplied} when the column has no ColumnIndex/OffsetIndex loaded, the value type isn't
 *       decodable, or the predicate doesn't translate to a column-index lookup (spatial predicates).
 * </ul>
 *
 * <p>Assumes the predicate has already been normalized (Not pushed to leaves, Always folded, And/Or flattened).
 */
final class ColumnIndexEvaluator {

    private static final Tier TIER = Tier.COLUMN_INDEX;

    private ColumnIndexEvaluator() {}

    /**
     * Evaluates {@code predicate} against per-page indexes for the columns it references.
     *
     * @param predicate the normalized predicate
     * @param columns lookup from column path to its page-level stats
     * @param rowGroupRowCount total rows in the row group; used to size the last page
     */
    public static PruningDecision evaluate(
            Predicate predicate, FilterPipeline.ColumnPageStatsLookup columns, long rowGroupRowCount) {
        Optional<RowRanges> surviving = matchRanges(predicate, columns, rowGroupRowCount);
        if (surviving.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, "no applicable column index for predicate");
        }
        RowRanges ranges = surviving.get();
        long total = RowRanges.all(rowGroupRowCount).totalRows();
        if (ranges.isEmpty()) {
            return new PruningDecision.Eliminated(TIER, "no page survives column-index pruning");
        }
        if (ranges.totalRows() == total) {
            return new PruningDecision.PassedAll(TIER, "every page survives column-index pruning");
        }
        return new PruningDecision.NarrowedTo(
                TIER, ranges, "narrowed to " + ranges.totalRows() + "/" + total + " rows");
    }

    /**
     * Returns the row ranges that could satisfy {@code predicate} according to the loaded ColumnIndex/OffsetIndex.
     * Empty when no column referenced by the predicate has a column index loaded (and so the tier doesn't apply).
     */
    private static Optional<RowRanges> matchRanges(
            Predicate predicate, FilterPipeline.ColumnPageStatsLookup columns, long rowGroupRowCount) {
        return switch (predicate) {
            case Predicate.Always(boolean value) ->
                Optional.of(value ? RowRanges.all(rowGroupRowCount) : RowRanges.empty());
            case Predicate.And(List<Predicate> children) -> combineAnd(children, columns, rowGroupRowCount);
            case Predicate.Or(List<Predicate> children) -> combineOr(children, columns, rowGroupRowCount);
            case Predicate.Not(Predicate child) ->
                matchRanges(child, columns, rowGroupRowCount).map(r -> complement(r, rowGroupRowCount));
            case Predicate.Eq(ColumnPath col, Value v) ->
                leafRanges(col, columns, rowGroupRowCount, (min, max) -> within(v, min, max));
            case Predicate.NotEq(ColumnPath col, Value v) ->
                leafRanges(col, columns, rowGroupRowCount, (min, max) -> !singleValueEqual(v, min, max));
            case Predicate.Lt(ColumnPath col, Value v) ->
                leafRanges(col, columns, rowGroupRowCount, (min, max) -> compare(v, min) > 0);
            case Predicate.LtEq(ColumnPath col, Value v) ->
                leafRanges(col, columns, rowGroupRowCount, (min, max) -> compare(v, min) >= 0);
            case Predicate.Gt(ColumnPath col, Value v) ->
                leafRanges(col, columns, rowGroupRowCount, (min, max) -> compare(v, max) < 0);
            case Predicate.GtEq(ColumnPath col, Value v) ->
                leafRanges(col, columns, rowGroupRowCount, (min, max) -> compare(v, max) <= 0);
            case Predicate.In(ColumnPath col, List<Value> values) ->
                leafRanges(
                        col,
                        columns,
                        rowGroupRowCount,
                        (min, max) -> values.stream().anyMatch(v -> within(v, min, max)));
            case Predicate.IsNull(ColumnPath col) -> nullLeafRanges(col, columns, rowGroupRowCount, true);
            case Predicate.IsNotNull(ColumnPath col) -> nullLeafRanges(col, columns, rowGroupRowCount, false);
            case Predicate.Spatial _ -> Optional.empty();
        };
    }

    private static Optional<RowRanges> combineAnd(
            List<Predicate> children, FilterPipeline.ColumnPageStatsLookup cols, long rowGroupRowCount) {
        RowRanges acc = RowRanges.all(rowGroupRowCount);
        boolean anyApplied = false;
        for (Predicate child : children) {
            Optional<RowRanges> childRanges = matchRanges(child, cols, rowGroupRowCount);
            if (childRanges.isPresent()) {
                anyApplied = true;
                acc = acc.intersect(childRanges.orElseThrow());
                if (acc.isEmpty()) {
                    break; // short-circuit on empty intersection
                }
            }
        }
        return anyApplied ? Optional.of(acc) : Optional.empty();
    }

    private static Optional<RowRanges> combineOr(
            List<Predicate> children, FilterPipeline.ColumnPageStatsLookup cols, long rowGroupRowCount) {
        RowRanges acc = RowRanges.empty();
        for (Predicate child : children) {
            Optional<RowRanges> childRanges = matchRanges(child, cols, rowGroupRowCount);
            if (childRanges.isEmpty()) {
                // If even one OR child can't be evaluated here, we can't safely narrow.
                return Optional.empty();
            }
            acc = union(acc, childRanges.get());
        }
        return Optional.of(acc);
    }

    private static Optional<RowRanges> leafRanges(
            ColumnPath col, FilterPipeline.ColumnPageStatsLookup cols, long rowGroupRowCount, PageMatcher matcher) {
        Optional<FilterPipeline.ColumnPageStats> opt = cols.get(col);
        if (opt.isEmpty()) {
            return Optional.empty();
        }
        FilterPipeline.ColumnPageStats stats = opt.get();
        ColumnIndex idx = stats.columnIndex();
        OffsetIndex off = stats.offsetIndex();
        int pageCount = idx.minValues().size();
        if (off.pageLocations().size() != pageCount) {
            return Optional.empty(); // index inconsistency
        }
        List<Range> surviving = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            if (Boolean.TRUE.equals(idx.nullPages().get(i))) {
                continue; // all-null page can't satisfy a value comparison
            }
            Optional<Value> minVal = decode(stats.kind(), idx.minValues().get(i));
            Optional<Value> maxVal = decode(stats.kind(), idx.maxValues().get(i));
            if (minVal.isEmpty() || maxVal.isEmpty()) {
                return Optional.empty(); // unsupported type; bail to NotApplied
            }
            if (matcher.matches(minVal.orElseThrow(), maxVal.orElseThrow())) {
                surviving.add(pageRange(off.pageLocations(), i, rowGroupRowCount));
            }
        }
        return Optional.of(new RowRanges(surviving));
    }

    private static Optional<RowRanges> nullLeafRanges(
            ColumnPath col, FilterPipeline.ColumnPageStatsLookup cols, long rowGroupRowCount, boolean wantNulls) {
        Optional<FilterPipeline.ColumnPageStats> opt = cols.get(col);
        if (opt.isEmpty()) {
            return Optional.empty();
        }
        FilterPipeline.ColumnPageStats stats = opt.get();
        ColumnIndex idx = stats.columnIndex();
        OffsetIndex off = stats.offsetIndex();
        int pageCount = idx.minValues().size();
        if (off.pageLocations().size() != pageCount) {
            return Optional.empty();
        }
        Optional<List<Long>> nullCounts = idx.nullCounts();
        List<Range> surviving = new ArrayList<>();
        for (int i = 0; i < pageCount; i++) {
            if (pageMatchesNullPredicate(i, idx, off, nullCounts, rowGroupRowCount, wantNulls)) {
                surviving.add(pageRange(off.pageLocations(), i, rowGroupRowCount));
            }
        }
        return Optional.of(new RowRanges(surviving));
    }

    /**
     * Decides whether page {@code i} can satisfy an IsNull (if {@code wantNulls}) or IsNotNull predicate.
     *
     * <p>{@code wantNulls}: keep all-null pages, plus non-all-null pages whose nullCounts entry is positive or whose
     * null counts are absent (so we can't rule out nulls).
     *
     * <p>{@code !wantNulls}: skip all-null pages; keep the rest unless null counts say every row of the page is null.
     */
    private static boolean pageMatchesNullPredicate(
            int i,
            ColumnIndex idx,
            OffsetIndex off,
            Optional<List<Long>> nullCounts,
            long rowGroupRowCount,
            boolean wantNulls) {
        boolean allNull = idx.nullPages().get(i);
        if (wantNulls) {
            if (allNull) {
                return true;
            }
            return nullCounts.isEmpty() || nullCounts.get().get(i) > 0;
        }
        if (allNull) {
            return false;
        }
        if (nullCounts.isEmpty()) {
            return true;
        }
        long pageRows = pageRange(off.pageLocations(), i, rowGroupRowCount).count();
        return nullCounts.get().get(i) < pageRows;
    }

    private static Range pageRange(List<PageLocation> pages, int i, long totalRows) {
        long first = pages.get(i).firstRowIndex();
        long last = (i + 1 < pages.size()) ? pages.get(i + 1).firstRowIndex() - 1 : totalRows - 1;
        return new Range(first, last);
    }

    /** Returns the complement of {@code ranges} within {@code [0, totalRows-1]}. */
    private static RowRanges complement(RowRanges ranges, long totalRows) {
        if (totalRows <= 0) {
            return RowRanges.empty();
        }
        List<Range> out = new ArrayList<>();
        long cursor = 0;
        for (Range r : ranges.ranges()) {
            if (r.first() > cursor) {
                out.add(new Range(cursor, r.first() - 1));
            }
            cursor = Math.max(cursor, r.last() + 1);
        }
        if (cursor < totalRows) {
            out.add(new Range(cursor, totalRows - 1));
        }
        return new RowRanges(out);
    }

    /** Returns the union of two sorted, disjoint row range sets. */
    private static RowRanges union(RowRanges a, RowRanges b) {
        List<Range> all = new ArrayList<>(a.ranges().size() + b.ranges().size());
        all.addAll(a.ranges());
        all.addAll(b.ranges());
        all.sort((x, y) -> Long.compare(x.first(), y.first()));
        List<Range> merged = new ArrayList<>();
        for (Range r : all) {
            if (!merged.isEmpty() && merged.get(merged.size() - 1).last() + 1 >= r.first()) {
                Range last = merged.remove(merged.size() - 1);
                merged.add(new Range(last.first(), Math.max(last.last(), r.last())));
            } else {
                merged.add(r);
            }
        }
        return new RowRanges(merged);
    }

    /** True if {@code v} is within {@code [min, max]} (inclusive). */
    private static boolean within(Value v, Value min, Value max) {
        return compare(v, min) >= 0 && compare(v, max) <= 0;
    }

    /** True when min == max == v (so the page has exactly one distinct value equal to {@code v}). */
    private static boolean singleValueEqual(Value v, Value min, Value max) {
        return compare(min, max) == 0 && compare(v, min) == 0;
    }

    /** Decodes a Parquet PLAIN-encoded min/max value into a typed {@link Value}. */
    private static Optional<Value> decode(PrimitiveKind kind, MemorySegment raw) {
        long size = raw.byteSize();
        return switch (kind) {
            case BOOLEAN -> size >= 1 ? Optional.of(new Value.BoolVal(raw.get(JAVA_BYTE, 0) != 0)) : Optional.empty();
            case INT32 -> size >= 4 ? Optional.of(new Value.IntVal(raw.get(INT32, 0))) : Optional.empty();
            case INT64 -> size >= 8 ? Optional.of(new Value.LongVal(raw.get(INT64, 0))) : Optional.empty();
            case FLOAT -> size >= 4 ? Optional.of(new Value.FloatVal(raw.get(FLOAT, 0))) : Optional.empty();
            case DOUBLE -> size >= 8 ? Optional.of(new Value.DoubleVal(raw.get(DOUBLE, 0))) : Optional.empty();
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> Optional.of(new Value.BinaryVal(raw));
            case INT96 -> Optional.empty();
        };
    }

    // S7475 (bare _ in nested record patterns) is informational only - palantirJavaFormat 2.90 cannot
    // parse the bare-underscore form Sonar suggests; see memory feedback-palantir-unnamed-pattern.
    @SuppressWarnings({"java:S3776", "java:S7475"})
    private static int compare(Value a, Value b) {
        return switch (a) {
            case Value.BoolVal(boolean av) when b instanceof Value.BoolVal(boolean bv) -> Boolean.compare(av, bv);
            case Value.IntVal(int av) when b instanceof Value.IntVal(int bv) -> Integer.compare(av, bv);
            case Value.LongVal(long av) when b instanceof Value.LongVal(long bv) -> Long.compare(av, bv);
            case Value.FloatVal(float av) when b instanceof Value.FloatVal(float bv) -> Float.compare(av, bv);
            case Value.DoubleVal(double av) when b instanceof Value.DoubleVal(double bv) -> Double.compare(av, bv);
            case Value.StringVal(String av)
            when b instanceof Value.BinaryVal(MemorySegment bv) ->
                compareBytes(MemorySegment.ofArray(av.getBytes(StandardCharsets.UTF_8)), bv);
            case Value.BinaryVal(MemorySegment av)
            when b instanceof Value.BinaryVal(MemorySegment bv) -> compareBytes(av, bv);
            case Value.DateVal(java.time.LocalDate av)
            when b instanceof Value.IntVal(int bv) -> Integer.compare((int) av.toEpochDay(), bv);
            case Value.TimestampVal(java.time.LocalDateTime av, boolean _)
            when b instanceof Value.LongVal(long bv) ->
                Long.compare(av.toEpochSecond(java.time.ZoneOffset.UTC) * 1000L, bv);
            default -> 0;
        };
    }

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

    @FunctionalInterface
    private interface PageMatcher {
        boolean matches(Value min, Value max);
    }
}
