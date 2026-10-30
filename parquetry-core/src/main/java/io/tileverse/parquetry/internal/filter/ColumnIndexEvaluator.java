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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.internal.filter.LeafVerdict.Matches;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-3 (COLUMN_INDEX) evaluator. Uses per-page min/max, null markers and NaN counts from {@code ColumnIndex} together
 * with the page row offsets from {@code OffsetIndex} to produce a {@link RowRanges} of surviving pages.
 *
 * <p>Decision semantics:
 *
 * <ul>
 *   <li>{@link PruningDecision.Eliminated} when no page survives.
 *   <li>{@link PruningDecision.PassedAll} when every page survives - i.e. the predicate doesn't help here.
 *   <li>{@link PruningDecision.NarrowedTo} when a strict subset of pages survives.
 *   <li>{@link PruningDecision.NotApplied} when the predicate has no part evaluable from page statistics (spatial
 *       predicates, negations), or when the page statistics of its columns cannot judge it.
 * </ul>
 *
 * <p>Each page is judged as a unit by {@link LeafVerdicts}, with the rules of the STATS tier. Assumes the predicate has
 * already been normalized (Not pushed to leaves, Always folded, And/Or flattened).
 */
final class ColumnIndexEvaluator {

    private static final Tier TIER = Tier.COLUMN_INDEX;
    private static final String NO_APPLICABLE_INDEX = "no applicable column index for predicate";

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
        Optional<Predicate> prunable = PagePrunable.part(predicate);
        if (prunable.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, NO_APPLICABLE_INDEX);
        }
        columns.warm(PagePrunable.columns(prunable.orElseThrow()));
        Optional<RowRanges> surviving = matchRanges(prunable.orElseThrow(), columns, rowGroupRowCount);
        if (surviving.isEmpty()) {
            return new PruningDecision.NotApplied(TIER, NO_APPLICABLE_INDEX);
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
     * Returns the row ranges that could satisfy {@code prunable} according to the loaded ColumnIndex/OffsetIndex. Empty
     * when the page statistics of its columns cannot judge it, and the tier does not apply.
     */
    private static Optional<RowRanges> matchRanges(
            Predicate prunable, FilterPipeline.ColumnPageStatsLookup columns, long rowGroupRowCount) {
        return switch (prunable) {
            case Predicate.Always(boolean value) ->
                Optional.of(value ? RowRanges.all(rowGroupRowCount) : RowRanges.empty());
            case Predicate.And(List<Predicate> children) -> combineAnd(children, columns, rowGroupRowCount);
            case Predicate.Or(List<Predicate> children) -> combineOr(children, columns, rowGroupRowCount);
            case Predicate.Eq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.NotEq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.Lt(ColumnPath col, Value _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.LtEq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.Gt(ColumnPath col, Value _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.GtEq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.In(ColumnPath col, List<Value> _) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.IsNull(ColumnPath col) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            case Predicate.IsNotNull(ColumnPath col) -> leafRanges(prunable, col, columns, rowGroupRowCount);
            // Out of reach of the page tier, and left out by PagePrunable.part.
            case Predicate.Not _,
                    Predicate.Spatial _,
                    Predicate.GeometryFilterPredicate _,
                    Predicate.RowIndexExcluded _,
                    Predicate.Quantified _ -> Optional.empty();
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

    /**
     * The rows of the pages that may hold a row matching {@code leaf}. Each page is judged as a unit, and a page
     * without rows is skipped. The result is empty when no page gets a verdict. The column then has no page statistics,
     * or its pages cannot judge the leaf.
     */
    private static Optional<RowRanges> leafRanges(
            Predicate leaf, ColumnPath col, FilterPipeline.ColumnPageStatsLookup cols, long rowGroupRowCount) {
        Optional<FilterPipeline.ColumnPageStats> found = cols.get(col);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        FilterPipeline.ColumnPageStats stats = found.orElseThrow();
        List<PageLocation> pages = stats.offsetIndex().pageLocations();
        if (stats.columnIndex().minValues().size() != pages.size()) {
            return Optional.empty(); // index inconsistency
        }
        boolean readBounds = !testsNulls(leaf);
        List<Range> surviving = new ArrayList<>();
        boolean judged = false;
        for (int page = 0; page < pages.size(); page++) {
            long rows = rowsOfPage(pages, page, rowGroupRowCount);
            if (rows <= 0) {
                continue;
            }
            ColumnSummary summary = pageSummary(stats, page, rows, readBounds);
            Matches matches = LeafVerdicts.of(leaf, summary).matches();
            if (matches != Matches.UNKNOWN) {
                judged = true;
            }
            if (matches != Matches.NONE) {
                surviving.add(pageRange(pages, page, rowGroupRowCount));
            }
        }
        if (!judged) {
            return Optional.empty();
        }
        return Optional.of(new RowRanges(surviving));
    }

    /** Whether {@code leaf} tests the nulls of its column, and needs the null counts of the pages alone. */
    private static boolean testsNulls(Predicate leaf) {
        return leaf instanceof Predicate.IsNull || leaf instanceof Predicate.IsNotNull;
    }

    /**
     * The statistics of page {@code page} as a unit of {@code rows} rows, holding one value per row. A page marked as
     * holding only nulls counts each of its rows as null, and its bounds are not read. The bounds of the other pages
     * are read only when {@code readBounds} is on.
     */
    private static ColumnSummary pageSummary(
            FilterPipeline.ColumnPageStats stats, int page, long rows, boolean readBounds) {
        ColumnIndex index = stats.columnIndex();
        int pageCount = index.minValues().size();
        OptionalLong values = OptionalLong.of(rows);
        if (Boolean.TRUE.equals(index.nullPages().get(page))) {
            return new ColumnSummary(
                    stats.kind(), Optional.empty(), Optional.empty(), values, values, NaNCells.POSSIBLE);
        }
        OptionalLong nullCount = trustedNullCount(countOfPage(index.nullCounts(), pageCount, page), rows);
        OptionalLong nanCount = countOfPage(index.nanCounts(), pageCount, page);
        Optional<Value> min = readBounds ? pageBound(stats, index.minValues().get(page)) : Optional.empty();
        Optional<Value> max = readBounds ? pageBound(stats, index.maxValues().get(page)) : Optional.empty();
        NaNCells counted = NaNCells.ofPageCounts(nanCount, nullCount, rows);
        return ColumnSummary.of(stats.kind(), min, max, nullCount, values, counted);
    }

    /**
     * The null count of a page not marked as holding only nulls, or empty when it contradicts the mark. Such a page
     * holds a value, and fewer nulls than rows.
     */
    private static OptionalLong trustedNullCount(OptionalLong recorded, long rows) {
        if (recorded.isPresent() && recorded.getAsLong() >= rows) {
            return OptionalLong.empty();
        }
        return recorded;
    }

    /**
     * A bound of a page decoded to a value. It is empty when the bound does not decode, or when the bounds follow an
     * order not applied by this reader.
     */
    private static Optional<Value> pageBound(FilterPipeline.ColumnPageStats stats, MemorySegment bound) {
        if (!stats.boundsOrdered()) {
            return Optional.empty();
        }
        return StatisticsValueDecoder.decode(stats.kind(), stats.logicalType(), bound);
    }

    /**
     * The count of page {@code i} in a per-page list of the index, usable only when the list has one count per page.
     */
    private static OptionalLong countOfPage(Optional<List<Long>> perPageCounts, int pageCount, int i) {
        Optional<List<Long>> counts = perPageCounts.filter(list -> list.size() == pageCount);
        if (counts.isEmpty()) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(counts.orElseThrow().get(i));
    }

    /** The number of rows of page {@code i}: up to the first row of the next page, or to the end of the row group. */
    private static long rowsOfPage(List<PageLocation> pages, int i, long rowGroupRowCount) {
        long first = pages.get(i).firstRowIndex();
        boolean lastPage = i + 1 == pages.size();
        long end = lastPage ? rowGroupRowCount : pages.get(i + 1).firstRowIndex();
        return end - first;
    }

    private static Range pageRange(List<PageLocation> pages, int i, long totalRows) {
        long first = pages.get(i).firstRowIndex();
        long last = (i + 1 < pages.size()) ? pages.get(i + 1).firstRowIndex() - 1 : totalRows - 1;
        return new Range(first, last);
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
}
