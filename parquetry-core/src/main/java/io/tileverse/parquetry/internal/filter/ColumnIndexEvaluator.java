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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;
import io.tileverse.parquetry.internal.filter.LeafVerdict.Matches;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Tier-3 (COLUMN_INDEX) evaluator. Judges the pages of each column from its {@link PageIndex}, and produces the
 * {@link RowRanges} of the surviving pages.
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
     * @param columns lookup from column path to its page index
     * @param rowGroupRowCount total rows in the row group
     */
    public static PruningDecision evaluate(
            Predicate predicate, FilterPipeline.PageIndexLookup columns, long rowGroupRowCount) {
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
            Predicate prunable, FilterPipeline.PageIndexLookup columns, long rowGroupRowCount) {
        return switch (prunable) {
            case Predicate.Always(boolean value) ->
                Optional.of(value ? RowRanges.all(rowGroupRowCount) : RowRanges.empty());
            case Predicate.And(List<Predicate> children) -> combineAnd(children, columns, rowGroupRowCount);
            case Predicate.Or(List<Predicate> children) -> combineOr(children, columns, rowGroupRowCount);
            case Predicate.Eq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns);
            case Predicate.NotEq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns);
            case Predicate.Lt(ColumnPath col, Value _) -> leafRanges(prunable, col, columns);
            case Predicate.LtEq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns);
            case Predicate.Gt(ColumnPath col, Value _) -> leafRanges(prunable, col, columns);
            case Predicate.GtEq(ColumnPath col, Value _) -> leafRanges(prunable, col, columns);
            case Predicate.In(ColumnPath col, List<Value> _) -> leafRanges(prunable, col, columns);
            case Predicate.IsNull(ColumnPath col) -> leafRanges(prunable, col, columns);
            case Predicate.IsNotNull(ColumnPath col) -> leafRanges(prunable, col, columns);
            // Out of reach of the page tier, and left out by PagePrunable.part.
            case Predicate.Not _,
                    Predicate.Spatial _,
                    Predicate.GeometryFilterPredicate _,
                    Predicate.RowIndexExcluded _,
                    Predicate.Quantified _ -> Optional.empty();
        };
    }

    private static Optional<RowRanges> combineAnd(
            List<Predicate> children, FilterPipeline.PageIndexLookup cols, long rowGroupRowCount) {
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
            List<Predicate> children, FilterPipeline.PageIndexLookup cols, long rowGroupRowCount) {
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
     * The rows of the pages that may hold a row matching {@code leaf}, each page judged as a unit. The result is empty
     * when no page gets a verdict. The column then has no page statistics, or its pages cannot judge the leaf.
     */
    private static Optional<RowRanges> leafRanges(Predicate leaf, ColumnPath col, FilterPipeline.PageIndexLookup cols) {
        Optional<PageIndex> found = cols.get(col).filter(PageIndex::hasStatistics);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        PageIndex pages = found.orElseThrow();
        boolean testsNulls = testsNulls(leaf);
        List<Range> surviving = new ArrayList<>();
        boolean judged = false;
        for (int page = 0; page < pages.pageCount(); page++) {
            ColumnSummary summary = testsNulls ? pages.nullStatistics(page) : pages.statistics(page);
            Matches matches = LeafVerdicts.of(leaf, summary).matches();
            if (matches != Matches.UNKNOWN) {
                judged = true;
            }
            if (matches != Matches.NONE) {
                surviving.add(new Range(pages.firstRow(page), pages.lastRow(page)));
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
