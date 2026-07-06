/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowPositionSet;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.filter.RowRanges.Range;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.filter.explain.Tier;

/**
 * Prunes a {@link Predicate.RowIndexExcluded} leaf from its deleted-position range alone, without decoding any column
 * data. A row group covers the absolute file positions {@code [base, base + rowCount)} and a page covers
 * {@code [pageBase, pageBase + pageRows)}; the same three-way test applies at both granularities:
 *
 * <ul>
 *   <li>every position in the span is deleted -> eliminate the span (decode nothing);
 *   <li>no deleted position touches the span -> the leaf keeps every row of the span, no per-row check needed;
 *   <li>otherwise the span is partial and is left for a finer tier (page tier, then record level).
 * </ul>
 *
 * <p>The deleted positions are absolute, zero-based file row indexes; the base offset is what turns a row-group- or
 * page-relative span into the absolute range the {@link RowPositionSet} understands. The evaluator never consults
 * column statistics: {@code $pos} has no physical Parquet leaf, hence no min/max and no column index. Its page
 * boundaries come from the offset index row counts of a real column in the same row group, passed in as page first-row
 * indexes.
 *
 * <p>Scope: this range pruning runs only when the normalized predicate is a <em>bare</em>
 * {@link Predicate.RowIndexExcluded}. That covers a full-table scan with deletes (an {@code And(ALWAYS_TRUE, deletes)}
 * normalizes to the bare leaf). When the delete leaf is combined with another predicate -- {@code And(filter,
 * deletes)}, a filtered query over a table with deletes -- the shared statistics and column-index tiers treat the
 * delete leaf as unevaluable (a conservative keep), and the deletes fall to record-level evaluation. Results stay
 * correct: the other predicate's own pruning still runs, and each surviving row pays one cheap
 * {@link RowPositionSet#contains(long)}. The only optimization missed is eliminating a row group that the other
 * predicate keeps but the deletes fully cover, which is uncommon. Combining per-leaf range pruning with the AND tiers
 * is a future optimization, deliberately left out.
 */
final class RowPositionEvaluator {

    private RowPositionEvaluator() {}

    /**
     * The row-group verdict from the deleted-position range. Returns {@link PruningDecision.NotApplied} for any
     * predicate other than a bare {@link Predicate.RowIndexExcluded}; the pipeline only calls this when the predicate
     * is exactly that leaf.
     *
     * @param predicate the normalized predicate
     * @param base the row group's absolute file row offset (the sum of {@code num_rows} of every prior row group)
     * @param rowCount the number of rows in the row group
     */
    static PruningDecision evaluateRowGroup(Predicate predicate, long base, long rowCount) {
        if (!(predicate instanceof Predicate.RowIndexExcluded leaf)) {
            return new PruningDecision.NotApplied(Tier.STATS, "predicate is not a row-position delete");
        }
        RowPositionSet deleted = leaf.deleted();
        long hiExclusive = base + rowCount;
        if (deleted.containsRange(base, hiExclusive)) {
            return new PruningDecision.Eliminated(Tier.STATS, "every row position in the row group is deleted");
        }
        if (!deleted.intersects(base, hiExclusive)) {
            return new PruningDecision.PassedAll(Tier.STATS, "no deleted position falls in the row group");
        }
        return new PruningDecision.Inconclusive(Tier.STATS, "some row positions in the row group are deleted");
    }

    /**
     * The page-granularity verdict from the deleted-position range. Drops fully-deleted pages from the surviving row
     * set and keeps untouched and partially-deleted pages for the record-level tier. Returns
     * {@link PruningDecision.NotApplied} for any predicate other than a bare {@link Predicate.RowIndexExcluded}, or
     * when no page boundaries are available to reason about.
     *
     * @param predicate the normalized predicate
     * @param base the row group's absolute file row offset
     * @param rowCount the number of rows in the row group
     * @param pageFirstRows the row-group-relative first-row index of each page, ascending, the first being zero
     */
    static PruningDecision evaluatePages(Predicate predicate, long base, long rowCount, List<Long> pageFirstRows) {
        if (!(predicate instanceof Predicate.RowIndexExcluded leaf)) {
            return new PruningDecision.NotApplied(Tier.COLUMN_INDEX, "predicate is not a row-position delete");
        }
        if (pageFirstRows.isEmpty()) {
            return new PruningDecision.NotApplied(Tier.COLUMN_INDEX, "no page boundaries for the row-position column");
        }
        RowPositionSet deleted = leaf.deleted();
        List<Range> surviving = survivingPageRanges(deleted, base, rowCount, pageFirstRows);
        return pageDecision(surviving, rowCount);
    }

    private static List<Range> survivingPageRanges(
            RowPositionSet deleted, long base, long rowCount, List<Long> pageFirstRows) {
        int pageCount = pageFirstRows.size();
        List<Range> surviving = new ArrayList<>(pageCount);
        for (int page = 0; page < pageCount; page++) {
            long pageFirstRow = pageFirstRows.get(page);
            long pageLastRowExclusive = lastRowExclusive(page, pageCount, pageFirstRows, rowCount);
            if (!pageIsFullyDeleted(deleted, base, pageFirstRow, pageLastRowExclusive)) {
                surviving.add(new Range(pageFirstRow, pageLastRowExclusive - 1));
            }
        }
        return surviving;
    }

    private static boolean pageIsFullyDeleted(
            RowPositionSet deleted, long base, long pageFirstRow, long pageLastRowExclusive) {
        long absoluteLo = base + pageFirstRow;
        long absoluteHiExclusive = base + pageLastRowExclusive;
        return deleted.containsRange(absoluteLo, absoluteHiExclusive);
    }

    private static long lastRowExclusive(int page, int pageCount, List<Long> pageFirstRows, long rowCount) {
        boolean isLastPage = page + 1 == pageCount;
        return isLastPage ? rowCount : pageFirstRows.get(page + 1);
    }

    private static PruningDecision pageDecision(List<Range> surviving, long rowCount) {
        if (surviving.isEmpty()) {
            return new PruningDecision.Eliminated(Tier.COLUMN_INDEX, "every page of the row group is fully deleted");
        }
        RowRanges ranges = new RowRanges(surviving);
        if (ranges.totalRows() == rowCount) {
            return new PruningDecision.NotApplied(Tier.COLUMN_INDEX, "no page is fully deleted");
        }
        return new PruningDecision.NarrowedTo(
                Tier.COLUMN_INDEX, ranges, "dropped fully-deleted pages, " + ranges.totalRows() + "/" + rowCount);
    }
}
