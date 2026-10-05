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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.internal.write.page.PageStatistics;

import lombok.NonNull;

/**
 * Per-row-group, per-column footer index builder.
 *
 * <p>Each call to {@link #appendPage(PageStatistics)} adds one data-page entry: a null-page flag, the page's typed
 * min/max (or {@link MemorySegment#NULL} when the page produced no ordered observation), the per-page null count and,
 * for a floating-point column, the per-page NaN count. {@link #finishChunk()} assembles the {@link ColumnIndex} for the
 * current accumulation window and clears state, readying the builder for the next column chunk.
 *
 * <p>{@code boundary_order} is computed from the non-null-page sequence in the {@link BoundsOrder} of the column, the
 * order of the page bounds themselves. Zero or one non-null pages default to {@link BoundaryOrder#UNORDERED} -- the
 * safe value that forces a linear scan on the read side.
 *
 * <p>A column with an {@link BoundsOrder#UNDEFINED undefined} order gets no column index: the format defines the page
 * bounds of a column index by the order of the column.
 */
public final class ColumnIndexBuilder {

    private final BoundsOrder order;

    private final List<Boolean> nullPages = new ArrayList<>();
    private final List<MemorySegment> minValues = new ArrayList<>();
    private final List<MemorySegment> maxValues = new ArrayList<>();
    private final List<Long> nullCounts = new ArrayList<>();
    private final List<Long> nanCounts = new ArrayList<>();

    /** @param order the order of the page bounds of the column described by this builder */
    ColumnIndexBuilder(@NonNull BoundsOrder order) {
        this.order = order;
    }

    /** Appends one page entry from the accumulator's per-page snapshot. */
    public void appendPage(@NonNull PageStatistics pageStats) {
        nullPages.add(pageStats.isNullPage());
        minValues.add(pageStats.min());
        maxValues.add(pageStats.max());
        nullCounts.add(pageStats.nullCount());
        if (order.isFloatingPoint()) {
            nanCounts.add(requiredNaNCount(pageStats));
        }
    }

    private static long requiredNaNCount(PageStatistics pageStats) {
        return pageStats
                .nanCount()
                .orElseThrow(() -> new IllegalStateException("a page of a floating-point column has no NaN count"));
    }

    /**
     * Returns the {@link ColumnIndex} assembled from the {@link #appendPage} calls since the last reset, empty when no
     * column index can describe the chunk; see {@link #indexable()}. Calling this method also clears internal state,
     * readying the builder for the next column chunk.
     */
    public Optional<ColumnIndex> finishChunk() {
        Optional<ColumnIndex> index = indexable() ? Optional.of(assemble()) : Optional.empty();
        reset();
        return index;
    }

    /**
     * Whether a column index can describe the appended pages: the column has a defined order, and each non-null page
     * has a min and a max, as required by the format. A page of only NaN is a non-null page without bounds.
     */
    private boolean indexable() {
        return order.isDefined() && eachNonNullPageHasBounds();
    }

    private boolean eachNonNullPageHasBounds() {
        for (int i = 0; i < nullPages.size(); i++) {
            boolean isNullPage = nullPages.get(i).booleanValue();
            if (!isNullPage && !hasBounds(i)) {
                return false;
            }
        }
        return true;
    }

    private boolean hasBounds(int page) {
        return minValues.get(page) != MemorySegment.NULL && maxValues.get(page) != MemorySegment.NULL;
    }

    private ColumnIndex assemble() {
        return new ColumnIndex(
                List.copyOf(nullPages),
                List.copyOf(minValues),
                List.copyOf(maxValues),
                computeBoundaryOrder(),
                Optional.of(List.copyOf(nullCounts)),
                Optional.empty(),
                Optional.empty(),
                nanCountOfEachPage());
    }

    /**
     * Walks the non-null-page sequence to decide whether the min/max bytes are monotonically ordered. Returns
     * {@link BoundaryOrder#UNORDERED} on the trivial cases (no pages, one page, all-null pages) so a reader cannot
     * perform an unsafe binary search.
     */
    private BoundaryOrder computeBoundaryOrder() {
        int firstNonNull = firstNonNullPageIndex();
        if (firstNonNull < 0) {
            return BoundaryOrder.UNORDERED;
        }
        int secondNonNull = nextNonNullPageIndex(firstNonNull + 1);
        if (secondNonNull < 0) {
            return BoundaryOrder.UNORDERED;
        }
        Direction direction = directionBetween(firstNonNull, secondNonNull);
        if (direction == Direction.UNDEFINED) {
            return BoundaryOrder.UNORDERED;
        }
        int prev = secondNonNull;
        int next = nextNonNullPageIndex(prev + 1);
        while (next >= 0) {
            if (directionBetween(prev, next) != direction) {
                return BoundaryOrder.UNORDERED;
            }
            prev = next;
            next = nextNonNullPageIndex(prev + 1);
        }
        return direction == Direction.ASCENDING ? BoundaryOrder.ASCENDING : BoundaryOrder.DESCENDING;
    }

    private int firstNonNullPageIndex() {
        return nextNonNullPageIndex(0);
    }

    private int nextNonNullPageIndex(int start) {
        for (int i = start; i < nullPages.size(); i++) {
            boolean isNullPage = nullPages.get(i).booleanValue();
            if (!isNullPage) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Computes the order relationship from page {@code prev} to page {@code next}. Equal min/max sequences across both
     * pages are treated as ASCENDING so a constant column doesn't degrade to UNORDERED. Crossing min/max (next.min &lt;
     * prev.max while next.max &gt; prev.max, etc.) yields {@link Direction#UNDEFINED}.
     */
    private Direction directionBetween(int prev, int next) {
        int minCmp = order.compare(minValues.get(prev), minValues.get(next));
        int maxCmp = order.compare(maxValues.get(prev), maxValues.get(next));
        if (minCmp <= 0 && maxCmp <= 0) {
            return Direction.ASCENDING;
        }
        if (minCmp >= 0 && maxCmp >= 0) {
            return Direction.DESCENDING;
        }
        return Direction.UNDEFINED;
    }

    /** The NaN counts of the pages of a floating-point column; no other column records them. */
    private Optional<List<Long>> nanCountOfEachPage() {
        if (nanCounts.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(List.copyOf(nanCounts));
    }

    /** Clears the accumulated page entries; the builder behaves as freshly constructed. */
    public void reset() {
        nullPages.clear();
        minValues.clear();
        maxValues.clear();
        nullCounts.clear();
        nanCounts.clear();
    }

    private enum Direction {
        ASCENDING,
        DESCENDING,
        UNDEFINED
    }
}
