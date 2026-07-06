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
package io.tileverse.parquetry.internal.write;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * Per-row-group, per-column footer index builder.
 *
 * <p>Each call to {@link #appendPage(PageStatistics)} adds one data-page entry: a null-page flag, the page's typed
 * min/max (or {@link MemorySegment#NULL} when the page produced no ordered observation), and the per-page null count.
 * {@link #finishChunk()} assembles the {@link ColumnIndex} for the current accumulation window and clears state so the
 * builder can be reused for the next column chunk.
 *
 * <p>{@code boundary_order} is computed from the non-null-page sequence using the same ordering semantics the read-side
 * stats evaluator applies: signed numeric comparison for the integer and floating kinds, and unsigned lexicographic
 * byte comparison for {@code BYTE_ARRAY} / {@code FIXED_LEN_BYTE_ARRAY}, matching Parquet's default
 * {@code ColumnOrder}. Zero or one non-null pages default to {@link BoundaryOrder#UNORDERED} -- the safe value that
 * forces a linear scan on the read side.
 *
 * <p>Geometry and geography columns have no meaningful PLAIN-byte ordering, so pages of those columns always carry
 * {@link MemorySegment#NULL} min/max and the assembled boundary order falls through to {@code UNORDERED} regardless of
 * how many entries are appended.
 */
public final class ColumnIndexBuilder {

    private final PrimitiveKind kind;
    private final boolean tracksOrdering;

    private final List<Boolean> nullPages = new ArrayList<>();
    private final List<MemorySegment> minValues = new ArrayList<>();
    private final List<MemorySegment> maxValues = new ArrayList<>();
    private final List<Long> nullCounts = new ArrayList<>();

    /**
     * @param kind physical kind of the column whose pages this builder describes
     * @param logicalType logical type of the column; geometry/geography columns skip ordering computation since their
     *     PLAIN bytes carry no meaningful order. {@code null} keeps ordering enabled.
     */
    public ColumnIndexBuilder(@NonNull PrimitiveKind kind, LogicalType logicalType) {
        this.kind = kind;
        this.tracksOrdering = supportsOrdering(kind) && !isGeometryLike(logicalType);
    }

    private static boolean supportsOrdering(PrimitiveKind kind) {
        return kind != PrimitiveKind.INT96;
    }

    private static boolean isGeometryLike(LogicalType logicalType) {
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    /** Appends one page entry from the accumulator's per-page snapshot. */
    public void appendPage(@NonNull PageStatistics pageStats) {
        nullPages.add(pageStats.isNullPage());
        minValues.add(pageStats.min());
        maxValues.add(pageStats.max());
        nullCounts.add(pageStats.nullCount());
    }

    /**
     * Returns the {@link ColumnIndex} assembled from every {@link #appendPage} call since the last reset. Calling this
     * method also clears internal state so the builder can be reused for the next column chunk.
     */
    public ColumnIndex finishChunk() {
        BoundaryOrder order = tracksOrdering ? computeBoundaryOrder() : BoundaryOrder.UNORDERED;
        ColumnIndex index = new ColumnIndex(
                List.copyOf(nullPages),
                List.copyOf(minValues),
                List.copyOf(maxValues),
                order,
                Optional.of(List.copyOf(nullCounts)),
                Optional.empty(),
                Optional.empty());
        reset();
        return index;
    }

    /** Clears every accumulated page entry; the builder behaves as freshly constructed. */
    public void reset() {
        nullPages.clear();
        minValues.clear();
        maxValues.clear();
        nullCounts.clear();
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
            if (!isNullPage && hasOrderedBytes(i)) {
                return i;
            }
        }
        return -1;
    }

    private boolean hasOrderedBytes(int i) {
        return minValues.get(i) != MemorySegment.NULL && maxValues.get(i) != MemorySegment.NULL;
    }

    /**
     * Computes the order relationship from page {@code prev} to page {@code next}. Equal min/max sequences across both
     * pages are treated as ASCENDING so a constant column doesn't degrade to UNORDERED. Crossing min/max (next.min &lt;
     * prev.max while next.max &gt; prev.max, etc.) yields {@link Direction#UNDEFINED}.
     */
    private Direction directionBetween(int prev, int next) {
        int minCmp = compare(minValues.get(prev), minValues.get(next));
        int maxCmp = compare(maxValues.get(prev), maxValues.get(next));
        if (minCmp <= 0 && maxCmp <= 0) {
            return Direction.ASCENDING;
        }
        if (minCmp >= 0 && maxCmp >= 0) {
            return Direction.DESCENDING;
        }
        return Direction.UNDEFINED;
    }

    private int compare(MemorySegment a, MemorySegment b) {
        return switch (kind) {
            case BOOLEAN -> Boolean.compare(a.get(JAVA_BYTE, 0) != 0, b.get(JAVA_BYTE, 0) != 0);
            case INT32 -> Integer.compare(a.get(INT32, 0), b.get(INT32, 0));
            case INT64 -> Long.compare(a.get(INT64, 0), b.get(INT64, 0));
            case FLOAT -> Float.compare(a.get(FLOAT, 0), b.get(FLOAT, 0));
            case DOUBLE -> Double.compare(a.get(DOUBLE, 0), b.get(DOUBLE, 0));
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> compareBytes(a, b);
            case INT96 -> 0;
        };
    }

    /** Unsigned lexicographic byte comparison, mirroring Parquet's default ColumnOrder for binary columns. */
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

    private enum Direction {
        ASCENDING,
        DESCENDING,
        UNDEFINED
    }
}
