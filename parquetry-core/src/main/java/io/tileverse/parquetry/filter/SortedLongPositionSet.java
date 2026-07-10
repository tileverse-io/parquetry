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
package io.tileverse.parquetry.filter;

import java.util.Arrays;

/**
 * A {@link RowPositionSet} backed by a sorted, deduplicated {@code long[]} of absolute row positions.
 *
 * <p>This is the dependency-free default implementation: membership and range queries reduce to binary searches over
 * the backing array. It suits the dense-to-moderate position counts a single data file produces; a
 * roaring-bitmap-backed implementation can replace it for very large, sparse sets without changing callers.
 */
public final class SortedLongPositionSet implements RowPositionSet {

    private static final long[] EMPTY = new long[0];

    private final long[] positions;

    private SortedLongPositionSet(long[] sortedDistinctPositions) {
        this.positions = sortedDistinctPositions;
    }

    /**
     * Creates a set from the given positions. The input is defensively copied, sorted, and deduplicated; the caller may
     * reuse or mutate the supplied array afterwards.
     *
     * @param positions the absolute row positions, in any order, possibly with duplicates
     * @return a set over the distinct positions
     */
    public static SortedLongPositionSet of(long[] positions) {
        return new SortedLongPositionSet(sortedDistinctCopyOf(positions));
    }

    @Override
    public boolean contains(long pos) {
        return Arrays.binarySearch(positions, pos) >= 0;
    }

    @Override
    public boolean intersects(long lo, long hiExclusive) {
        if (isEmptyRange(lo, hiExclusive)) {
            return false;
        }
        int firstAtOrAfterLo = lowerBound(lo);
        return firstAtOrAfterLo < positions.length && positions[firstAtOrAfterLo] < hiExclusive;
    }

    @Override
    public boolean containsRange(long lo, long hiExclusive) {
        if (isEmptyRange(lo, hiExclusive)) {
            return true;
        }
        long expectedCount = hiExclusive - lo;
        int firstIndex = lowerBound(lo);
        int afterLastIndex = lowerBound(hiExclusive);
        long presentCount = (long) afterLastIndex - firstIndex;
        return presentCount == expectedCount;
    }

    @Override
    public long cardinality() {
        return positions.length;
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }

    private static boolean isEmptyRange(long lo, long hiExclusive) {
        return lo >= hiExclusive;
    }

    /**
     * Returns the index of the first member greater than or equal to {@code key}, or the array length if every member
     * is smaller. This is the standard lower-bound insertion point derived from {@link Arrays#binarySearch}.
     */
    private int lowerBound(long key) {
        int found = Arrays.binarySearch(positions, key);
        if (found >= 0) {
            return found;
        }
        return -(found + 1);
    }

    private static long[] sortedDistinctCopyOf(long[] positions) {
        if (positions.length == 0) {
            return EMPTY;
        }
        long[] sorted = positions.clone();
        Arrays.sort(sorted);
        return deduplicate(sorted);
    }

    private static long[] deduplicate(long[] sorted) {
        int writeIndex = 1;
        for (int readIndex = 1; readIndex < sorted.length; readIndex++) {
            if (sorted[readIndex] != sorted[writeIndex - 1]) {
                sorted[writeIndex] = sorted[readIndex];
                writeIndex++;
            }
        }
        if (writeIndex == sorted.length) {
            return sorted;
        }
        return Arrays.copyOf(sorted, writeIndex);
    }
}
