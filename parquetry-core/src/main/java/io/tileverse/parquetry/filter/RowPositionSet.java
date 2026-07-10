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

/**
 * A compact set of absolute row positions within a single data file.
 *
 * <p>This is the abstraction that keeps a concrete bitmap library out of the core. A consumer provides the
 * implementation: a sorted {@code long[]} for a positional delete file, or a roaring bitmap adapted behind this
 * interface for a deletion vector, without the rest of the engine depending on either.
 *
 * <p>All positions are absolute, zero-based row indices within the file. Ranges are half-open: a range {@code [lo,
 * hiExclusive)} covers every position {@code p} with {@code lo <= p < hiExclusive}.
 */
public interface RowPositionSet {

    /**
     * Tests whether the given absolute row position is a member of this set.
     *
     * @param pos the absolute, zero-based row position
     * @return {@code true} if {@code pos} is a member
     */
    boolean contains(long pos);

    /**
     * Tests whether any member lies in the half-open range {@code [lo, hiExclusive)}.
     *
     * @param lo the inclusive lower bound
     * @param hiExclusive the exclusive upper bound
     * @return {@code true} if at least one member {@code p} satisfies {@code lo <= p < hiExclusive}; {@code false} for
     *     an empty range ({@code lo >= hiExclusive})
     */
    boolean intersects(long lo, long hiExclusive);

    /**
     * Tests whether every position in the half-open range {@code [lo, hiExclusive)} is a member, that is, the range is
     * a contiguous, fully-present run.
     *
     * @param lo the inclusive lower bound
     * @param hiExclusive the exclusive upper bound
     * @return {@code true} if every position {@code p} with {@code lo <= p < hiExclusive} is a member; an empty range
     *     ({@code lo >= hiExclusive}) is vacuously fully contained and returns {@code true}
     */
    boolean containsRange(long lo, long hiExclusive);

    /**
     * Returns the number of members in this set.
     *
     * @return the member count
     */
    long cardinality();

    /**
     * Tests whether this set has no members.
     *
     * @return {@code true} if the cardinality is zero
     */
    boolean isEmpty();
}
