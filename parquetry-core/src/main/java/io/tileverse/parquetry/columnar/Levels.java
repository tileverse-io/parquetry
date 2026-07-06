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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;

/**
 * Immutable sequence of Parquet repetition or definition levels for one leaf column window. The backing is either a
 * heap {@code int[]} or an off-heap segment of 32-bit native-order ints; consumers never learn which. Like
 * {@link Validity}, the instance owns its representation: {@link #of(int[])} takes ownership of a freshly built array
 * no caller retains a mutable reference to, and no accessor exposes the backing.
 *
 * <p>{@link #get(int)} requires {@code 0 <= index < size()}; out-of-range indexes are not bounds-checked beyond the
 * backing's own limits on this hot path. Instances compare by identity.
 *
 * <p>The segment factory documents a lifetime, not an ownership transfer: the caller retains the segment and must keep
 * it valid (and unchanged) for the lifetime of this instance.
 */
public final class Levels {

    private final int[] heapLevels; // null == not heap-backed
    private final MemorySegment i32; // null == not segment-backed; 32-bit native-order ints, read unaligned
    private final int size;

    private Levels(int[] heapLevels, MemorySegment i32, int size) {
        this.heapLevels = heapLevels;
        this.i32 = i32;
        this.size = size;
    }

    /** Wraps a freshly built level array, taking ownership of it. */
    public static Levels of(int[] levels) {
        return new Levels(levels, null, levels.length);
    }

    /**
     * A view over {@code size} 32-bit native-order ints at the start of {@code i32}. The caller retains ownership of
     * the segment for the lifetime of this instance; the segment may be larger than {@code 4 * size} bytes.
     */
    public static Levels ofSegment(MemorySegment i32, int size) {
        return new Levels(null, i32, size);
    }

    /** Number of level entries. */
    public int size() {
        return size;
    }

    /** The level value at {@code index}. */
    public int get(int index) {
        if (heapLevels != null) {
            return heapLevels[index];
        }
        return i32.getAtIndex(ValueLayout.JAVA_INT_UNALIGNED, index);
    }

    /** Count of entries equal to {@code level}; for a rep-level stream, {@code countOf(0)} is the logical row count. */
    public int countOf(int level) {
        int count = 0;
        for (int i = 0; i < size; i++) {
            if (get(i) == level) {
                count++;
            }
        }
        return count;
    }

    /**
     * For a rep-level stream: the number of entries from {@code from} that cover the next {@code logicalRows} logical
     * rows, i.e. entries up to (excluding) the {@code logicalRows}-th row-start marker (level 0) after {@code from}.
     * When fewer rows remain, the whole tail counts. {@code from} must be a row boundary (index 0 or a level-0
     * position); for a mid-row {@code from} the count includes the tail of the current row.
     */
    public int valuesForRows(int from, int logicalRows) {
        int rowsCrossed = 0;
        int i = from;
        while (i < size) {
            if (get(i) == 0) {
                if (rowsCrossed == logicalRows) {
                    return i - from;
                }
                rowsCrossed++;
            }
            i++;
        }
        return size - from;
    }

    /** For a rep-level stream: the count of row-start markers (level 0) in {@code [from, from + count)}. */
    public int rowsInRange(int from, int count) {
        int rows = 0;
        int end = from + count;
        for (int i = from; i < end; i++) {
            if (get(i) == 0) {
                rows++;
            }
        }
        return rows;
    }

    /**
     * For a def-level stream: the per-entry null mask, present where the level equals {@code maxLevel}. An all-defined
     * stream collapses to the bitmap-free all-valid representation via {@link Validity#of}.
     */
    public Validity validityAt(int maxLevel) {
        BitSet validity = new BitSet(size);
        for (int i = 0; i < size; i++) {
            if (get(i) == maxLevel) {
                validity.set(i);
            }
        }
        return Validity.of(validity, size);
    }

    /**
     * The levels at positions {@code keep[0]..keep[keep.length - 1]}, reindexed to {@code [0, keep.length)};
     * heap-backed and owned.
     */
    public Levels gather(int[] keep) {
        int[] out = new int[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = get(keep[j]);
        }
        return of(out);
    }
}
