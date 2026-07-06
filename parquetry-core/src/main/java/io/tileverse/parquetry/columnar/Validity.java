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
 * Immutable per-row null mask for a {@link ColumnVector}: row {@code i} is either valid (non-null) or null. A null-free
 * column uses an all-valid representation that allocates no bitmap, which keeps {@link #hasNulls()} free and lets the
 * common {@code required}-column path hold no validity state at all.
 *
 * <p>The instance owns its bitmap: factories take a freshly built {@link BitSet} that no caller retains a mutable
 * reference to, and the only way back to a mutable mask is {@link #copy()}. There is no accessor that exposes the
 * internal bitmap, which is what makes the mask un-drift-able after construction.
 *
 * <p>The per-row accessors {@link #isValid(int)}, {@link #isNull(int)}, and {@link #nextSetBit(int)} require {@code 0
 * <= row < size()}; out-of-range rows are not bounds-checked on this hot path.
 */
public final class Validity {

    private final BitSet validBits; // null == no heap bitmap
    private final MemorySegment validBitmap; // null == no off-heap bitmap; LSB-first, bit set == valid
    private final int nullCount;
    private final int size;

    private Validity(BitSet validBits, MemorySegment validBitmap, int nullCount, int size) {
        this.validBits = validBits;
        this.validBitmap = validBitmap;
        this.nullCount = nullCount;
        this.size = size;
    }

    /** Every row valid; allocates no bitmap. */
    public static Validity allValid(int size) {
        return new Validity(null, null, 0, size);
    }

    /**
     * Wraps a freshly built mask where bit {@code i} set means row {@code i} is valid. Takes ownership of
     * {@code validBits}. Collapses to the all-valid representation when no row in {@code [0, size)} is clear, dropping
     * the bitmap.
     */
    public static Validity of(BitSet validBits, int size) {
        if (validBits.length() > size) {
            validBits.clear(size, validBits.length());
        }
        boolean everyRowValid = validBits.nextClearBit(0) >= size;
        if (everyRowValid) {
            return new Validity(null, null, 0, size);
        }
        return new Validity(validBits, null, size - validBits.cardinality(), size);
    }

    /**
     * A validity mask reading an off-heap LSB-first Arrow bitmap (bit set == valid). The caller retains ownership of
     * the segment for the lifetime of this mask.
     */
    public static Validity ofSegment(MemorySegment validBitmap, int nullCount, int size) {
        if (nullCount == 0) {
            return new Validity(null, null, 0, size);
        }
        return new Validity(null, validBitmap, nullCount, size);
    }

    /** Logical row count this mask covers. */
    public int size() {
        return size;
    }

    /** Whether row {@code row} is non-null. */
    public boolean isValid(int row) {
        if (validBitmap != null) {
            int byteIndex = row >>> 3;
            int bitIndex = row & 7;
            int bits = validBitmap.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xFF;
            return ((bits >>> bitIndex) & 1) != 0;
        }
        return validBits == null || validBits.get(row);
    }

    /** Whether row {@code row} is null. */
    public boolean isNull(int row) {
        return !isValid(row);
    }

    /** Whether any row is null. O(1): the all-valid representation answers without scanning. */
    public boolean hasNulls() {
        return nullCount > 0;
    }

    /** Number of null rows. */
    public int nullCount() {
        return nullCount;
    }

    /** Index of the next valid row at or after {@code fromRow}, or {@code -1} when none remains. */
    public int nextSetBit(int fromRow) {
        if (validBitmap != null) {
            for (int row = fromRow; row < size; row++) {
                if (isValid(row)) {
                    return row;
                }
            }
            return -1;
        }
        if (validBits == null) {
            return fromRow < size ? fromRow : -1;
        }
        return validBits.nextSetBit(fromRow);
    }

    /** Count of valid rows. */
    public int cardinality() {
        return size - nullCount;
    }

    /**
     * The mask for rows {@code [start, start + n)} of this mask, rebased to row zero and owning its own bitmap. A range
     * with no null row collapses to the all-valid representation. The off-heap representation materializes the slice
     * into a heap {@link BitSet}; a caller that owns the underlying segment can copy bytes off-heap instead.
     */
    public Validity slice(int start, int n) {
        if (nullCount == 0) {
            return allValid(n);
        }
        if (validBits != null) {
            return of(validBits.get(start, start + n), n);
        }
        BitSet out = new BitSet(n);
        for (int i = 0; i < n; i++) {
            if (isValid(start + i)) {
                out.set(i);
            }
        }
        return of(out, n);
    }

    /**
     * The mask projected through {@code selection}: logical row {@code j} reads this mask at
     * {@code selection.physical(j)}, rebased to a logical domain of {@code selection.length()} rows and owning its own
     * bitmap. The generalization of {@link #slice(int, int)} from a contiguous window to an arbitrary index map.
     *
     * <p>This materializes the projected mask (the sibling behavior of {@code slice}); it does not store the selection.
     * The result is a plain logical-indexed mask, which is what keeps {@link Selection} the single index-translation
     * layer: validity stays <i>data</i> and is read <i>through</i> the selection, never as a second selection.
     * {@link Selection#ALL} returns this mask unchanged.
     */
    public Validity select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        int length = selection.length();
        if (nullCount == 0) {
            return allValid(length);
        }
        BitSet out = new BitSet(length);
        for (int logical = 0; logical < length; logical++) {
            if (isValid(selection.physical(logical))) {
                out.set(logical);
            }
        }
        return of(out, length);
    }

    /** A fresh mutable copy of the valid-bit mask, independent of this instance. */
    public BitSet copy() {
        BitSet out = new BitSet(size);
        if (validBitmap != null) {
            for (int row = 0; row < size; row++) {
                if (isValid(row)) {
                    out.set(row);
                }
            }
        } else if (validBits == null) {
            out.set(0, size);
        } else {
            out.or(validBits);
        }
        return out;
    }

    /** Approximate heap bytes this mask holds; the all-valid and off-heap representations hold none. */
    public long heapBytes() {
        return validBits == null ? 0L : (long) size / Byte.SIZE + 1L;
    }
}
