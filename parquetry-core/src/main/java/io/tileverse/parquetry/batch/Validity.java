/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.batch;

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

    private final BitSet validBits; // null == all rows valid
    private final int size;

    private Validity(BitSet validBits, int size) {
        this.validBits = validBits;
        this.size = size;
    }

    /** Every row valid; allocates no bitmap. */
    public static Validity allValid(int size) {
        return new Validity(null, size);
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
            return new Validity(null, size);
        }
        return new Validity(validBits, size);
    }

    /** Logical row count this mask covers. */
    public int size() {
        return size;
    }

    /** Whether row {@code row} is non-null. */
    public boolean isValid(int row) {
        return validBits == null || validBits.get(row);
    }

    /** Whether row {@code row} is null. */
    public boolean isNull(int row) {
        return !isValid(row);
    }

    /** Whether any row is null. O(1): the all-valid representation answers without scanning. */
    public boolean hasNulls() {
        return validBits != null;
    }

    /** Number of null rows. */
    public int nullCount() {
        return validBits == null ? 0 : size - validBits.cardinality();
    }

    /** Index of the next valid row at or after {@code fromRow}, or {@code -1} when none remains. */
    public int nextSetBit(int fromRow) {
        if (validBits == null) {
            return fromRow < size ? fromRow : -1;
        }
        return validBits.nextSetBit(fromRow);
    }

    /** Count of valid rows. */
    public int cardinality() {
        return validBits == null ? size : validBits.cardinality();
    }

    /** A fresh mutable copy of the valid-bit mask, independent of this instance. */
    public BitSet copy() {
        BitSet out = new BitSet(size);
        if (validBits == null) {
            out.set(0, size);
        } else {
            out.or(validBits);
        }
        return out;
    }

    /** Approximate heap bytes this mask holds; the all-valid representation holds none. */
    public long heapBytes() {
        return validBits == null ? 0L : (long) size / Byte.SIZE + 1L;
    }
}
