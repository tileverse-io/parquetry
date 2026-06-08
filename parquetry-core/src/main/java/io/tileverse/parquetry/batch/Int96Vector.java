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

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

/**
 * Column vector for INT96 (deprecated 12-byte timestamp) values, in one of two layouts. INT96 is a fixed 12-byte value.
 *
 * <p>Consolidated mode: the rows are stored full-slot in a single read-only backing buffer of {@code size() * 12}
 * bytes, with row {@code i} occupying the slot at {@code i * 12}. Null cells (validity bit clear) keep a zeroed slot;
 * the layout has no holes. {@link #get(int)} returns a read-only slice of the backing on demand. The decode path hands
 * out an off-heap (native) backing; materialized and dictionary-built vectors use a heap backing.
 *
 * <p>Dictionary mode (low-cardinality columns): the distinct values live once in a shared {@code dictEntries} array,
 * and an {@code int[]} of per-row indexes selects an entry for each row. This holds {@code 4} bytes per row over the
 * shared entries instead of one slice per row. {@link #get(int)} returns the shared entry directly, with no slice.
 *
 * <p>The mode is chosen by which fields are populated: {@code indices != null} means dictionary mode. Either way the
 * returned segments are read-only; a native consolidated backing is owned by the batch and released on its close.
 */
public final class Int96Vector implements ColumnVector {

    private static final int WIDTH = 12;

    private final MemorySegment backing;
    private final MemorySegment[] dictEntries;
    private final int[] indices;
    private final Validity validity;

    private Int96Vector(@NonNull MemorySegment backing, @NonNull Validity validity) {
        this.backing = backing;
        this.dictEntries = null;
        this.indices = null;
        this.validity = validity;
    }

    private Int96Vector(@NonNull MemorySegment[] dictEntries, @NonNull int[] indices, @NonNull Validity validity) {
        this.backing = null;
        this.dictEntries = dictEntries;
        this.indices = indices;
        this.validity = validity;
    }

    /** Builds a vector over a backing buffer whose length is an exact multiple of 12 bytes. */
    public static Int96Vector of(@NonNull MemorySegment backing, @NonNull Validity validity) {
        return new Int96Vector(backing.asReadOnly(), validity);
    }

    /**
     * Consolidates per-value segments into a single backing buffer, dropping the per-value wrapper objects. Each value
     * fills its 12-byte slot in row order; null rows leave their slot zeroed.
     */
    public static Int96Vector materialized(@NonNull MemorySegment[] values, @NonNull Validity validity) {
        return new Int96Vector(FixedLenBinaryVector.packFullSlots(values, WIDTH), validity);
    }

    /** Builds a dictionary-encoded vector: each row indexes a shared 12-byte dictionary entry. */
    public static Int96Vector dictionary(
            @NonNull MemorySegment[] dictEntries, @NonNull int[] indices, @NonNull Validity validity) {
        return new Int96Vector(dictEntries, indices, validity);
    }

    /**
     * Whether this vector is in dictionary mode (per-row indexes into shared entries) rather than consolidated mode.
     */
    public boolean isDictionary() {
        return indices != null;
    }

    /**
     * The read-only backing buffer of a consolidated-mode vector, holding {@code size() * 12} bytes with row {@code i}
     * at slot {@code i * 12}. The returned view is read-only; do not mutate it. Valid only when {@link #isDictionary()}
     * is {@code false}.
     */
    public MemorySegment consolidatedBacking() {
        return backing;
    }

    /**
     * The shared dictionary entries of a dictionary-mode vector, returned directly without copying; the array is
     * read-only by contract and callers must not mutate it, and the entries themselves are already read-only. Valid
     * only in dictionary mode.
     */
    public MemorySegment[] dictionaryEntries() {
        return dictEntries;
    }

    /**
     * The per-row indexes into {@link #dictionaryEntries()} of a dictionary-mode vector, returned directly without
     * copying; the array is read-only by contract and callers must not mutate it. Valid only in dictionary mode.
     */
    public int[] dictionaryIndices() {
        return indices;
    }

    @Override
    public int size() {
        return indices != null ? indices.length : Math.toIntExact(backing.byteSize() / WIDTH);
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Returns the value at {@code row}, or {@code null} when the row is null. A null row has no entry to read; an
     * all-null dictionary page has an empty entry array, and indexing it would throw.
     */
    @Override
    @SuppressWarnings("unchecked")
    public MemorySegment get(int row) {
        if (validity.isNull(row)) {
            return null;
        }
        if (indices != null) {
            return dictEntries[indices[row]];
        }
        return backing.asSlice((long) row * WIDTH, WIDTH);
    }

    /** Byte length of the value at {@code row}: {@code 12} for a present row, {@code -1} for a null row. */
    public int valueLength(int row) {
        if (validity.isNull(row)) {
            return -1;
        }
        return WIDTH;
    }

    /**
     * Copies the value at {@code row} into {@code target} starting at {@code targetOffset}, returning the byte count
     * written, or {@code -1} for a null row (nothing written). The caller reuses one target across rows by advancing
     * {@code targetOffset} by the returned count.
     */
    public int getInto(int row, MemorySegment target, long targetOffset) {
        if (validity.isNull(row)) {
            return -1;
        }
        if (indices != null) {
            MemorySegment entry = dictEntries[indices[row]];
            MemorySegment.copy(entry, 0L, target, targetOffset, WIDTH);
            return WIDTH;
        }
        MemorySegment.copy(backing, (long) row * WIDTH, target, targetOffset, WIDTH);
        return WIDTH;
    }

    @Override
    public long approximateHeapBytes() {
        if (indices != null) {
            return (long) indices.length * Integer.BYTES + dictionaryEntryBytes() + validity.heapBytes();
        }
        long backingBytes = backing.isNative() ? 0L : backing.byteSize();
        return backingBytes + validity.heapBytes();
    }

    private long dictionaryEntryBytes() {
        long total = 0;
        for (MemorySegment entry : dictEntries) {
            total += entry.byteSize();
        }
        return total;
    }
}
