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
 * Column vector for FIXED_LEN_BYTE_ARRAY values, in one of two layouts.
 *
 * <p>Consolidated mode: the rows share one read-only backing buffer of {@code size() * byteWidth()} bytes, with row
 * {@code i} occupying the slot at {@code i * byteWidth}. Null cells (validity bit clear) keep a zeroed slot; the layout
 * has no holes. {@link #get(int)} returns a read-only slice of the backing on demand. The decode path hands out an
 * off-heap (native) backing; materialized and dictionary-built vectors use a heap backing.
 *
 * <p>Dictionary mode (low-cardinality columns): the distinct values live once in a shared {@code dictEntries} array,
 * and an {@link IntSequence} of per-row indexes selects an entry for each row. This holds {@code 4} bytes per row over
 * the shared entries instead of one slice per row. {@link #get(int)} returns the shared entry directly, with no slice.
 *
 * <p>The mode is chosen by which fields are populated: {@code indices != null} means dictionary mode. Either way the
 * returned segments are read-only; a native consolidated backing is owned by the batch and released on its close.
 */
public final class FixedLenBinaryVector implements ColumnVector {

    private final MemorySegment backing;
    private final MemorySegment[] dictEntries;
    private final IntSequence indices;
    private final int byteWidth;
    private final Validity validity;

    private FixedLenBinaryVector(@NonNull MemorySegment backing, int byteWidth, @NonNull Validity validity) {
        this.backing = backing;
        this.dictEntries = null;
        this.indices = null;
        this.byteWidth = byteWidth;
        this.validity = validity;
    }

    private FixedLenBinaryVector(
            @NonNull MemorySegment[] dictEntries,
            @NonNull IntSequence indices,
            int byteWidth,
            @NonNull Validity validity) {
        this.backing = null;
        this.dictEntries = dictEntries;
        this.indices = indices;
        this.byteWidth = byteWidth;
        this.validity = validity;
    }

    /** Builds a vector over a backing buffer whose length is an exact multiple of {@code byteWidth}. */
    public static FixedLenBinaryVector of(@NonNull MemorySegment backing, int byteWidth, @NonNull Validity validity) {
        return new FixedLenBinaryVector(backing.asReadOnly(), byteWidth, validity);
    }

    /**
     * Consolidates per-value segments into a single backing buffer, dropping the per-value wrapper objects. Each value
     * fills its fixed-width slot in row order; null rows leave their slot zeroed.
     */
    public static FixedLenBinaryVector materialized(
            @NonNull MemorySegment[] values, int byteWidth, @NonNull Validity validity) {
        return new FixedLenBinaryVector(packFullSlots(values, byteWidth), byteWidth, validity);
    }

    /**
     * Packs per-value segments into one read-only backing of {@code values.length * byteWidth} bytes, each value in its
     * row slot, null rows left zeroed. Shared by the consolidating factory, {@link Int96Vector}, and the page reader's
     * freeze, which all turn a fixed-width per-value segment array into this full-slot layout.
     */
    public static MemorySegment packFullSlots(MemorySegment[] values, int byteWidth) {
        MemorySegment backing = MemorySegment.ofArray(new byte[Math.multiplyExact(values.length, byteWidth)]);
        for (int i = 0; i < values.length; i++) {
            MemorySegment value = values[i];
            if (value != null) {
                MemorySegment.copy(value, 0L, backing, (long) i * byteWidth, byteWidth);
            }
        }
        return backing.asReadOnly();
    }

    /** Builds a dictionary-encoded vector: each row indexes a shared fixed-width dictionary entry. */
    public static FixedLenBinaryVector dictionary(
            @NonNull MemorySegment[] dictEntries,
            @NonNull IntSequence indices,
            int byteWidth,
            @NonNull Validity validity) {
        return new FixedLenBinaryVector(dictEntries, indices, byteWidth, validity);
    }

    /**
     * Whether this vector is in dictionary mode (per-row indexes into shared entries) rather than consolidated mode.
     */
    public boolean isDictionary() {
        return indices != null;
    }

    /**
     * The read-only backing buffer of a consolidated-mode vector, holding {@code size() * byteWidth()} bytes with row
     * {@code i} at slot {@code i * byteWidth}. The returned view is read-only; do not mutate it. Valid only when
     * {@link #isDictionary()} is {@code false}.
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
     * The per-row indexes into {@link #dictionaryEntries()} of a dictionary-mode vector. Valid only in dictionary mode.
     */
    public IntSequence dictionaryIndices() {
        return indices;
    }

    @Override
    public int size() {
        if (indices != null) {
            return indices.size();
        }
        return byteWidth == 0 ? 0 : Math.toIntExact(backing.byteSize() / byteWidth);
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /** Fixed width in bytes per value, as declared in the column schema's {@code typeLength}. */
    public int byteWidth() {
        return byteWidth;
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
            return dictEntries[indices.get(row)];
        }
        return backing.asSlice((long) row * byteWidth, byteWidth);
    }

    /** Byte length of the value at {@code row}: the fixed width for a present row, {@code -1} for a null row. */
    public int valueLength(int row) {
        if (validity.isNull(row)) {
            return -1;
        }
        return byteWidth;
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
            MemorySegment entry = dictEntries[indices.get(row)];
            MemorySegment.copy(entry, 0L, target, targetOffset, byteWidth);
            return byteWidth;
        }
        MemorySegment.copy(backing, (long) row * byteWidth, target, targetOffset, byteWidth);
        return byteWidth;
    }

    @Override
    public long approximateHeapBytes() {
        if (indices != null) {
            return indices.heapBytes() + dictionaryEntryBytes() + validity.heapBytes();
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
