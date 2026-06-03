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
import java.util.BitSet;

import lombok.NonNull;

/**
 * Column vector for FIXED_LEN_BYTE_ARRAY values, in one of two layouts.
 *
 * <p>Consolidated mode: the rows share one read-only heap backing buffer of {@code size() * byteWidth()} bytes, with
 * row {@code i} occupying the slot at {@code i * byteWidth}. Null cells (validity bit clear) keep a zeroed slot; the
 * layout has no holes. {@link #get(int)} returns a read-only slice of the backing on demand.
 *
 * <p>Dictionary mode (low-cardinality columns): the distinct values live once in a shared {@code dictEntries} array,
 * and an {@code int[]} of per-row indexes selects an entry for each row. This holds {@code 4} bytes per row over the
 * shared entries instead of one slice per row. {@link #get(int)} returns the shared entry directly, with no slice.
 *
 * <p>The mode is chosen by which fields are populated: {@code indices != null} means dictionary mode. Either way the
 * returned segments are read-only and heap-owned, which outlives any decode Arena.
 */
public final class FixedLenBinaryVector implements ColumnVector {

    private final MemorySegment backing;
    private final MemorySegment[] dictEntries;
    private final int[] indices;
    private final int byteWidth;
    private final BitSet validity;

    private FixedLenBinaryVector(@NonNull MemorySegment backing, int byteWidth, @NonNull BitSet validity) {
        this.backing = backing;
        this.dictEntries = null;
        this.indices = null;
        this.byteWidth = byteWidth;
        this.validity = validity;
    }

    private FixedLenBinaryVector(
            @NonNull MemorySegment[] dictEntries, @NonNull int[] indices, int byteWidth, @NonNull BitSet validity) {
        this.backing = null;
        this.dictEntries = dictEntries;
        this.indices = indices;
        this.byteWidth = byteWidth;
        this.validity = validity;
    }

    /** Builds a vector over a backing buffer whose length is an exact multiple of {@code byteWidth}. */
    public static FixedLenBinaryVector of(@NonNull MemorySegment backing, int byteWidth, @NonNull BitSet validity) {
        return new FixedLenBinaryVector(backing.asReadOnly(), byteWidth, validity);
    }

    /**
     * Consolidates per-value segments into a single backing buffer, dropping the per-value wrapper objects. Each value
     * fills its fixed-width slot in row order; null rows leave their slot zeroed.
     */
    public static FixedLenBinaryVector materialized(
            @NonNull MemorySegment[] values, int byteWidth, @NonNull BitSet validity) {
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
            @NonNull MemorySegment[] dictEntries, @NonNull int[] indices, int byteWidth, @NonNull BitSet validity) {
        return new FixedLenBinaryVector(dictEntries, indices, byteWidth, validity);
    }

    @Override
    public int size() {
        if (indices != null) {
            return indices.length;
        }
        return byteWidth == 0 ? 0 : Math.toIntExact(backing.byteSize() / byteWidth);
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    /** Fixed width in bytes per value, as declared in the column schema's {@code typeLength}. */
    public int byteWidth() {
        return byteWidth;
    }

    public MemorySegment get(int row) {
        if (indices != null) {
            return dictEntries[indices[row]];
        }
        return backing.asSlice((long) row * byteWidth, byteWidth);
    }

    @Override
    public Object getOrNull(int row) {
        return validity.get(row) ? get(row) : null;
    }

    @Override
    public long approximateHeapBytes() {
        if (indices != null) {
            return (long) indices.length * Integer.BYTES + dictionaryEntryBytes() + ColumnVector.validityBytes(size());
        }
        return backing.byteSize() + ColumnVector.validityBytes(size());
    }

    private long dictionaryEntryBytes() {
        long total = 0;
        for (MemorySegment entry : dictEntries) {
            total += entry.byteSize();
        }
        return total;
    }
}
