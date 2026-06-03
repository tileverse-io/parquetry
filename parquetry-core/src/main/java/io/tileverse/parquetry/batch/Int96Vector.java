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
 * Column vector for INT96 (deprecated 12-byte timestamp) values, in one of two layouts. INT96 is a fixed 12-byte value.
 *
 * <p>Consolidated mode: the rows are stored full-slot in a single read-only heap backing buffer of {@code size() * 12}
 * bytes, with row {@code i} occupying the slot at {@code i * 12}. Null cells (validity bit clear) keep a zeroed slot;
 * the layout has no holes. {@link #get(int)} returns a read-only slice of the backing on demand.
 *
 * <p>Dictionary mode (low-cardinality columns): the distinct values live once in a shared {@code dictEntries} array,
 * and an {@code int[]} of per-row indexes selects an entry for each row. This holds {@code 4} bytes per row over the
 * shared entries instead of one slice per row. {@link #get(int)} returns the shared entry directly, with no slice.
 *
 * <p>The mode is chosen by which fields are populated: {@code indices != null} means dictionary mode. Either way the
 * returned segments are read-only and heap-owned, which outlives any decode Arena.
 */
public final class Int96Vector implements ColumnVector {

    private static final int WIDTH = 12;

    private final MemorySegment backing;
    private final MemorySegment[] dictEntries;
    private final int[] indices;
    private final BitSet validity;

    private Int96Vector(@NonNull MemorySegment backing, @NonNull BitSet validity) {
        this.backing = backing;
        this.dictEntries = null;
        this.indices = null;
        this.validity = validity;
    }

    private Int96Vector(@NonNull MemorySegment[] dictEntries, @NonNull int[] indices, @NonNull BitSet validity) {
        this.backing = null;
        this.dictEntries = dictEntries;
        this.indices = indices;
        this.validity = validity;
    }

    /** Builds a vector over a backing buffer whose length is an exact multiple of 12 bytes. */
    public static Int96Vector of(@NonNull MemorySegment backing, @NonNull BitSet validity) {
        return new Int96Vector(backing.asReadOnly(), validity);
    }

    /**
     * Consolidates per-value segments into a single backing buffer, dropping the per-value wrapper objects. Each value
     * fills its 12-byte slot in row order; null rows leave their slot zeroed.
     */
    public static Int96Vector materialized(@NonNull MemorySegment[] values, @NonNull BitSet validity) {
        return new Int96Vector(FixedLenBinaryVector.packFullSlots(values, WIDTH), validity);
    }

    /** Builds a dictionary-encoded vector: each row indexes a shared 12-byte dictionary entry. */
    public static Int96Vector dictionary(
            @NonNull MemorySegment[] dictEntries, @NonNull int[] indices, @NonNull BitSet validity) {
        return new Int96Vector(dictEntries, indices, validity);
    }

    @Override
    public int size() {
        return indices != null ? indices.length : Math.toIntExact(backing.byteSize() / WIDTH);
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    public MemorySegment get(int row) {
        if (indices != null) {
            return dictEntries[indices[row]];
        }
        return backing.asSlice((long) row * WIDTH, WIDTH);
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
