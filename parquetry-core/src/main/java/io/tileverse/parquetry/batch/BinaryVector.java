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
 * Column vector for BYTE_ARRAY (variable-length binary) values, in one of two layouts.
 *
 * <p>Consolidated mode: the values share one read-only heap backing buffer; an {@code int[]} of length {@code size()+1}
 * delimits each row's bytes as {@code [offsets[i], offsets[i+1])}. Null cells (validity bit clear) are stored as
 * zero-length runs. {@link #get(int)} returns a read-only slice of the backing on demand.
 *
 * <p>Dictionary mode (low-cardinality columns): the distinct values live once in a shared {@code dictEntries} array,
 * and an {@code int[]} of per-row indexes selects an entry for each row. This holds {@code 4} bytes per row over the
 * shared entries instead of one slice per row. {@link #get(int)} returns the shared entry directly, with no slice.
 *
 * <p>The mode is chosen by which fields are populated: {@code indices != null} means dictionary mode. Either way the
 * returned segments are read-only and heap-owned, which outlives any decode Arena.
 */
public final class BinaryVector implements ColumnVector {

    private final MemorySegment backing;
    private final int[] offsets;
    private final MemorySegment[] dictEntries;
    private final int[] indices;
    private final BitSet validity;

    private BinaryVector(@NonNull MemorySegment backing, @NonNull int[] offsets, @NonNull BitSet validity) {
        this.backing = backing;
        this.offsets = offsets;
        this.dictEntries = null;
        this.indices = null;
        this.validity = validity;
    }

    private BinaryVector(@NonNull MemorySegment[] dictEntries, @NonNull int[] indices, @NonNull BitSet validity) {
        this.backing = null;
        this.offsets = null;
        this.dictEntries = dictEntries;
        this.indices = indices;
        this.validity = validity;
    }

    /** Builds a vector over a backing buffer and its row offsets ({@code offsets.length == values + 1}). */
    public static BinaryVector of(@NonNull MemorySegment backing, @NonNull int[] offsets, @NonNull BitSet validity) {
        return new BinaryVector(backing.asReadOnly(), offsets, validity);
    }

    /**
     * Consolidates per-value segments into a single backing buffer, dropping the per-value wrapper objects. Non-null
     * values are concatenated in row order; null rows become zero-length runs.
     */
    public static BinaryVector materialized(@NonNull MemorySegment[] values, @NonNull BitSet validity) {
        VariableLayout layout = consolidate(values);
        return new BinaryVector(layout.backing(), layout.offsets(), validity);
    }

    /** A read-only backing buffer paired with its row offsets ({@code offsets.length == rows + 1}). */
    @SuppressWarnings("java:S6218") // transient holder, never compared/hashed/printed; identity equality is fine
    public record VariableLayout(MemorySegment backing, int[] offsets) {}

    /**
     * Packs per-value segments into one read-only backing buffer plus row offsets. Non-null values are concatenated in
     * row order; a null row repeats the running offset, making it a zero-length run. Shared by the consolidating
     * factory and the page reader's freeze, which both turn a per-value segment array into this layout.
     */
    public static VariableLayout consolidate(MemorySegment[] values) {
        int[] offsets = new int[values.length + 1];
        long total = 0;
        for (MemorySegment value : values) {
            if (value != null) {
                total += value.byteSize();
            }
        }
        MemorySegment backing = MemorySegment.ofArray(new byte[Math.toIntExact(total)]);
        long cursor = 0;
        for (int i = 0; i < values.length; i++) {
            MemorySegment value = values[i];
            offsets[i] = Math.toIntExact(cursor);
            if (value != null) {
                long length = value.byteSize();
                MemorySegment.copy(value, 0L, backing, cursor, length);
                cursor += length;
            }
        }
        offsets[values.length] = Math.toIntExact(cursor);
        return new VariableLayout(backing.asReadOnly(), offsets);
    }

    /** Builds a dictionary-encoded vector: each row indexes a shared dictionary entry. */
    public static BinaryVector dictionary(
            @NonNull MemorySegment[] dictEntries, @NonNull int[] indices, @NonNull BitSet validity) {
        return new BinaryVector(dictEntries, indices, validity);
    }

    @Override
    public int size() {
        return indices != null ? indices.length : offsets.length - 1;
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    public MemorySegment get(int row) {
        if (indices != null) {
            return dictEntries[indices[row]];
        }
        int start = offsets[row];
        int length = offsets[row + 1] - start;
        return backing.asSlice(start, length);
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
        // Count only this vector's window into the shared page backing. Sibling slices each count their own window,
        // which keeps the page bytes from being multiplied across the batches the page was split into.
        long windowBytes = offsets[offsets.length - 1] - offsets[0];
        return windowBytes + (long) offsets.length * Integer.BYTES + ColumnVector.validityBytes(size());
    }

    private long dictionaryEntryBytes() {
        long total = 0;
        for (MemorySegment entry : dictEntries) {
            total += entry.byteSize();
        }
        return total;
    }
}
