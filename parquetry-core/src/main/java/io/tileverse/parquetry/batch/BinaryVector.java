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
 * Column vector for BYTE_ARRAY (variable-length binary) values, in one of two layouts.
 *
 * <p>Consolidated mode: the values share one read-only backing buffer; an {@link IntSequence} of length
 * {@code size()+1} delimits each row's bytes as {@code [offsets.get(i), offsets.get(i+1))}. Null cells (validity bit
 * clear) are stored as zero-length runs; {@link #get(int)} consults validity and returns {@code null} for a null cell,
 * reserving a zero-length slice for a present-but-empty value. The decode path hands out an off-heap (native) backing;
 * materialized and spill-restored vectors use a heap backing.
 *
 * <p>Dictionary mode (low-cardinality columns): the distinct values live once in a shared {@code dictEntries} array,
 * and an {@link IntSequence} of per-row indexes selects an entry for each row. This holds {@code 4} bytes per row over
 * the shared entries instead of one slice per row. {@link #get(int)} returns the shared entry directly, with no slice.
 *
 * <p>The mode is chosen by which fields are populated: {@code indices != null} means dictionary mode. Either way the
 * returned segments are read-only; a native consolidated backing is owned by the batch and released on its close.
 */
public final class BinaryVector implements ColumnVector {

    private final MemorySegment backing;
    private final IntSequence offsets;
    private final MemorySegment[] dictEntries;
    private final IntSequence indices;
    private final Validity validity;

    private BinaryVector(@NonNull MemorySegment backing, @NonNull IntSequence offsets, @NonNull Validity validity) {
        this.backing = backing;
        this.offsets = offsets;
        this.dictEntries = null;
        this.indices = null;
        this.validity = validity;
    }

    private BinaryVector(
            @NonNull MemorySegment[] dictEntries, @NonNull IntSequence indices, @NonNull Validity validity) {
        this.backing = null;
        this.offsets = null;
        this.dictEntries = dictEntries;
        this.indices = indices;
        this.validity = validity;
    }

    /** Builds a vector over a backing buffer and its row offsets ({@code offsets.size() == values + 1}). */
    public static BinaryVector of(
            @NonNull MemorySegment backing, @NonNull IntSequence offsets, @NonNull Validity validity) {
        return new BinaryVector(backing.asReadOnly(), offsets, validity);
    }

    /**
     * Consolidates per-value segments into a single backing buffer, dropping the per-value wrapper objects. Non-null
     * values are concatenated in row order; null rows become zero-length runs.
     */
    public static BinaryVector materialized(@NonNull MemorySegment[] values, @NonNull Validity validity) {
        VariableLayout layout = consolidate(values);
        return new BinaryVector(layout.backing(), IntSequence.of(layout.offsets()), validity);
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
            @NonNull MemorySegment[] dictEntries, @NonNull IntSequence indices, @NonNull Validity validity) {
        return new BinaryVector(dictEntries, indices, validity);
    }

    /**
     * Whether this vector is in dictionary mode (per-row indexes into shared entries) rather than consolidated mode.
     */
    public boolean isDictionary() {
        return indices != null;
    }

    /**
     * The read-only backing buffer of a consolidated-mode vector. The returned view is read-only; do not mutate it.
     * Valid only when {@link #isDictionary()} is {@code false}.
     */
    public MemorySegment consolidatedBacking() {
        return backing;
    }

    /**
     * The row offsets of a consolidated-mode vector ({@code offsets.size() == size() + 1}). Offsets index absolutely
     * into {@link #consolidatedBacking()} and need not start at zero. Valid only when {@link #isDictionary()} is
     * {@code false}.
     */
    public IntSequence consolidatedOffsets() {
        return offsets;
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
        return indices != null ? indices.size() : offsets.size() - 1;
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
            return dictEntries[indices.get(row)];
        }
        int start = offsets.get(row);
        int length = offsets.get(row + 1) - start;
        return backing.asSlice(start, length);
    }

    /**
     * Byte length of the value at {@code row}, or {@code -1} for a null row (symmetric with {@link #getInto}); a
     * present empty value returns {@code 0}, which distinguishes it from null. For pre-sizing a target before getInto,
     * a caller sums only the non-negative lengths.
     */
    public int valueLength(int row) {
        if (validity.isNull(row)) {
            return -1;
        }
        if (indices != null) {
            return Math.toIntExact(dictEntries[indices.get(row)].byteSize());
        }
        return offsets.get(row + 1) - offsets.get(row);
    }

    /**
     * Copies the value at {@code row} into {@code target} starting at {@code targetOffset}, returning the byte count
     * written, or {@code -1} for a null row (nothing written). The caller sizes {@code target} (e.g. via
     * {@link #valueLength}) and may reuse one target across rows by advancing {@code targetOffset} by the returned
     * count.
     */
    public int getInto(int row, MemorySegment target, long targetOffset) {
        if (validity.isNull(row)) {
            return -1;
        }
        if (indices != null) {
            MemorySegment entry = dictEntries[indices.get(row)];
            int length = Math.toIntExact(entry.byteSize());
            MemorySegment.copy(entry, 0L, target, targetOffset, length);
            return length;
        }
        int start = offsets.get(row);
        int length = offsets.get(row + 1) - start;
        MemorySegment.copy(backing, start, target, targetOffset, length);
        return length;
    }

    @Override
    public long approximateHeapBytes() {
        if (indices != null) {
            return indices.heapBytes() + dictionaryEntryBytes() + validity.heapBytes();
        }
        // A native backing lives off-heap and is not counted. A heap backing counts only this vector's window into the
        // shared page backing; sibling slices each count their own window, which keeps the page bytes from being
        // multiplied across the batches the page was split into.
        long windowBytes = backing.isNative() ? 0L : (offsets.get(offsets.size() - 1) - offsets.get(0));
        return windowBytes + offsets.heapBytes() + validity.heapBytes();
    }

    private long dictionaryEntryBytes() {
        long total = 0;
        for (MemorySegment entry : dictEntries) {
            total += entry.byteSize();
        }
        return total;
    }
}
