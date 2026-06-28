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
package io.tileverse.parquetry.columnar;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import lombok.NonNull;

/**
 * A column of {@code BOOLEAN} values, backed either by a heap {@code boolean[]} ({@link Heap}) or by an off-heap
 * LSB-first bit-packed {@link MemorySegment} ({@link Segment}). The backing is chosen at construction and reached only
 * through the two implementations, which keeps the read accessors free of a per-row backing check. Selection, validity,
 * and the logical-to-physical translation are centralized in this interface; each implementation contributes only its
 * backing read ({@link #valueAt(int)}).
 */
public sealed interface BooleanVector extends ColumnVector permits BooleanVector.Heap, BooleanVector.Segment {

    /** A vector over heap-resident values, as produced by the assembly compaction lane and the Arrow packer. */
    static BooleanVector materialized(@NonNull boolean[] values, @NonNull Validity validity) {
        return new Heap(values, validity, Selection.ALL);
    }

    /** A vector that reads from an off-heap LSB-first bit-packed segment; the segment's owner controls its lifetime. */
    static BooleanVector segmentBacked(@NonNull MemorySegment segmentBitmap, int size, @NonNull Validity validity) {
        return new Segment(segmentBitmap, size, validity, Selection.ALL);
    }

    /**
     * The stored value at backing index {@code physicalRow} WITHOUT consulting validity: a null row reads back its
     * parked placeholder (the decode contract fills null slots deterministically). The index is physical, not logical;
     * a caller on a selected view translates first via {@link #getBoolean(int)} or {@link #copyInto}.
     */
    boolean valueAt(int physicalRow);

    /** This vector's backing exposed through {@code selection}; {@link Selection#ALL} returns it unchanged. */
    BooleanVector withSelection(Selection selection);

    @Override
    default ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return withSelection(selection);
    }

    private int physical(int row) {
        Selection selection = selection();
        return selection == Selection.ALL ? row : selection.physical(row);
    }

    /**
     * Returns the value at logical row {@code row}; throws {@link IllegalStateException} when the row is null. Guard
     * with {@link #isNull(int)} / {@link #hasNulls()}, or use {@link #get(int)} for a null-aware boxed read.
     */
    default boolean getBoolean(int row) {
        if (validity().isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return valueAt(physical(row));
    }

    @Override
    @SuppressWarnings("unchecked")
    default <T> T get(int row) {
        return validity().isNull(row) ? null : (T) Boolean.valueOf(getBoolean(row));
    }

    default boolean[] asArray() {
        int n = size();
        boolean[] out = new boolean[n];
        for (int row = 0; row < n; row++) {
            out[row] = valueAt(physical(row));
        }
        return out;
    }

    /**
     * Packs {@code count} values starting at logical row {@code from} into {@code target} as an LSB-first bitmap
     * beginning at bit 0 of byte {@code targetOffset}. Lets a bulk consumer reuse one target instead of allocating via
     * {@link #asArray()}. A selected view packs its survivors. Values at null rows are packed as stored; the caller
     * applies validity separately.
     */
    default void copyInto(MemorySegment target, long targetOffset, int from, int count) {
        int byteCount = (count + 7) / 8;
        for (int b = 0; b < byteCount; b++) {
            target.set(ValueLayout.JAVA_BYTE, targetOffset + b, (byte) 0);
        }
        for (int i = 0; i < count; i++) {
            if (valueAt(physical(from + i))) {
                long byteIndex = targetOffset + (i >>> 3);
                int current = target.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xFF;
                target.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (current | (1 << (i & 7))));
            }
        }
    }

    /** Values held in a heap {@code boolean[]}. */
    final class Heap implements BooleanVector {

        private final boolean[] values;
        private final Validity validity;
        private final Selection selection;

        private Heap(boolean[] values, @NonNull Validity validity, @NonNull Selection selection) {
            this.values = values;
            this.validity = validity;
            this.selection = selection;
        }

        @Override
        public Selection selection() {
            return selection;
        }

        @Override
        public int baseSize() {
            return values.length;
        }

        @Override
        public Validity validity() {
            return validity;
        }

        @Override
        public boolean valueAt(int physicalRow) {
            return values[physicalRow];
        }

        @Override
        public boolean[] asArray() {
            if (selection() == Selection.ALL) {
                return values;
            }
            return BooleanVector.super.asArray();
        }

        @Override
        public BooleanVector withSelection(Selection selection) {
            return new Heap(values, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return values.length + validity.heapBytes();
        }
    }

    /** Values read from an off-heap LSB-first bit-packed segment. */
    final class Segment implements BooleanVector {

        private final MemorySegment segmentBitmap;
        private final int segmentSize;
        private final Validity validity;
        private final Selection selection;

        private Segment(
                MemorySegment segmentBitmap,
                int segmentSize,
                @NonNull Validity validity,
                @NonNull Selection selection) {
            this.segmentBitmap = segmentBitmap;
            this.segmentSize = segmentSize;
            this.validity = validity;
            this.selection = selection;
        }

        @Override
        public Selection selection() {
            return selection;
        }

        @Override
        public int baseSize() {
            return segmentSize;
        }

        @Override
        public Validity validity() {
            return validity;
        }

        @Override
        public boolean valueAt(int physicalRow) {
            int byteIndex = physicalRow >>> 3;
            int bitIndex = physicalRow & 7;
            int bits = segmentBitmap.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xFF;
            return ((bits >>> bitIndex) & 1) != 0;
        }

        @Override
        public BooleanVector withSelection(Selection selection) {
            return new Segment(segmentBitmap, segmentSize, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return validity.heapBytes();
        }
    }
}
