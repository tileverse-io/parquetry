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

public final class BooleanVector implements ColumnVector {

    private final boolean[] values; // null when segment-backed
    private final MemorySegment segmentBitmap; // null when heap-backed; LSB-first, one bit per row
    private final int segmentSize; // row count for the segment mode
    private final Validity validity;
    private final Selection selection;

    private BooleanVector(
            boolean[] values,
            MemorySegment segmentBitmap,
            int segmentSize,
            @NonNull Validity validity,
            @NonNull Selection selection) {
        this.values = values;
        this.segmentBitmap = segmentBitmap;
        this.segmentSize = segmentSize;
        this.validity = validity;
        this.selection = selection;
    }

    public static BooleanVector materialized(@NonNull boolean[] values, @NonNull Validity validity) {
        return new BooleanVector(values, null, 0, validity, Selection.ALL);
    }

    /** Reads values from an off-heap LSB-first bit-packed segment; the segment's owner controls its lifetime. */
    public static BooleanVector segmentBacked(
            @NonNull MemorySegment segmentBitmap, int size, @NonNull Validity validity) {
        return new BooleanVector(null, segmentBitmap, size, validity, Selection.ALL);
    }

    @Override
    public Selection selection() {
        return selection;
    }

    @Override
    public int baseSize() {
        return segmentBitmap != null ? segmentSize : values.length;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Returns the value at logical row {@code row}; throws {@link IllegalStateException} when the row is null. Guard
     * with {@link #isNull(int)} / {@link #hasNulls()}, or use {@link #get(int)} for a null-aware boxed read.
     * Sequential-access optimized on a selected view.
     */
    public boolean getBoolean(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        return bitValue(physical);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Boolean.valueOf(getBoolean(row));
    }

    public boolean[] asArray() {
        if (selection == Selection.ALL && segmentBitmap == null) {
            return values;
        }
        int n = size();
        boolean[] out = new boolean[n];
        for (int row = 0; row < n; row++) {
            int physical = selection == Selection.ALL ? row : selection.physical(row);
            out[row] = bitValue(physical);
        }
        return out;
    }

    /**
     * Packs {@code count} values starting at logical row {@code from} into {@code target} as an LSB-first bitmap
     * beginning at bit 0 of byte {@code targetOffset}. Lets a bulk consumer reuse one target instead of allocating via
     * {@link #asArray()}. A selected view packs its survivors. Values at null rows are packed as stored; the caller
     * applies validity separately.
     */
    public void copyInto(MemorySegment target, long targetOffset, int from, int count) {
        int byteCount = (count + 7) / 8;
        for (int b = 0; b < byteCount; b++) {
            target.set(ValueLayout.JAVA_BYTE, targetOffset + b, (byte) 0);
        }
        for (int i = 0; i < count; i++) {
            int physical = selection == Selection.ALL ? from + i : selection.physical(from + i);
            if (bitValue(physical)) {
                long byteIndex = targetOffset + (i >>> 3);
                int current = target.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xFF;
                target.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) (current | (1 << (i & 7))));
            }
        }
    }

    @Override
    public ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return new BooleanVector(values, segmentBitmap, segmentSize, validity.select(selection), selection);
    }

    @Override
    public long approximateHeapBytes() {
        return segmentBitmap != null ? validity.heapBytes() : values.length + validity.heapBytes();
    }

    private boolean bitValue(int physical) {
        return segmentBitmap != null ? bitAt(physical) : values[physical];
    }

    private boolean bitAt(int physical) {
        int byteIndex = physical >>> 3;
        int bitIndex = physical & 7;
        int bits = segmentBitmap.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xFF;
        return ((bits >>> bitIndex) & 1) != 0;
    }
}
