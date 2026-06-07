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
import java.lang.foreign.ValueLayout;

import lombok.NonNull;

public final class BooleanVector implements ColumnVector {

    private final boolean[] values; // null when segment-backed
    private final MemorySegment segmentBitmap; // null when heap-backed; LSB-first, one bit per row
    private final int segmentSize; // row count for the segment mode
    private final Validity validity;

    private BooleanVector(boolean[] values, MemorySegment segmentBitmap, int segmentSize, @NonNull Validity validity) {
        this.values = values;
        this.segmentBitmap = segmentBitmap;
        this.segmentSize = segmentSize;
        this.validity = validity;
    }

    public static BooleanVector materialized(@NonNull boolean[] values, @NonNull Validity validity) {
        return new BooleanVector(values, null, 0, validity);
    }

    /** Reads values from an off-heap LSB-first bit-packed segment; the segment's owner controls its lifetime. */
    public static BooleanVector segmentBacked(
            @NonNull MemorySegment segmentBitmap, int size, @NonNull Validity validity) {
        return new BooleanVector(null, segmentBitmap, size, validity);
    }

    @Override
    public int size() {
        return segmentBitmap != null ? segmentSize : values.length;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Returns the value at {@code row}; throws {@link IllegalStateException} when the row is null. Guard with
     * {@link #isNull(int)} / {@link #hasNulls()}, or use {@link #get(int)} for a null-aware boxed read.
     */
    public boolean getBoolean(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return segmentBitmap != null ? bitAt(row) : values[row];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Boolean.valueOf(getBoolean(row));
    }

    public boolean[] asArray() {
        if (segmentBitmap == null) {
            return values;
        }
        boolean[] out = new boolean[size()];
        for (int row = 0; row < out.length; row++) {
            out[row] = bitAt(row);
        }
        return out;
    }

    @Override
    public long approximateHeapBytes() {
        return segmentBitmap != null ? validity.heapBytes() : values.length + validity.heapBytes();
    }

    private boolean bitAt(int row) {
        int byteIndex = row >>> 3;
        int bitIndex = row & 7;
        int bits = segmentBitmap.get(ValueLayout.JAVA_BYTE, byteIndex) & 0xFF;
        return ((bits >>> bitIndex) & 1) != 0;
    }
}
