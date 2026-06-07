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

import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

public final class FloatVector implements ColumnVector {

    private final float[] values;
    private final MemorySegment segmentValues;
    private final Validity validity;

    private FloatVector(float[] values, MemorySegment segmentValues, @NonNull Validity validity) {
        this.values = values;
        this.segmentValues = segmentValues;
        this.validity = validity;
    }

    public static FloatVector materialized(@NonNull float[] values, @NonNull Validity validity) {
        return new FloatVector(values, null, validity);
    }

    /** Reads values from an off-heap little-endian segment; the segment's owner controls its lifetime. */
    public static FloatVector segmentBacked(@NonNull MemorySegment segmentValues, @NonNull Validity validity) {
        return new FloatVector(null, segmentValues, validity);
    }

    @Override
    public int size() {
        return segmentValues != null ? (int) (segmentValues.byteSize() / Integer.BYTES) : values.length;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Returns the value at {@code row}; throws {@link IllegalStateException} when the row is null. Guard with
     * {@link #isNull(int)} / {@link #hasNulls()}, or use {@link #get(int)} for a null-aware boxed read.
     */
    public float getFloat(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return segmentValues != null ? segmentValues.getAtIndex(FLOAT, row) : values[row];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Float.valueOf(getFloat(row));
    }

    public float[] asArray() {
        if (segmentValues == null) {
            return values;
        }
        float[] out = new float[size()];
        MemorySegment.copy(segmentValues, FLOAT, 0L, out, 0, out.length);
        return out;
    }

    /**
     * Copies {@code count} values starting at row {@code from} into {@code target} at byte {@code targetOffset}, in
     * little-endian Arrow layout. Lets a bulk consumer reuse one target instead of allocating via {@link #asArray()}.
     * Values at null rows are copied as stored; the caller applies validity separately, as with {@code asArray}.
     */
    public void copyInto(MemorySegment target, long targetOffset, int from, int count) {
        if (segmentValues != null) {
            MemorySegment.copy(segmentValues, FLOAT, (long) from * Float.BYTES, target, FLOAT, targetOffset, count);
        } else {
            MemorySegment.copy(values, from, target, FLOAT, targetOffset, count);
        }
    }

    @Override
    public long approximateHeapBytes() {
        if (segmentValues != null) {
            return validity.heapBytes();
        }
        return (long) values.length * Integer.BYTES + validity.heapBytes();
    }
}
