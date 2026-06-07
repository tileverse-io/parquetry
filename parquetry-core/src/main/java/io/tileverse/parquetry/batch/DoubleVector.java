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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

public final class DoubleVector implements ColumnVector {

    private final double[] values;
    private final MemorySegment segmentValues;
    private final Validity validity;

    private DoubleVector(double[] values, MemorySegment segmentValues, @NonNull Validity validity) {
        this.values = values;
        this.segmentValues = segmentValues;
        this.validity = validity;
    }

    public static DoubleVector materialized(@NonNull double[] values, @NonNull Validity validity) {
        return new DoubleVector(values, null, validity);
    }

    /** Reads values from an off-heap little-endian segment; the segment's owner controls its lifetime. */
    public static DoubleVector segmentBacked(@NonNull MemorySegment segmentValues, @NonNull Validity validity) {
        return new DoubleVector(null, segmentValues, validity);
    }

    @Override
    public int size() {
        return segmentValues != null ? (int) (segmentValues.byteSize() / Double.BYTES) : values.length;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /**
     * Returns the value at {@code row}; throws {@link IllegalStateException} when the row is null. Guard with
     * {@link #isNull(int)} / {@link #hasNulls()}, or use {@link #get(int)} for a null-aware boxed read.
     */
    public double getDouble(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return segmentValues != null ? segmentValues.getAtIndex(DOUBLE, row) : values[row];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Double.valueOf(getDouble(row));
    }

    public double[] asArray() {
        if (segmentValues == null) {
            return values;
        }
        double[] out = new double[size()];
        MemorySegment.copy(segmentValues, DOUBLE, 0L, out, 0, out.length);
        return out;
    }

    /**
     * Copies {@code count} values starting at row {@code from} into {@code target} at byte {@code targetOffset}, in
     * little-endian Arrow layout. Lets a bulk consumer reuse one target instead of allocating via {@link #asArray()}.
     * Values at null rows are copied as stored; the caller applies validity separately, as with {@code asArray}.
     */
    public void copyInto(MemorySegment target, long targetOffset, int from, int count) {
        if (segmentValues != null) {
            MemorySegment.copy(segmentValues, DOUBLE, (long) from * Double.BYTES, target, DOUBLE, targetOffset, count);
        } else {
            MemorySegment.copy(values, from, target, DOUBLE, targetOffset, count);
        }
    }

    @Override
    public long approximateHeapBytes() {
        if (segmentValues != null) {
            return validity.heapBytes();
        }
        return (long) values.length * Long.BYTES + validity.heapBytes();
    }
}
