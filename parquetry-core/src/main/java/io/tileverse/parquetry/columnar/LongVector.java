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

import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

public final class LongVector implements ColumnVector {

    private final long[] values;
    private final MemorySegment segmentValues;
    private final Validity validity;
    private final Selection selection;

    private LongVector(
            long[] values, MemorySegment segmentValues, @NonNull Validity validity, @NonNull Selection selection) {
        this.values = values;
        this.segmentValues = segmentValues;
        this.validity = validity;
        this.selection = selection;
    }

    public static LongVector materialized(@NonNull long[] values, @NonNull Validity validity) {
        return new LongVector(values, null, validity, Selection.ALL);
    }

    /** Reads values from an off-heap little-endian segment; the segment's owner controls its lifetime. */
    public static LongVector segmentBacked(@NonNull MemorySegment segmentValues, @NonNull Validity validity) {
        return new LongVector(null, segmentValues, validity, Selection.ALL);
    }

    @Override
    public Selection selection() {
        return selection;
    }

    @Override
    public int baseSize() {
        return segmentValues != null ? (int) (segmentValues.byteSize() / Long.BYTES) : values.length;
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
    public long getLong(int row) {
        if (validity.isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        return segmentValues != null ? segmentValues.getAtIndex(INT64, physical) : values[physical];
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int row) {
        return validity.isNull(row) ? null : (T) Long.valueOf(getLong(row));
    }

    public long[] asArray() {
        if (selection == Selection.ALL) {
            if (segmentValues == null) {
                return values;
            }
            long[] out = new long[baseSize()];
            MemorySegment.copy(segmentValues, INT64, 0L, out, 0, out.length);
            return out;
        }
        int n = size();
        long[] out = new long[n];
        for (int row = 0; row < n; row++) {
            int physical = selection.physical(row);
            out[row] = segmentValues != null ? segmentValues.getAtIndex(INT64, physical) : values[physical];
        }
        return out;
    }

    /**
     * Copies {@code count} values starting at logical row {@code from} into {@code target} at byte
     * {@code targetOffset}, in little-endian Arrow layout. Lets a bulk consumer reuse one target instead of allocating
     * via {@link #asArray()}. An unselected vector copies a contiguous run; a selected view scatters its survivors into
     * the contiguous destination. Values at null rows are copied as stored; the caller applies validity separately, as
     * with {@code asArray}.
     */
    public void copyInto(MemorySegment target, long targetOffset, int from, int count) {
        if (selection == Selection.ALL) {
            if (segmentValues != null) {
                MemorySegment.copy(segmentValues, INT64, (long) from * Long.BYTES, target, INT64, targetOffset, count);
            } else {
                MemorySegment.copy(values, from, target, INT64, targetOffset, count);
            }
            return;
        }
        for (int i = 0; i < count; i++) {
            int physical = selection.physical(from + i);
            long value = segmentValues != null ? segmentValues.getAtIndex(INT64, physical) : values[physical];
            target.set(INT64, targetOffset + (long) i * Long.BYTES, value);
        }
    }

    @Override
    public ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return new LongVector(values, segmentValues, validity.select(selection), selection);
    }

    @Override
    public long approximateHeapBytes() {
        if (segmentValues != null) {
            return validity.heapBytes();
        }
        return (long) values.length * Long.BYTES + validity.heapBytes();
    }
}
