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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;

import java.lang.foreign.MemorySegment;

import lombok.NonNull;

/**
 * A column of {@code DOUBLE} values, backed either by a heap {@code double[]} ({@link Heap}) or by an off-heap
 * little-endian {@link MemorySegment} ({@link Segment}). The backing is chosen at construction and reached only through
 * the two implementations, which keeps the read accessors free of a per-row backing check. Selection, validity, and the
 * logical-to-physical translation are centralized in this interface; each implementation contributes only its backing
 * read ({@link #valueAt(int)}) and its contiguous bulk copy.
 */
public sealed interface DoubleVector extends ColumnVector permits DoubleVector.Heap, DoubleVector.Segment {

    /** A vector over heap-resident values, as produced by the assembly compaction lane and the Arrow packer. */
    static DoubleVector materialized(@NonNull double[] values, @NonNull Validity validity) {
        return new Heap(values, validity, Selection.ALL);
    }

    /** A vector that reads from an off-heap little-endian segment; the segment's owner controls its lifetime. */
    static DoubleVector segmentBacked(@NonNull MemorySegment segmentValues, @NonNull Validity validity) {
        return new Segment(segmentValues, validity, Selection.ALL);
    }

    /**
     * The stored value at backing index {@code physicalRow} WITHOUT consulting validity: a null row reads back its
     * parked placeholder (the decode contract fills null slots deterministically). The index is physical, not logical;
     * a caller on a selected view translates first via {@link #getDouble(int)} or {@link #copyInto}.
     */
    double valueAt(int physicalRow);

    /** Copies {@code count} backing values from {@code fromPhysical} into {@code target} at {@code targetOffset}. */
    void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count);

    /** This vector's backing exposed through {@code selection}; {@link Selection#ALL} returns it unchanged. */
    DoubleVector withSelection(Selection selection);

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
    default double getDouble(int row) {
        if (validity().isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return valueAt(physical(row));
    }

    @Override
    @SuppressWarnings("unchecked")
    default <T> T get(int row) {
        return validity().isNull(row) ? null : (T) Double.valueOf(getDouble(row));
    }

    /**
     * Copies {@code count} values starting at logical row {@code from} into {@code target} at byte
     * {@code targetOffset}, in little-endian Arrow layout. Lets a bulk consumer reuse one target instead of allocating
     * a fresh array. An unselected vector copies a contiguous run; a selected view scatters its survivors into the
     * contiguous destination. Values at null rows are copied as stored; the caller applies validity separately.
     */
    default void copyInto(MemorySegment target, long targetOffset, int from, int count) {
        Selection selection = selection();
        if (selection == Selection.ALL) {
            copyContiguous(target, targetOffset, from, count);
            return;
        }
        for (int i = 0; i < count; i++) {
            double value = valueAt(selection.physical(from + i));
            target.set(DOUBLE, targetOffset + (long) i * Double.BYTES, value);
        }
    }

    /** Values held in a heap {@code double[]}. */
    final class Heap implements DoubleVector {

        private final double[] values;
        private final Validity validity;
        private final Selection selection;

        private Heap(double[] values, @NonNull Validity validity, @NonNull Selection selection) {
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
        public double valueAt(int physicalRow) {
            return values[physicalRow];
        }

        @Override
        public void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count) {
            MemorySegment.copy(values, fromPhysical, target, DOUBLE, targetOffset, count);
        }

        @Override
        public DoubleVector withSelection(Selection selection) {
            return new Heap(values, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return (long) values.length * Double.BYTES + validity.heapBytes();
        }
    }

    /** Values read from an off-heap little-endian segment. */
    final class Segment implements DoubleVector {

        private final MemorySegment segmentValues;
        private final Validity validity;
        private final Selection selection;

        private Segment(MemorySegment segmentValues, @NonNull Validity validity, @NonNull Selection selection) {
            this.segmentValues = segmentValues;
            this.validity = validity;
            this.selection = selection;
        }

        @Override
        public Selection selection() {
            return selection;
        }

        @Override
        public int baseSize() {
            return (int) (segmentValues.byteSize() / Double.BYTES);
        }

        @Override
        public Validity validity() {
            return validity;
        }

        @Override
        public double valueAt(int physicalRow) {
            return segmentValues.getAtIndex(DOUBLE, physicalRow);
        }

        @Override
        public void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count) {
            MemorySegment.copy(
                    segmentValues, DOUBLE, (long) fromPhysical * Double.BYTES, target, DOUBLE, targetOffset, count);
        }

        @Override
        public DoubleVector withSelection(Selection selection) {
            return new Segment(segmentValues, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return validity.heapBytes();
        }
    }
}
