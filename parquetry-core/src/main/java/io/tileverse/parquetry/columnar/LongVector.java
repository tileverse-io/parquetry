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

/**
 * A column of {@code INT64} values, backed by a heap {@code long[]} ({@link Heap}), an off-heap little-endian
 * {@link MemorySegment} ({@link Segment}), or the synthesized per-file row position computed without a backing array
 * ({@link RowPositions}). The backing is chosen at construction and reached only through the implementations, which
 * keeps the read accessors free of a per-row backing check. Selection, validity, and the logical-to-physical
 * translation are centralized in this interface; each implementation contributes only its backing read
 * ({@link #valueAt(int)}) and its contiguous bulk copy.
 */
public sealed interface LongVector extends ColumnVector
        permits LongVector.Heap, LongVector.Segment, LongVector.RowPositions, LongVector.Coalesced {

    /** A vector over heap-resident values, as produced by the assembly compaction lane and the Arrow packer. */
    static LongVector materialized(@NonNull long[] values, @NonNull Validity validity) {
        return new Heap(values, validity, Selection.ALL);
    }

    /** A vector that reads from an off-heap little-endian segment; the segment's owner controls its lifetime. */
    static LongVector segmentBacked(@NonNull MemorySegment segmentValues, @NonNull Validity validity) {
        return new Segment(segmentValues, validity, Selection.ALL);
    }

    /**
     * The synthesized absolute row-position column over {@code length} decoded rows: logical row {@code j} reports
     * {@code base + positionMap.physical(j)}, the row's 0-based position within the data file. {@code base} is the row
     * group's file row offset (the sum of {@code num_rows} of all prior row groups, pruned ones included) and
     * {@code positionMap} maps each decoded row to its row-group-relative physical position; {@link Selection#ALL}
     * means the decoded rows are the full row group in order. Computed lazily, with no backing array.
     */
    static LongVector rowPositions(long base, @NonNull Selection positionMap, int length) {
        if (length < 0) {
            throw new IllegalArgumentException("length must be >= 0, got " + length);
        }
        return new RowPositions(base, positionMap, length, Validity.allValid(length), Selection.ALL);
    }

    /**
     * A per-row coalesce of a physical column with a fallback: logical row {@code j} reports the physical value when
     * the physical cell is non-null, otherwise the fallback's value at {@code j}. The result has no nulls (the fallback
     * is always valid). The two inputs must expose the same logical rows in the same order; the fallback is the
     * synthesized row-position column (an Iceberg {@code _row_id}) or a constant (an Iceberg
     * {@code _last_updated_sequence_number}).
     */
    static LongVector coalesced(@NonNull LongVector physical, @NonNull LongVector fallback) {
        return new Coalesced(physical, fallback);
    }

    /**
     * The stored value at backing index {@code physicalRow} WITHOUT consulting validity: a null row reads back its
     * parked placeholder (the decode contract fills null slots deterministically). The index is physical, not logical;
     * a caller on a selected view translates first via {@link #getLong(int)} or {@link #copyInto}.
     */
    long valueAt(int physicalRow);

    /** Copies {@code count} backing values from {@code fromPhysical} into {@code target} at {@code targetOffset}. */
    void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count);

    /** This vector's backing exposed through {@code selection}; {@link Selection#ALL} returns it unchanged. */
    LongVector withSelection(Selection selection);

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
    default long getLong(int row) {
        if (validity().isNull(row)) {
            throw new IllegalStateException("row %d is null; guard with isNull(row) or hasNulls()".formatted(row));
        }
        return valueAt(physical(row));
    }

    @Override
    @SuppressWarnings("unchecked")
    default <T> T get(int row) {
        return validity().isNull(row) ? null : (T) Long.valueOf(getLong(row));
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
            long value = valueAt(selection.physical(from + i));
            target.set(INT64, targetOffset + (long) i * Long.BYTES, value);
        }
    }

    /** Values held in a heap {@code long[]}. */
    final class Heap implements LongVector {

        private final long[] values;
        private final Validity validity;
        private final Selection selection;

        private Heap(long[] values, @NonNull Validity validity, @NonNull Selection selection) {
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
        public long valueAt(int physicalRow) {
            return values[physicalRow];
        }

        @Override
        public void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count) {
            MemorySegment.copy(values, fromPhysical, target, INT64, targetOffset, count);
        }

        @Override
        public LongVector withSelection(Selection selection) {
            return new Heap(values, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return (long) values.length * Long.BYTES + validity.heapBytes();
        }
    }

    /** Values read from an off-heap little-endian segment. */
    final class Segment implements LongVector {

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
            return (int) (segmentValues.byteSize() / Long.BYTES);
        }

        @Override
        public Validity validity() {
            return validity;
        }

        @Override
        public long valueAt(int physicalRow) {
            return segmentValues.getAtIndex(INT64, physicalRow);
        }

        @Override
        public void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count) {
            MemorySegment.copy(
                    segmentValues, INT64, (long) fromPhysical * Long.BYTES, target, INT64, targetOffset, count);
        }

        @Override
        public LongVector withSelection(Selection selection) {
            return new Segment(segmentValues, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return validity.heapBytes();
        }
    }

    /**
     * The synthesized per-file absolute row position: backing index {@code physicalRow} reads {@code base +
     * positionMap.physical(physicalRow)}. Allocates no array; the position is computed from the page-skip survivor map
     * held by reference, which keeps the value TRUE under column-index page-skipping (a surviving row reports the
     * position it holds in the file, not a compacted index). Always non-null.
     */
    final class RowPositions implements LongVector {

        private final long base;
        private final Selection positionMap;
        private final int length;
        private final Validity validity;
        private final Selection selection;

        private RowPositions(
                long base,
                Selection positionMap,
                int length,
                @NonNull Validity validity,
                @NonNull Selection selection) {
            this.base = base;
            this.positionMap = positionMap;
            this.length = length;
            this.validity = validity;
            this.selection = selection;
        }

        @Override
        public Selection selection() {
            return selection;
        }

        @Override
        public int baseSize() {
            return length;
        }

        @Override
        public Validity validity() {
            return validity;
        }

        @Override
        public long valueAt(int physicalRow) {
            long relative = positionMap == Selection.ALL ? physicalRow : positionMap.physical(physicalRow);
            return base + relative;
        }

        @Override
        public void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count) {
            for (int i = 0; i < count; i++) {
                target.set(INT64, targetOffset + (long) i * Long.BYTES, valueAt(fromPhysical + i));
            }
        }

        @Override
        public LongVector withSelection(Selection selection) {
            return new RowPositions(base, positionMap, length, validity.select(selection), selection);
        }

        @Override
        public long approximateHeapBytes() {
            return validity.heapBytes();
        }
    }

    /**
     * A per-row coalesce: logical row {@code j} reports the {@code physical} value when that cell is non-null,
     * otherwise the {@code fallback} value at {@code j}. Holds no backing array and no selection of its own; it
     * delegates to the two inputs' logical accessors and re-selects both on {@link #withSelection}, which keeps it
     * aligned to the batch's rows through record-level filtering and column-index page-skipping. Always non-null: the
     * fallback fills every cell the physical column leaves null.
     */
    final class Coalesced implements LongVector {

        private final LongVector physical;
        private final LongVector fallback;
        private final Validity validity;

        private Coalesced(@NonNull LongVector physical, @NonNull LongVector fallback) {
            if (physical.size() != fallback.size()) {
                throw new IllegalArgumentException("coalesce inputs expose different row counts: %d and %d"
                        .formatted(physical.size(), fallback.size()));
            }
            this.physical = physical;
            this.fallback = fallback;
            this.validity = Validity.allValid(physical.size());
        }

        @Override
        public int baseSize() {
            return physical.size();
        }

        @Override
        public Validity validity() {
            return validity;
        }

        // This vector has no single backing: it keeps its own selection at ALL and reads the two inputs through their
        // logical accessors (which apply their own selection). Here valueAt's index is a logical row, not a backing
        // index, and the physical null it consults selects between the stored value and the fallback.
        @Override
        public long valueAt(int physicalRow) {
            if (physical.isNull(physicalRow)) {
                return fallback.getLong(physicalRow);
            }
            return physical.getLong(physicalRow);
        }

        @Override
        public void copyContiguous(MemorySegment target, long targetOffset, int fromPhysical, int count) {
            for (int i = 0; i < count; i++) {
                target.set(INT64, targetOffset + (long) i * Long.BYTES, valueAt(fromPhysical + i));
            }
        }

        @Override
        public LongVector withSelection(Selection selection) {
            return new Coalesced((LongVector) physical.select(selection), (LongVector) fallback.select(selection));
        }

        @Override
        public long approximateHeapBytes() {
            return physical.approximateHeapBytes() + fallback.approximateHeapBytes();
        }
    }
}
