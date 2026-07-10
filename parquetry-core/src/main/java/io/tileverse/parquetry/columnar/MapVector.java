/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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

import lombok.NonNull;

// Not a record: holds an int[] needing custom equality and read-only cloning accessors a record cannot provide.
@SuppressWarnings("java:S6206")
public final class MapVector implements ColumnVector {

    private final int[] offsets;
    private final ColumnVector keys;
    private final ColumnVector values;
    private final Validity validity;
    private final int size;
    private final Selection selection;

    /**
     * Constructs a MapVector from offsets, keys, and values.
     *
     * @param offsets array of size + 1, where offsets[i] and offsets[i+1] define the range of entries in the keys and
     *     values vectors for row i
     * @param keys the key column
     * @param values the value column
     * @param validity row-level validity bitmap; a true bit means row i is a non-null map (may be empty)
     * @param size number of rows
     * @throws IllegalArgumentException if offsets.length != size + 1
     */
    public MapVector(
            @NonNull int[] offsets,
            @NonNull ColumnVector keys,
            @NonNull ColumnVector values,
            @NonNull Validity validity,
            int size) {
        this(offsets, keys, values, validity, size, Selection.ALL);
    }

    private MapVector(
            @NonNull int[] offsets,
            @NonNull ColumnVector keys,
            @NonNull ColumnVector values,
            @NonNull Validity validity,
            int size,
            @NonNull Selection selection) {
        if (offsets.length != size + 1) {
            throw new IllegalArgumentException(
                    "offsets length must be size + 1; got offsets.length=" + offsets.length + ", size=" + size);
        }
        this.offsets = offsets;
        this.keys = keys;
        this.values = values;
        this.validity = validity;
        this.size = size;
        this.selection = selection;
    }

    @Override
    public Selection selection() {
        return selection;
    }

    @Override
    public int baseSize() {
        return size;
    }

    @Override
    public Validity validity() {
        return validity;
    }

    /** Returns the start offset (inclusive) of logical row {@code row}'s entries in the keys and values vectors. */
    public int rowOffsetStart(int row) {
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        return offsets[physical];
    }

    /** Returns the end offset (exclusive) of logical row {@code row}'s entries in the keys and values vectors. */
    public int rowOffsetEnd(int row) {
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        return offsets[physical + 1];
    }

    /** The keys column. Stays whole on a selected view; only the parent row index is selected. */
    public ColumnVector keys() {
        return keys;
    }

    /** The values column. Stays whole on a selected view; only the parent row index is selected. */
    public ColumnVector values() {
        return values;
    }

    /**
     * The {@code baseSize + 1} row offsets into the keys and values vectors, returned directly without copying; the
     * array is read-only by contract and callers must not mutate it. Only valid on an unselected view: a selected map's
     * contiguous offsets index into compacted children, rebuilt at the Arrow export boundary (see the Arrow session
     * handoff), not here. Per-row consumers use {@link #rowOffsetStart(int)} / {@link #rowOffsetEnd(int)}.
     */
    public int[] offsets() {
        if (selection != Selection.ALL) {
            throw new UnsupportedOperationException(
                    "contiguous offsets of a selected map are rebuilt at the Arrow export boundary; "
                            + "use rowOffsetStart/rowOffsetEnd for per-row access");
        }
        return offsets;
    }

    @Override
    public ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return new MapVector(offsets, keys, values, validity.select(selection), size, selection);
    }

    @Override
    public long approximateHeapBytes() {
        return (long) offsets.length * Integer.BYTES
                + validity.heapBytes()
                + keys.approximateHeapBytes()
                + values.approximateHeapBytes();
    }
}
