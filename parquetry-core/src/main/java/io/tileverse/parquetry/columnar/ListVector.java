/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
public final class ListVector implements ColumnVector {

    private final int[] offsets; // length = baseSize + 1
    private final ColumnVector child;
    private final Validity validity;
    private final int size;
    private final Selection selection;

    public ListVector(@NonNull int[] offsets, @NonNull ColumnVector child, @NonNull Validity validity, int size) {
        this(offsets, child, validity, size, Selection.ALL);
    }

    private ListVector(
            @NonNull int[] offsets,
            @NonNull ColumnVector child,
            @NonNull Validity validity,
            int size,
            @NonNull Selection selection) {
        if (offsets.length != size + 1) {
            throw new IllegalArgumentException(
                    "offsets length must be size + 1; got offsets.length=%d, size=%d".formatted(offsets.length, size));
        }
        this.offsets = offsets;
        this.child = child;
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

    /** Returns the start offset (inclusive) of logical row {@code row}'s slice in the child vector. */
    public int rowOffsetStart(int row) {
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        return offsets[physical];
    }

    /** Returns the end offset (exclusive) of logical row {@code row}'s slice in the child vector. */
    public int rowOffsetEnd(int row) {
        int physical = selection == Selection.ALL ? row : selection.physical(row);
        return offsets[physical + 1];
    }

    /**
     * The child vector this list points into. Stays whole on a selected view; only the parent row index is selected.
     */
    public ColumnVector child() {
        return child;
    }

    /**
     * The {@code baseSize + 1} row offsets into the child vector, returned directly without copying; the array is
     * read-only by contract and callers must not mutate it. Only valid on an unselected view: a selected list's
     * contiguous offsets index into a compacted child, which is rebuilt at the Arrow export boundary (see the Arrow
     * session handoff), not here. Per-row consumers use {@link #rowOffsetStart(int)} / {@link #rowOffsetEnd(int)},
     * which honor the selection.
     */
    public int[] offsets() {
        if (selection != Selection.ALL) {
            throw new UnsupportedOperationException(
                    "contiguous offsets of a selected list are rebuilt at the Arrow export boundary; "
                            + "use rowOffsetStart/rowOffsetEnd for per-row access");
        }
        return offsets;
    }

    @Override
    public ColumnVector select(Selection selection) {
        if (selection == Selection.ALL) {
            return this;
        }
        return new ListVector(offsets, child, validity.select(selection), size, selection);
    }

    @Override
    public long approximateHeapBytes() {
        return (long) offsets.length * Integer.BYTES + validity.heapBytes() + child.approximateHeapBytes();
    }
}
