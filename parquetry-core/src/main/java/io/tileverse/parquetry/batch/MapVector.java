/*
 * Copyright (c) 2026 Tileverse.io
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

import java.util.BitSet;

import lombok.NonNull;

public final class MapVector implements ColumnVector {

    private final int[] offsets;
    private final ColumnVector keys;
    private final ColumnVector values;
    private final BitSet validity;
    private final int size;

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
            @NonNull BitSet validity,
            int size) {
        if (offsets.length != size + 1) {
            throw new IllegalArgumentException(
                    "offsets length must be size + 1; got offsets.length=" + offsets.length + ", size=" + size);
        }
        this.offsets = offsets;
        this.keys = keys;
        this.values = values;
        this.validity = validity;
        this.size = size;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    /** Returns the start offset (inclusive) of row {@code row}'s entries in the keys and values vectors. */
    public int rowOffsetStart(int row) {
        return offsets[row];
    }

    /** Returns the end offset (exclusive) of row {@code row}'s entries in the keys and values vectors. */
    public int rowOffsetEnd(int row) {
        return offsets[row + 1];
    }

    /** The keys column. */
    public ColumnVector keys() {
        return keys;
    }

    /** The values column. */
    public ColumnVector values() {
        return values;
    }
}
