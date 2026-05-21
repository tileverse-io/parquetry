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

public final class ListVector implements ColumnVector {

    private final int[] offsets; // length = size + 1
    private final ColumnVector child;
    private final BitSet validity;
    private final int size;

    public ListVector(@NonNull int[] offsets, @NonNull ColumnVector child, @NonNull BitSet validity, int size) {
        if (offsets.length != size + 1) {
            throw new IllegalArgumentException(
                    "offsets length must be size + 1; got offsets.length=%d, size=%d".formatted(offsets.length, size));
        }
        this.offsets = offsets;
        this.child = child;
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

    /** Returns the start offset (inclusive) of row {@code row}'s slice in the child vector. */
    public int rowOffsetStart(int row) {
        return offsets[row];
    }

    /** Returns the end offset (exclusive) of row {@code row}'s slice in the child vector. */
    public int rowOffsetEnd(int row) {
        return offsets[row + 1];
    }

    /** The child vector this list points into. */
    public ColumnVector child() {
        return child;
    }
}
