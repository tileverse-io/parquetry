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

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import lombok.NonNull;

/**
 * Column vector for FIXED_LEN_BYTE_ARRAY values. Each cell is a {@link #byteWidth()}-byte {@link MemorySegment} view;
 * null cells hold {@code null}. Segments are heap-backed and outlive any Arena.
 */
public final class FixedLenBinaryVector implements ColumnVector {

    private final MemorySegment[] values;
    private final int byteWidth;
    private final BitSet validity;

    private FixedLenBinaryVector(@NonNull MemorySegment[] values, int byteWidth, @NonNull BitSet validity) {
        this.values = values;
        this.byteWidth = byteWidth;
        this.validity = validity;
    }

    public static FixedLenBinaryVector materialized(
            @NonNull MemorySegment[] values, int byteWidth, @NonNull BitSet validity) {
        return new FixedLenBinaryVector(values, byteWidth, validity);
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    /** Fixed width in bytes per value, as declared in the column schema's {@code typeLength}. */
    public int byteWidth() {
        return byteWidth;
    }

    public MemorySegment get(int row) {
        return values[row];
    }

    public MemorySegment[] asArray() {
        return values;
    }
}
