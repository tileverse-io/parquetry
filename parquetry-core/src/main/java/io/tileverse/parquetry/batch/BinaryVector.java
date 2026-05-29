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

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import lombok.NonNull;

/**
 * Column vector for BYTE_ARRAY (variable-length binary) values. Each cell is a {@link MemorySegment} view over its
 * bytes; null cells hold {@code null}. Segments are heap-backed (decoded into Java byte arrays during page load) and
 * outlive any Arena.
 */
public final class BinaryVector implements ColumnVector {

    private final MemorySegment[] values;
    private final BitSet validity;

    private BinaryVector(@NonNull MemorySegment[] values, @NonNull BitSet validity) {
        this.values = values;
        this.validity = validity;
    }

    public static BinaryVector materialized(@NonNull MemorySegment[] values, @NonNull BitSet validity) {
        return new BinaryVector(values, validity);
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public BitSet validity() {
        return validity;
    }

    public MemorySegment get(int row) {
        return values[row];
    }

    public MemorySegment[] asArray() {
        return values;
    }
}
