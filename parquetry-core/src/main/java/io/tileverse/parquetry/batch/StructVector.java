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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * A column vector holding nested struct rows. Each row is a named map of child vectors keyed by their relative column
 * path. The {@code validity} mask marks which rows are non-null structs; children may still be null at their own level.
 *
 * <p>The {@code children} map is copied on construction into an insertion-ordered, immutable map. The assembler inserts
 * children in schema order, and preserving that order lets a positional row view address struct children by index.
 */
public record StructVector(
        @NonNull Map<ColumnPath, ColumnVector> children,
        @NonNull Validity validity,
        int size) implements ColumnVector {

    /** Compact canonical constructor: validates inputs and defensively copies the children map, preserving order. */
    public StructVector {
        children = Collections.unmodifiableMap(new LinkedHashMap<>(children));
    }

    @Override
    public long approximateHeapBytes() {
        long total = validity.heapBytes();
        for (ColumnVector child : children.values()) {
            total += child.approximateHeapBytes();
        }
        return total;
    }
}
