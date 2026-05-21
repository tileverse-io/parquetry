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
import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;

import lombok.NonNull;

/**
 * A column vector carrying nested struct rows. Each row is a named map of child vectors keyed by their relative column
 * path. The {@code validity} bitmap marks which rows are non-null structs; children may still be null at their own
 * level.
 *
 * <p>The {@code children} map is copied on construction to guarantee immutability of the record's state.
 */
public record StructVector(
        @NonNull Map<ColumnPath, ColumnVector> children,
        @NonNull BitSet validity,
        int size) implements ColumnVector {

    /** Compact canonical constructor: validates inputs and defensively copies the children map. */
    public StructVector {
        children = Map.copyOf(children);
    }

    @Override
    public boolean isMaterialized() {
        return children.values().stream().allMatch(ColumnVector::isMaterialized);
    }

    @Override
    public void materialize() {
        children.values().forEach(ColumnVector::materialize);
    }

    @Override
    public void materializeSurvivors(BitSet survivors) {
        children.values().forEach(c -> c.materializeSurvivors(survivors));
    }
}
