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
import java.util.Objects;

import io.tileverse.parquetry.schema.ColumnPath;

public final class StructVector implements ColumnVector {

    private final Map<ColumnPath, ColumnVector> children;
    private final BitSet validity;
    private final int size;

    /**
     * Constructs a StructVector from child columns.
     *
     * @param children map of child column paths to their vectors
     * @param validity row-level validity bitmap; a true bit means row i is a non-null struct (children may still be
     *     null)
     * @param size number of rows
     */
    public StructVector(Map<ColumnPath, ColumnVector> children, BitSet validity, int size) {
        this.children = Map.copyOf(Objects.requireNonNull(children, "children"));
        this.validity = Objects.requireNonNull(validity, "validity");
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

    /** Returns the map of child column paths to their vectors. */
    public Map<ColumnPath, ColumnVector> children() {
        return children;
    }
}
