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
package io.tileverse.parquetry.internal.footer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The two-way mapping between a schema's leaf columns and the dense ordinals that address them, in the depth-first
 * order of {@link ParquetSchema#leafColumns()}.
 *
 * <p>An ordinal is the column's slot in every per-leaf lane of a {@link CompactFooter}: one index computation replaces
 * a map lookup per column chunk. Instances are immutable and safe to share across threads.
 */
public final class LeafIndex {

    /** The answer of {@link #ordinalOf(ColumnPath)} for a path not declared by the schema. */
    public static final int UNKNOWN = -1;

    private final Map<ColumnPath, Integer> ordinalByPath;
    private final ColumnPath[] pathByOrdinal;

    private LeafIndex(Map<ColumnPath, Integer> ordinalByPath, ColumnPath[] pathByOrdinal) {
        this.ordinalByPath = ordinalByPath;
        this.pathByOrdinal = pathByOrdinal;
    }

    /** Indexes the leaf columns of {@code schema} in their depth-first order. */
    public static LeafIndex of(ParquetSchema schema) {
        List<ColumnPath> leaves = schema.leafColumns();
        ColumnPath[] pathByOrdinal = leaves.toArray(new ColumnPath[0]);
        Map<ColumnPath, Integer> ordinalByPath = HashMap.newHashMap(pathByOrdinal.length);
        for (int ordinal = 0; ordinal < pathByOrdinal.length; ordinal++) {
            ordinalByPath.put(pathByOrdinal[ordinal], ordinal);
        }
        return new LeafIndex(ordinalByPath, pathByOrdinal);
    }

    /** The number of leaf columns in the indexed schema. */
    public int size() {
        return pathByOrdinal.length;
    }

    /** The ordinal of {@code path}, or {@link #UNKNOWN} when the schema declares no such leaf. */
    public int ordinalOf(ColumnPath path) {
        Integer ordinal = ordinalByPath.get(path);
        return ordinal == null ? UNKNOWN : ordinal;
    }

    /**
     * The leaf column at {@code ordinal}.
     *
     * @throws IndexOutOfBoundsException when {@code ordinal} does not address a leaf of the indexed schema
     */
    public ColumnPath pathOf(int ordinal) {
        return pathByOrdinal[ordinal];
    }
}
