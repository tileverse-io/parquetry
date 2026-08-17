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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The ordinal each descendant leaf of one level-backed group is known by. A metadata tree resolved against those leaves
 * records a scalar's ordinal in place of its path, and every {@link LeafOrder} built from this assignment lays its
 * parallel arrays out in the same order, which lets the per-cell navigation index straight by ordinal.
 *
 * <p>An assignment depends only on the schema and the leaves the projection kept. It holds no batch state, which lets
 * one instance serve every batch that reads the same leaves.
 */
public final class LeafOrdinals {

    private final ColumnPath[] paths;
    private final Map<ColumnPath, Integer> ordinalByPath;

    private LeafOrdinals(ColumnPath[] paths, Map<ColumnPath, Integer> ordinalByPath) {
        this.paths = paths;
        this.ordinalByPath = ordinalByPath;
    }

    /** Numbers {@code leafPaths} from zero in their iteration order. */
    public static LeafOrdinals of(Collection<ColumnPath> leafPaths) {
        ColumnPath[] paths = leafPaths.toArray(ColumnPath[]::new);
        Map<ColumnPath, Integer> ordinalByPath = HashMap.newHashMap(paths.length);
        for (int ordinal = 0; ordinal < paths.length; ordinal++) {
            ordinalByPath.put(paths[ordinal], ordinal);
        }
        return new LeafOrdinals(paths, ordinalByPath);
    }

    /** Number of leaves this assignment numbers. */
    public int leafCount() {
        return paths.length;
    }

    /** The leaf path at {@code ordinal}. */
    public ColumnPath pathAt(int ordinal) {
        return paths[ordinal];
    }

    /** The ordinal of {@code path}; the path must be one of this assignment's leaves. */
    public int ordinalOf(ColumnPath path) {
        return ordinalByPath.get(path);
    }
}
