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
package io.tileverse.parquetry.columnar;

import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The descendant-leaf access a level-form navigation needs: the schema the elements resolve against, each present
 * descendant leaf's dense vector and batch-owned level window, and the set of present leaf paths. Both
 * {@link LevelListVector} and {@link LevelMapVector} provide this, which lets one lazy navigation engine drive list
 * elements and map entries alike without knowing which container holds them.
 */
public interface LevelSource {

    /** The schema this container's elements resolve against. */
    ParquetSchema schema();

    /**
     * The dense entry-aligned descendant leaf vector at {@code path}, or {@code null} when the projection dropped it.
     */
    ColumnVector leaf(ColumnPath path);

    /** The batch-owned level window for the descendant leaf at {@code path}. */
    LeafLevels levels(ColumnPath path);

    /** The paths of the descendant leaves present in this container (those the projection kept). */
    Set<ColumnPath> leafPaths();

    /**
     * The batch-constant leaf order and per-leaf level streams the navigation reads. Built once at vector construction;
     * the per-cell navigation hoists all its constant state from here. Valid only while the owning batch is open.
     */
    LeafOrder leafOrder();
}
