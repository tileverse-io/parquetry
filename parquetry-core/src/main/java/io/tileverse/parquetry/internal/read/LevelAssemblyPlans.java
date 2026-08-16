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
package io.tileverse.parquetry.internal.read;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The level-assembly plans one row group's batches read, one per set of present leaves. A batch whose leaves have been
 * seen before assembles from the plan already resolved for them; an unseen set resolves its own. Keying on the leaves
 * rather than assuming one plan per row group keeps the metadata's leaf ordinals matched to the leaves a batch actually
 * decoded.
 *
 * <p>An instance belongs to one row-group reader and is read only by the thread driving it, which is why the lookup
 * takes no lock.
 */
final class LevelAssemblyPlans {

    private final ParquetSchema schema;
    private final Map<Set<ColumnPath>, LevelAssemblyPlan> byPresentLeaves = new HashMap<>();

    LevelAssemblyPlans(ParquetSchema schema) {
        this.schema = schema;
    }

    /** The plan for {@code presentLeaves}, resolved on first use and returned unchanged afterwards. */
    LevelAssemblyPlan forLeaves(Set<ColumnPath> presentLeaves) {
        LevelAssemblyPlan resolved = byPresentLeaves.get(presentLeaves);
        if (resolved != null) {
            return resolved;
        }
        Set<ColumnPath> ownLeaves = Set.copyOf(presentLeaves);
        LevelAssemblyPlan plan = LevelAssemblyPlan.of(schema, ownLeaves);
        byPresentLeaves.put(ownLeaves, plan);
        return plan;
    }
}
