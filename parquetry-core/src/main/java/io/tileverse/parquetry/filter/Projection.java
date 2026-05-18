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
package io.tileverse.parquetry.filter;

import java.util.Set;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Column projection for {@code Dataset.read(predicate, projection, ...)}.
 *
 * <p>{@link #ALL} keeps every leaf column; otherwise the projection lists the leaf paths
 * to keep. Nested ancestor groups are preserved automatically by
 * {@link io.tileverse.parquetry.schema.Schema#project(Set)}.
 */
public sealed interface Projection permits Projection.All, Projection.Columns {

    Projection ALL = new All();

    static Projection of(Set<ColumnPath> kept) {
        return new Columns(kept);
    }

    record All() implements Projection {}

    record Columns(Set<ColumnPath> kept) implements Projection {
        public Columns {
            kept = Set.copyOf(kept);
        }
    }
}
