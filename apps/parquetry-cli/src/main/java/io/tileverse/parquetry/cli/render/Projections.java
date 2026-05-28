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
package io.tileverse.parquetry.cli.render;

import java.util.LinkedHashSet;
import java.util.List;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Turns {@code --columns} names into a {@link Projection} and the ordered list of kept leaf paths, validated against
 * the file's schema.
 */
public final class Projections {

    private Projections() {}

    /** The resolved projection and the ordered list of leaf paths that will be kept. */
    public record Resolved(Projection projection, List<ColumnPath> keptLeaves) {}

    /**
     * Resolves a column selection against a schema.
     *
     * <p>An empty {@code columns} list means "keep all": returns {@link Projection#ALL} and the full depth-first leaf
     * list from the schema. Otherwise each name is split on {@code '.'} to form a {@link ColumnPath}, validated against
     * the schema, and collected in the order the caller supplied. Duplicates are silently de-duplicated while
     * preserving first-occurrence order.
     *
     * @param columns dotted column names from {@code --columns}; empty means all
     * @param schema the file schema to validate against
     * @throws IllegalArgumentException if any name does not match a node in the schema
     */
    public static Resolved resolve(List<String> columns, ParquetSchema schema) {
        if (columns.isEmpty()) {
            return new Resolved(Projection.ALL, schema.leafColumns());
        }
        LinkedHashSet<ColumnPath> kept = new LinkedHashSet<>();
        for (String name : columns) {
            ColumnPath path = ColumnPath.of(name.split("\\."));
            if (schema.find(path).isEmpty()) {
                throw new IllegalArgumentException("unknown column '" + name + "'");
            }
            kept.add(path);
        }
        return new Resolved(Projection.of(kept), List.copyOf(kept));
    }
}
