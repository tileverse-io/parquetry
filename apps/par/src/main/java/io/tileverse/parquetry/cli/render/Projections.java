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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

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
     * the schema, and collected in the order the caller supplied. A name that addresses a group expands to every leaf
     * beneath it, because both {@link Projection} and {@link ParquetSchema#project(java.util.Set)} operate on leaf
     * paths. Duplicates are silently de-duplicated while preserving first-occurrence order.
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
            SchemaNode node =
                    schema.find(path).orElseThrow(() -> new IllegalArgumentException("unknown column '" + name + "'"));
            kept.addAll(leavesUnder(path, node));
        }
        return new Resolved(Projection.of(kept), List.copyOf(kept));
    }

    private static List<ColumnPath> leavesUnder(ColumnPath path, SchemaNode node) {
        if (node instanceof SchemaNode.Primitive) {
            return List.of(path);
        }
        List<ColumnPath> leaves = new ArrayList<>();
        collectLeaves((SchemaNode.Group) node, segmentsOf(path), leaves);
        return leaves;
    }

    private static List<String> segmentsOf(ColumnPath path) {
        List<String> segments = new ArrayList<>(path.numParts());
        for (int index = 0; index < path.numParts(); index++) {
            segments.add(path.part(index));
        }
        return segments;
    }

    private static void collectLeaves(SchemaNode.Group group, List<String> prefix, List<ColumnPath> out) {
        for (SchemaNode child : group.children()) {
            List<String> childPath = new ArrayList<>(prefix);
            childPath.add(child.name());
            switch (child) {
                case SchemaNode.Primitive _ -> out.add(ColumnPath.of(childPath));
                case SchemaNode.Group nested -> collectLeaves(nested, childPath, out);
            }
        }
    }
}
