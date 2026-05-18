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
package io.tileverse.parquetry.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * The schema of a Parquet file: a root group with named leaf columns and optional nested groups.
 *
 * <p>The root group itself is anonymous in path notation; paths start from its direct children. For example, a flat
 * schema with columns "year" and "country" yields paths {@code ColumnPath.of("year")} and
 * {@code ColumnPath.of("country")}.
 *
 * <p>Three operations:
 *
 * <ul>
 *   <li>{@link #leafColumns()} - depth-first list of all primitive column paths.
 *   <li>{@link #find(ColumnPath)} - look up the field at a given path.
 *   <li>{@link #project(Set)} - produce a new Schema retaining only the requested columns.
 * </ul>
 */
public record Schema(Field.Group root) {

    /**
     * Returns all leaf (primitive) column paths in depth-first order.
     *
     * <p>The root group's name is excluded from every path.
     */
    public List<ColumnPath> leafColumns() {
        List<ColumnPath> out = new ArrayList<>();
        collectLeaves(root, new ArrayList<>(), out, true);
        return out;
    }

    private static void collectLeaves(Field field, List<String> prefix, List<ColumnPath> out, boolean isRoot) {
        switch (field) {
            case Field.Primitive p -> {
                List<String> path = new ArrayList<>(prefix);
                path.add(p.name());
                out.add(new ColumnPath(path));
            }
            case Field.Group g -> {
                List<String> next = new ArrayList<>(prefix);
                if (!isRoot) {
                    next.add(g.name());
                }
                for (Field child : g.children()) {
                    collectLeaves(child, next, out, false);
                }
            }
        }
    }

    /**
     * Finds the field at the given path, or {@link Optional#empty()} if not found.
     *
     * <p>Traversal starts from the root's children; the root itself is not addressable by path.
     */
    public Optional<Field> find(ColumnPath path) {
        Field current = root;
        for (String part : path.parts()) {
            if (!(current instanceof Field.Group g)) {
                return Optional.empty();
            }
            current = g.children().stream()
                    .filter(c -> c.name().equals(part))
                    .findFirst()
                    .orElse(null);
            if (current == null) {
                return Optional.empty();
            }
        }
        return Optional.of(current);
    }

    /**
     * Returns a new Schema containing only the leaf columns in {@code kept}.
     *
     * <p>Group nodes that become empty after filtering are dropped entirely.
     */
    public Schema project(Set<ColumnPath> kept) {
        Field.Group projectedRoot = projectGroup(root, new ArrayList<>(), kept, true);
        return new Schema(projectedRoot);
    }

    private static Field.Group projectGroup(
            Field.Group group, List<String> prefix, Set<ColumnPath> kept, boolean isRoot) {
        List<String> groupPath = new ArrayList<>(prefix);
        if (!isRoot) {
            groupPath.add(group.name());
        }
        List<Field> projected = new ArrayList<>();
        for (Field child : group.children()) {
            switch (child) {
                case Field.Primitive p -> {
                    List<String> childPath = new ArrayList<>(groupPath);
                    childPath.add(p.name());
                    if (kept.contains(new ColumnPath(childPath))) {
                        projected.add(p);
                    }
                }
                case Field.Group g -> {
                    Field.Group sub = projectGroup(g, groupPath, kept, false);
                    if (!sub.children().isEmpty()) {
                        projected.add(sub);
                    }
                }
            }
        }
        return new Field.Group(group.name(), group.repetition(), projected, group.logicalType(), group.fieldId());
    }
}
