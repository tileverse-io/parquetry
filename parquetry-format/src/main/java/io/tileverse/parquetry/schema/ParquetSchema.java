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
 * <p>Four operations:
 *
 * <ul>
 *   <li>{@link #leafColumns()} - depth-first list of all primitive column paths.
 *   <li>{@link #find(ColumnPath)} - look up the field at a given path.
 *   <li>{@link #maxLevels(ColumnPath)} - resolve max repetition / definition levels for a leaf.
 *   <li>{@link #project(Set)} - produce a new ParquetSchema retaining only the requested columns.
 * </ul>
 *
 * @param root the anonymous root group; its name does not appear in any {@link ColumnPath} and its children are the
 *     top-level columns of the schema
 */
public record ParquetSchema(SchemaNode.Group root) {

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

    private static void collectLeaves(SchemaNode field, List<String> prefix, List<ColumnPath> out, boolean isRoot) {
        switch (field) {
            case SchemaNode.Primitive p -> {
                List<String> path = new ArrayList<>(prefix);
                path.add(p.name());
                out.add(new ColumnPath(path));
            }
            case SchemaNode.Group g -> {
                List<String> next = new ArrayList<>(prefix);
                if (!isRoot) {
                    next.add(g.name());
                }
                for (SchemaNode child : g.children()) {
                    collectLeaves(child, next, out, false);
                }
            }
        }
    }

    /**
     * Resolves the max repetition and definition levels for the leaf at {@code path}.
     *
     * <p>Walks from the root down to the leaf summing per-ancestor contributions:
     *
     * <ul>
     *   <li>REQUIRED: no contribution.
     *   <li>OPTIONAL: definition += 1.
     *   <li>REPEATED: definition += 1, repetition += 1.
     * </ul>
     *
     * <p>Both the read-side page decoders and the write-side level encoder consume the result.
     *
     * @throws ParquetSchemaException if {@code path} cannot be resolved against this schema
     */
    public LevelMaxima maxLevels(ColumnPath path) {
        int maxRep = 0;
        int maxDef = 0;
        SchemaNode current = root;
        for (String part : path.parts()) {
            if (!(current instanceof SchemaNode.Group group)) {
                throw new ParquetSchemaException("Path traversal hit a non-group at " + path.dot());
            }
            SchemaNode child = findChildByName(group, part)
                    .orElseThrow(() -> new ParquetSchemaException(
                            "ParquetSchema missing child '" + part + "' along path " + path.dot()));
            switch (child.repetition()) {
                case REQUIRED -> {
                    // No contribution.
                }
                case OPTIONAL -> maxDef++;
                case REPEATED -> {
                    maxDef++;
                    maxRep++;
                }
            }
            current = child;
        }
        return new LevelMaxima(maxRep, maxDef);
    }

    private static Optional<SchemaNode> findChildByName(SchemaNode.Group group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return Optional.of(child);
            }
        }
        return Optional.empty();
    }

    /**
     * Finds the field at the given path, or {@link Optional#empty()} if not found.
     *
     * <p>Traversal starts from the root's children; the root itself is not addressable by path.
     */
    public Optional<SchemaNode> find(ColumnPath path) {
        SchemaNode current = root;
        for (String part : path.parts()) {
            if (!(current instanceof SchemaNode.Group g)) {
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
     * Returns a copy of this schema with the logical-type annotation overridden on each leaf listed in
     * {@code overrides}. Leaves not in {@code overrides} are returned unchanged. Used by the GeoParquet 1.x metadata
     * bridge at {@code ParquetDataset.open()} time to synthesize {@code Geometry} / {@code Geography} logical types on
     * columns that the file's {@code "geo"} key-value metadata identifies as geometry / geography but whose Thrift
     * {@code SchemaElement} carries no native logical-type annotation.
     */
    public ParquetSchema withLogicalTypes(
            java.util.Map<ColumnPath, io.tileverse.parquetry.format.LogicalType> overrides) {
        if (overrides.isEmpty()) {
            return this;
        }
        return new ParquetSchema(applyLogicalTypes(root, new ArrayList<>(), overrides, true));
    }

    private static SchemaNode.Group applyLogicalTypes(
            SchemaNode.Group group,
            List<String> prefix,
            java.util.Map<ColumnPath, io.tileverse.parquetry.format.LogicalType> overrides,
            boolean isRoot) {
        List<String> groupPath = new ArrayList<>(prefix);
        if (!isRoot) {
            groupPath.add(group.name());
        }
        List<SchemaNode> children = new ArrayList<>(group.children().size());
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.Primitive p -> {
                    List<String> childPath = new ArrayList<>(groupPath);
                    childPath.add(p.name());
                    io.tileverse.parquetry.format.LogicalType override = overrides.get(new ColumnPath(childPath));
                    if (override != null) {
                        children.add(new SchemaNode.Primitive(
                                p.name(),
                                p.repetition(),
                                p.kind(),
                                p.typeLength(),
                                Optional.of(override),
                                p.fieldId()));
                    } else {
                        children.add(p);
                    }
                }
                case SchemaNode.Group g -> children.add(applyLogicalTypes(g, groupPath, overrides, false));
            }
        }
        return new SchemaNode.Group(group.name(), group.repetition(), children, group.logicalType(), group.fieldId());
    }

    /**
     * Returns a new ParquetSchema containing only the leaf columns in {@code kept}.
     *
     * <p>Group nodes that become empty after filtering are dropped entirely.
     */
    public ParquetSchema project(Set<ColumnPath> kept) {
        SchemaNode.Group projectedRoot = projectGroup(root, new ArrayList<>(), kept, true);
        return new ParquetSchema(projectedRoot);
    }

    private static SchemaNode.Group projectGroup(
            SchemaNode.Group group, List<String> prefix, Set<ColumnPath> kept, boolean isRoot) {
        List<String> groupPath = new ArrayList<>(prefix);
        if (!isRoot) {
            groupPath.add(group.name());
        }
        List<SchemaNode> projected = new ArrayList<>();
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.Primitive p -> {
                    List<String> childPath = new ArrayList<>(groupPath);
                    childPath.add(p.name());
                    if (kept.contains(new ColumnPath(childPath))) {
                        projected.add(p);
                    }
                }
                case SchemaNode.Group g -> {
                    SchemaNode.Group sub = projectGroup(g, groupPath, kept, false);
                    if (!sub.children().isEmpty()) {
                        projected.add(sub);
                    }
                }
            }
        }
        return new SchemaNode.Group(group.name(), group.repetition(), projected, group.logicalType(), group.fieldId());
    }
}
