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
 * <p>Operations:
 *
 * <ul>
 *   <li>{@link #leafColumns()} - depth-first list of all primitive column paths.
 *   <li>{@link #find(ColumnPath)} - look up the field at a given path.
 *   <li>{@link #maxLevels(ColumnPath)} - resolve max repetition / definition levels for a leaf.
 *   <li>{@link #project(Set)} - produce a new ParquetSchema retaining only the requested columns.
 *   <li>{@link #resolve(ColumnPath)} - map a logical column path to its physical leaf, descending synthetic LIST/MAP
 *       levels by structure.
 *   <li>{@link #physicalToLogical(ColumnPath)} - recover the logical form of a physical path for diagnostics.
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
                out.add(ColumnPath.of(path));
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
        for (int i = 0; i < path.numParts(); i++) {
            String part = path.part(i);
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
        for (int i = 0; i < path.numParts(); i++) {
            String part = path.part(i);
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
     * Resolves a logical column path to its physical leaf, descending the synthetic LIST {@code list}/{@code element}
     * and MAP {@code key_value} levels by their structure rather than their literal names.
     *
     * <p>An exact physical match wins first. Otherwise the resolver descends each logical segment: a plain child wins
     * by name; failing that, a group whose only child is a single {@code repeated} group is treated as a LIST/MAP
     * wrapper, and the resolver crosses it to match the segment inside the wrapper's element group or against a direct
     * child of the wrapper such as a map key or value. The physical-path index of each {@code repeated} level is
     * recorded in {@link ResolvedColumn#repeatedPhysicalIndices()}.
     *
     * @return the resolved leaf, or {@link Optional#empty()} when the path names no reachable leaf
     */
    public Optional<ResolvedColumn> resolve(ColumnPath logical) {
        Optional<SchemaNode> exact = find(logical);
        if (exact.isPresent() && exact.get() instanceof SchemaNode.Primitive primitive) {
            return Optional.of(new ResolvedColumn(logical, primitive.kind(), repeatedBoundariesOf(logical)));
        }
        List<String> physical = new ArrayList<>();
        List<Integer> repeatedAt = new ArrayList<>();
        SchemaNode current = root;
        for (int i = 0; i < logical.numParts(); i++) {
            if (!(current instanceof SchemaNode.Group group)) {
                return Optional.empty();
            }
            SchemaNode next = descend(group, logical.part(i), physical, repeatedAt);
            if (next == null) {
                return Optional.empty();
            }
            current = next;
        }
        if (!(current instanceof SchemaNode.Primitive leaf)) {
            return Optional.empty();
        }
        return Optional.of(new ResolvedColumn(ColumnPath.of(physical), leaf.kind(), repeatedAt));
    }

    private SchemaNode descend(
            SchemaNode.Group group, String segment, List<String> physical, List<Integer> repeatedAt) {
        Optional<SchemaNode> direct = childNamed(group, segment);
        if (direct.isPresent()) {
            physical.add(segment);
            if (direct.get().repetition() == Repetition.REPEATED) {
                repeatedAt.add(physical.size() - 1);
            }
            return direct.get();
        }
        Optional<SchemaNode.Group> wrapper = repeatedWrapperChild(group);
        if (wrapper.isEmpty()) {
            return null;
        }
        SchemaNode.Group repeatedGroup = wrapper.get();
        physical.add(repeatedGroup.name());
        repeatedAt.add(physical.size() - 1);
        for (SchemaNode wrapperChild : repeatedGroup.children()) {
            if (wrapperChild instanceof SchemaNode.Group elementGroup) {
                Optional<SchemaNode> inElement = childNamed(elementGroup, segment);
                if (inElement.isPresent()) {
                    physical.add(elementGroup.name());
                    physical.add(segment);
                    return inElement.get();
                }
            }
            if (wrapperChild.name().equals(segment)) {
                physical.add(segment);
                return wrapperChild;
            }
        }
        return null;
    }

    private static Optional<SchemaNode> childNamed(SchemaNode.Group group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return Optional.of(child);
            }
        }
        return Optional.empty();
    }

    private static Optional<SchemaNode.Group> repeatedWrapperChild(SchemaNode.Group group) {
        if (group.children().size() != 1) {
            return Optional.empty();
        }
        SchemaNode only = group.children().get(0);
        if (only instanceof SchemaNode.Group repeated && repeated.repetition() == Repetition.REPEATED) {
            return Optional.of(repeated);
        }
        return Optional.empty();
    }

    private List<Integer> repeatedBoundariesOf(ColumnPath path) {
        List<Integer> repeated = new ArrayList<>();
        SchemaNode current = root;
        for (int i = 0; i < path.numParts(); i++) {
            if (!(current instanceof SchemaNode.Group group)) {
                return repeated;
            }
            SchemaNode child = childNamed(group, path.part(i)).orElseThrow();
            if (child.repetition() == Repetition.REPEATED) {
                repeated.add(i);
            }
            current = child;
        }
        return repeated;
    }

    /**
     * Drops the synthetic LIST/MAP levels from a physical path to recover its logical form, for diagnostics only. A
     * path that does not resolve against this schema is returned unchanged.
     */
    public ColumnPath physicalToLogical(ColumnPath physical) {
        // Best-effort, name-based recovery for diagnostics: a file with non-standard element names passes through
        // unchanged rather than guessing which levels are synthetic.
        List<String> logical = new ArrayList<>();
        SchemaNode current = root;
        for (int i = 0; i < physical.numParts(); i++) {
            String segment = physical.part(i);
            if (!(current instanceof SchemaNode.Group group)) {
                return physical;
            }
            Optional<SchemaNode> child = childNamed(group, segment);
            if (child.isEmpty()) {
                return physical;
            }
            boolean syntheticWrapper = repeatedWrapperChild(group)
                    .map(w -> w.name().equals(segment))
                    .orElse(false);
            boolean elementWrapper = "element".equals(segment) || "key_value".equals(segment);
            if (!syntheticWrapper && !elementWrapper) {
                logical.add(segment);
            }
            current = child.get();
        }
        return ColumnPath.of(logical);
    }

    /**
     * A new schema with {@code leaves} added as additional top-level columns after the existing ones. Used to present
     * synthesized columns (e.g. Hive path-only partition columns) that no file physically holds.
     */
    public ParquetSchema withAppendedLeaves(List<SchemaNode.Primitive> leaves) {
        if (leaves.isEmpty()) {
            return this;
        }
        List<SchemaNode> children = new ArrayList<>(root().children());
        children.addAll(leaves);
        SchemaNode.Group augmentedRoot = new SchemaNode.Group(
                root().name(), root().repetition(), children, root().logicalType(), root().fieldId());
        return new ParquetSchema(augmentedRoot);
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
                    io.tileverse.parquetry.format.LogicalType override = overrides.get(ColumnPath.of(childPath));
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
     * Returns a new ParquetSchema containing only the columns in {@code kept}. A path may name a leaf column or a
     * group: naming a group keeps its entire subtree, the way a consumer selects a nested column by its root name
     * without spelling out every leaf below it.
     *
     * <p>Group nodes that become empty after filtering are dropped entirely.
     *
     * @throws ParquetSchemaException if any path in {@code kept} does not name an existing column or group, mirroring
     *     the predicate-path validation done at read setup (a projection typo is a programming error, not
     *     silently-missing data)
     */
    public ParquetSchema project(Set<ColumnPath> kept) {
        requireKnownColumns(kept);
        SchemaNode.Group projectedRoot = projectGroup(root, new ArrayList<>(), kept, true, false);
        return new ParquetSchema(projectedRoot);
    }

    private void requireKnownColumns(Set<ColumnPath> kept) {
        List<String> unknown = kept.stream()
                .filter(path -> find(path).isEmpty())
                .map(ColumnPath::dot)
                .sorted()
                .toList();
        if (!unknown.isEmpty()) {
            throw new ParquetSchemaException("projection references unknown column(s): " + String.join(", ", unknown));
        }
    }

    private static SchemaNode.Group projectGroup(
            SchemaNode.Group group, List<String> prefix, Set<ColumnPath> kept, boolean isRoot, boolean keepAll) {
        List<String> groupPath = new ArrayList<>(prefix);
        if (!isRoot) {
            groupPath.add(group.name());
        }
        List<SchemaNode> projected = new ArrayList<>();
        for (SchemaNode child : group.children()) {
            List<String> childPath = new ArrayList<>(groupPath);
            childPath.add(child.name());
            boolean childKept = keepAll || kept.contains(ColumnPath.of(childPath));
            switch (child) {
                case SchemaNode.Primitive p -> {
                    if (childKept) {
                        projected.add(p);
                    }
                }
                case SchemaNode.Group g -> {
                    SchemaNode.Group sub = projectGroup(g, groupPath, kept, false, childKept);
                    if (!sub.children().isEmpty()) {
                        projected.add(sub);
                    }
                }
            }
        }
        return new SchemaNode.Group(group.name(), group.repetition(), projected, group.logicalType(), group.fieldId());
    }
}
