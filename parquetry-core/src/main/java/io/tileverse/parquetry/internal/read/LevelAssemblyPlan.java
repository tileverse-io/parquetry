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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.tileverse.parquetry.columnar.LeafOrdinals;
import io.tileverse.parquetry.columnar.ListMeta;
import io.tileverse.parquetry.columnar.MapMeta;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.GroupKind;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Everything {@link LevelVectorAssembler} needs about a schema that does not change from batch to batch: which leaves
 * feed the level windows, and, for every row-aligned group the walk turns into a vector, the metadata and level
 * constants that vector is built from.
 *
 * <p>A plan is resolved against one set of present leaves and holds no batch data, which lets every batch reading those
 * same leaves assemble from it. The leaf ordinals recorded inside a group's {@link ListMeta} / {@link MapMeta} tree and
 * the {@link LeafOrdinals} a batch packs that group's level arrays into are the same instance, which is what keeps a
 * scalar's ordinal pointing at its own leaf across batches.
 *
 * @see LevelAssemblyPlans
 */
final class LevelAssemblyPlan {

    private final Set<ColumnPath> repeatedLeaves;
    private final List<GroupPlan> topLevelGroups;

    private LevelAssemblyPlan(Set<ColumnPath> repeatedLeaves, List<GroupPlan> topLevelGroups) {
        this.repeatedLeaves = repeatedLeaves;
        this.topLevelGroups = topLevelGroups;
    }

    /** Resolves the level-form walk of {@code schema} against the leaves in {@code presentLeaves}. */
    static LevelAssemblyPlan of(ParquetSchema schema, Set<ColumnPath> presentLeaves) {
        return new Resolver(schema, presentLeaves).resolve();
    }

    /** Every descendant leaf of a LIST or MAP group that is present: the leaves a level vector reads. */
    Set<ColumnPath> repeatedLeaves() {
        return repeatedLeaves;
    }

    /** One entry per top-level group that assembles into a vector, in schema order. */
    List<GroupPlan> topLevelGroups() {
        return topLevelGroups;
    }

    /**
     * The resolution for one group the walk assembles into a vector.
     *
     * @see ListPlan
     * @see MapPlan
     * @see StructPlan
     * @see VariantPlan
     */
    sealed interface GroupPlan {

        /** The path the assembled vector is keyed by among the batch's columns. */
        ColumnPath path();

        /**
         * The leaf paths this group removes from the batch's row-aligned columns, because a level vector reads them at
         * element granularity instead. A LIST or MAP names every descendant leaf, which its own vector reads; a STRUCT
         * or Variant names only the leaves below a repeated child, which a nested list or map vector reads.
         */
        Set<ColumnPath> hiddenLeaves();
    }

    /**
     * A LIST group assembled into a level-backed list vector.
     *
     * @param path the list group's path
     * @param leafOrdinals the numbering of the group's present descendant leaves
     * @param meta the level constants and element tree, resolved against those leaves
     * @param groupDefLevel the definition level at which the list container is present
     * @param validityLeaf the leaf whose definition levels report per-row presence of the container
     * @param hiddenLeaves every present descendant leaf of the group
     */
    record ListPlan(
            ColumnPath path,
            LeafOrdinals leafOrdinals,
            ListMeta meta,
            int groupDefLevel,
            ColumnPath validityLeaf,
            Set<ColumnPath> hiddenLeaves)
            implements GroupPlan {}

    /**
     * A MAP group assembled into a level-backed map vector.
     *
     * @param path the map group's path
     * @param leafOrdinals the numbering of the group's present descendant leaves
     * @param meta the level constants and key/value resolution, resolved against those leaves
     * @param groupDefLevel the definition level at which the map container is present
     * @param validityLeaf the leaf whose definition levels report per-row presence of the container
     * @param hiddenLeaves every present descendant leaf of the group
     */
    record MapPlan(
            ColumnPath path,
            LeafOrdinals leafOrdinals,
            MapMeta meta,
            int groupDefLevel,
            ColumnPath validityLeaf,
            Set<ColumnPath> hiddenLeaves)
            implements GroupPlan {}

    /**
     * A STRUCT group assembled into an eager struct wrapper whose fields keep the parent's logical-row granularity.
     *
     * @param path the struct group's path
     * @param fields the fields that resolve to a vector, in schema child order
     * @param structDefLevel the struct's max definition level
     * @param validityLeaf the row-aligned descendant leaf whose definition levels report the struct's per-slot presence
     * @param hiddenLeaves the descendant leaves that live under a repeated child and belong to its own vector
     */
    record StructPlan(
            ColumnPath path,
            List<StructFieldPlan> fields,
            int structDefLevel,
            ColumnPath validityLeaf,
            Set<ColumnPath> hiddenLeaves)
            implements GroupPlan {

        StructPlan {
            fields = List.copyOf(fields);
        }
    }

    /**
     * A Variant group, which the eager assembler reconstructs from its own schema walk.
     *
     * @param path the Variant group's path
     * @param group the Variant group node
     * @param groupPath the group's path in segment form, the shape the eager assembler walks with
     * @param hiddenLeaves the descendant leaves that live under a repeated child and belong to its own vector
     */
    record VariantPlan(ColumnPath path, SchemaNode.Group group, List<String> groupPath, Set<ColumnPath> hiddenLeaves)
            implements GroupPlan {

        VariantPlan {
            groupPath = List.copyOf(groupPath);
        }
    }

    /**
     * One field of a struct that resolves to a vector, keyed by the field name the struct wrapper addresses it under.
     *
     * @see LeafField
     * @see GroupField
     */
    sealed interface StructFieldPlan {

        /** The single-segment path the struct wrapper keys this field by. */
        ColumnPath key();
    }

    /**
     * A field read straight from a decoded leaf vector.
     *
     * @param key the field name
     * @param leafPath the full path of the leaf the values come from
     */
    record LeafField(ColumnPath key, ColumnPath leafPath) implements StructFieldPlan {}

    /**
     * A field that is itself a group and assembles into its own vector.
     *
     * @param key the field name
     * @param group the nested group's resolution
     */
    record GroupField(ColumnPath key, GroupPlan group) implements StructFieldPlan {}

    /** Walks a schema once, resolving every group it assembles against a fixed set of present leaves. */
    private static final class Resolver {

        private final ParquetSchema schema;
        private final Set<ColumnPath> presentLeaves;

        Resolver(ParquetSchema schema, Set<ColumnPath> presentLeaves) {
            this.schema = schema;
            this.presentLeaves = presentLeaves;
        }

        LevelAssemblyPlan resolve() {
            List<GroupPlan> topLevel = new ArrayList<>();
            for (SchemaNode child : schema.root().children()) {
                GroupPlan group = groupPlan(child, List.of(child.name()));
                if (group != null) {
                    topLevel.add(group);
                }
            }
            return new LevelAssemblyPlan(repeatedDescendantLeaves(), List.copyOf(topLevel));
        }

        /** The resolution for {@code field}, or null when it is not a group or assembles into nothing. */
        private GroupPlan groupPlan(SchemaNode field, List<String> nodePath) {
            if (!(field instanceof SchemaNode.Group group)) {
                return null;
            }
            return switch (GroupKind.of(group)) {
                case LIST -> listPlan(group, nodePath);
                case MAP -> mapPlan(group, nodePath);
                case STRUCT -> structPlan(group, nodePath);
                case VARIANT -> variantPlan(group, nodePath);
            };
        }

        private ListPlan listPlan(SchemaNode.Group group, List<String> groupPath) {
            List<ColumnPath> descendants = descendantLeaves(group, groupPath);
            if (descendants.isEmpty()) {
                return null;
            }
            ColumnPath path = ColumnPath.of(groupPath);
            LeafOrdinals leafOrdinals = LeafOrdinals.of(descendants);
            Set<ColumnPath> ownLeaves = Set.copyOf(descendants);
            ListMeta meta = ListMeta.of(schema, group, path, ownLeaves, leafOrdinals::ordinalOf);
            return new ListPlan(
                    path,
                    leafOrdinals,
                    meta,
                    groupDefLevel(path),
                    firstPresentDescendantLeaf(group, groupPath),
                    ownLeaves);
        }

        private MapPlan mapPlan(SchemaNode.Group group, List<String> groupPath) {
            List<ColumnPath> descendants = descendantLeaves(group, groupPath);
            if (descendants.isEmpty()) {
                return null;
            }
            ColumnPath path = ColumnPath.of(groupPath);
            LeafOrdinals leafOrdinals = LeafOrdinals.of(descendants);
            Set<ColumnPath> ownLeaves = Set.copyOf(descendants);
            MapMeta meta = MapMeta.of(schema, group, path, ownLeaves, leafOrdinals::ordinalOf);
            return new MapPlan(
                    path,
                    leafOrdinals,
                    meta,
                    groupDefLevel(path),
                    firstPresentDescendantLeaf(group, groupPath),
                    ownLeaves);
        }

        private StructPlan structPlan(SchemaNode.Group group, List<String> groupPath) {
            List<StructFieldPlan> fields = new ArrayList<>(group.children().size());
            for (SchemaNode child : group.children()) {
                StructFieldPlan field = structFieldPlan(child, concat(groupPath, child.name()));
                if (field != null) {
                    fields.add(field);
                }
            }
            if (fields.isEmpty()) {
                return null;
            }
            ColumnPath path = ColumnPath.of(groupPath);
            return new StructPlan(
                    path,
                    fields,
                    groupDefLevel(path),
                    DremelAssembler.firstRowAlignedDescendantLeaf(group, groupPath, presentLeaves),
                    repeatedDescendantLeavesOf(group, groupPath));
        }

        private StructFieldPlan structFieldPlan(SchemaNode child, List<String> childPath) {
            ColumnPath key = ColumnPath.of(child.name());
            if (child instanceof SchemaNode.Primitive) {
                ColumnPath leafPath = ColumnPath.of(childPath);
                return presentLeaves.contains(leafPath) ? new LeafField(key, leafPath) : null;
            }
            GroupPlan nested = groupPlan(child, childPath);
            return nested == null ? null : new GroupField(key, nested);
        }

        private VariantPlan variantPlan(SchemaNode.Group group, List<String> groupPath) {
            return new VariantPlan(
                    ColumnPath.of(groupPath), group, groupPath, repeatedDescendantLeavesOf(group, groupPath));
        }

        private int groupDefLevel(ColumnPath path) {
            return schema.maxLevels(path).maxDefinitionLevel();
        }

        // --- descendant-leaf resolution ---

        /** Every present descendant leaf of a LIST or MAP group anywhere in the schema. */
        private Set<ColumnPath> repeatedDescendantLeaves() {
            Set<ColumnPath> repeated = new HashSet<>();
            for (SchemaNode child : schema.root().children()) {
                collectRepeatedLeaves(child, List.of(child.name()), repeated);
            }
            return Set.copyOf(repeated);
        }

        private void collectRepeatedLeaves(SchemaNode node, List<String> nodePath, Set<ColumnPath> repeated) {
            if (!(node instanceof SchemaNode.Group group)) {
                return;
            }
            GroupKind kind = GroupKind.of(group);
            if (kind == GroupKind.LIST || kind == GroupKind.MAP) {
                repeated.addAll(descendantLeaves(group, nodePath));
                return;
            }
            for (SchemaNode child : group.children()) {
                collectRepeatedLeaves(child, concat(nodePath, child.name()), repeated);
            }
        }

        /** The leaves below {@code group} that descend through a repeated child and belong to its own vector. */
        private Set<ColumnPath> repeatedDescendantLeavesOf(SchemaNode.Group group, List<String> groupPath) {
            Set<ColumnPath> hidden = new HashSet<>();
            NestedVectorAssembler.hideRepeatedDescendantLeaves(group, groupPath, presentLeaves, hidden);
            return Set.copyOf(hidden);
        }

        /** Every present leaf below {@code group}, in schema child order. */
        private List<ColumnPath> descendantLeaves(SchemaNode.Group group, List<String> groupPath) {
            List<ColumnPath> leaves = new ArrayList<>();
            collectDescendantLeaves(group, groupPath, leaves);
            return leaves;
        }

        private void collectDescendantLeaves(SchemaNode.Group group, List<String> groupPath, List<ColumnPath> out) {
            for (SchemaNode child : group.children()) {
                List<String> childPath = concat(groupPath, child.name());
                if (child instanceof SchemaNode.Primitive) {
                    ColumnPath leafPath = ColumnPath.of(childPath);
                    if (presentLeaves.contains(leafPath)) {
                        out.add(leafPath);
                    }
                    continue;
                }
                collectDescendantLeaves((SchemaNode.Group) child, childPath, out);
            }
        }

        private ColumnPath firstPresentDescendantLeaf(SchemaNode.Group group, List<String> groupPath) {
            for (SchemaNode child : group.children()) {
                List<String> childPath = concat(groupPath, child.name());
                if (child instanceof SchemaNode.Primitive) {
                    ColumnPath leafPath = ColumnPath.of(childPath);
                    if (presentLeaves.contains(leafPath)) {
                        return leafPath;
                    }
                    continue;
                }
                ColumnPath nested = firstPresentDescendantLeaf((SchemaNode.Group) child, childPath);
                if (nested != null) {
                    return nested;
                }
            }
            return null;
        }

        private static List<String> concat(List<String> prefix, String segment) {
            List<String> result = new ArrayList<>(prefix.size() + 1);
            result.addAll(prefix);
            result.add(segment);
            return result;
        }
    }
}
