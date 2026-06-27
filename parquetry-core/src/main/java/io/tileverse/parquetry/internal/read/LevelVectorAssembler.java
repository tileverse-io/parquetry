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
package io.tileverse.parquetry.internal.read;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.LevelMapVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.GroupKind;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The levels-form counterpart of {@link NestedVectorAssembler#assembleNestedViews}. It walks the projected schema the
 * same way, but a row-aligned LIST or MAP group becomes a {@link LevelListVector} / {@link LevelMapVector} that keeps
 * the dense leaf vectors plus batch-owned level windows and defers the Dremel assembly to a lazy row view, instead of
 * the eager offset-and-child vectors {@link DremelAssembler} builds. A STRUCT or unshredded-Variant group keeps the
 * same eager wrapper, with its LIST and MAP children replaced by level vectors.
 *
 * <p>The per-batch level windows for every repeated leaf are copied once into one pooled segment through
 * {@link BatchLevels}; the returned instance is added to {@code acquiredBuffers}, which the caller's batch owns and
 * closes. When the schema has no repeated leaf the empty {@link BatchLevels} acquires no segment.
 *
 * <p>The leaf-hiding rules match the eager walk and reuse its widened helpers: a LIST or MAP hides every descendant
 * leaf, a STRUCT keeps its row-aligned child leaves addressable and hides only the leaves descending through a repeated
 * child.
 */
public final class LevelVectorAssembler {

    private LevelVectorAssembler() {}

    /**
     * Walks {@code projectedSchema} and produces a column-vector map keyed by full {@link ColumnPath}, the level-form
     * shape of {@link NestedVectorAssembler#assembleNestedViews}.
     *
     * @param acquiredBuffers receives the per-batch {@link BatchLevels}; the caller's batch closes it
     */
    static Map<ColumnPath, ColumnVector> assembleLevelForm(
            ParquetSchema projectedSchema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            int numRows,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {

        Set<ColumnPath> repeatedLeaves = repeatedDescendantLeaves(projectedSchema, leafVectors);
        BatchLevels batchLevels =
                BatchLevels.build(allocator, repLevelsByLeaf, defLevelsByLeaf, repeatedLeaves, numRows);
        acquiredBuffers.add(batchLevels);

        DremelAssembler dremel = new DremelAssembler(projectedSchema, leafVectors, repLevelsByLeaf, defLevelsByLeaf);
        Assembly assembly = new Assembly(projectedSchema, leafVectors, batchLevels, dremel, numRows);

        Map<ColumnPath, ColumnVector> result = new HashMap<>(leafVectors);
        Set<ColumnPath> hiddenLeaves = new HashSet<>();
        for (SchemaNode child : projectedSchema.root().children()) {
            assembly.walkField(child, new ArrayList<>(), result, hiddenLeaves);
        }
        for (ColumnPath leaf : hiddenLeaves) {
            result.remove(leaf);
        }
        return result;
    }

    /** Every descendant leaf of a LIST or MAP group present in {@code leafVectors}: the leaves a level vector reads. */
    private static Set<ColumnPath> repeatedDescendantLeaves(
            ParquetSchema schema, Map<ColumnPath, ColumnVector> leafVectors) {
        Set<ColumnPath> repeated = new HashSet<>();
        for (SchemaNode child : schema.root().children()) {
            collectRepeatedLeaves(child, new ArrayList<>(), leafVectors, repeated);
        }
        return repeated;
    }

    private static void collectRepeatedLeaves(
            SchemaNode node,
            List<String> nodePath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Set<ColumnPath> repeated) {
        if (!(node instanceof SchemaNode.Group group)) {
            return;
        }
        List<String> groupPath = concat(nodePath, group.name());
        GroupKind kind = GroupKind.of(group);
        if (kind == GroupKind.LIST || kind == GroupKind.MAP) {
            addDescendantLeaves(group, groupPath, leafVectors, repeated);
            return;
        }
        for (SchemaNode child : group.children()) {
            collectRepeatedLeaves(child, groupPath, leafVectors, repeated);
        }
    }

    private static void addDescendantLeaves(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Set<ColumnPath> out) {
        for (SchemaNode child : group.children()) {
            List<String> childPath = concat(groupPath, child.name());
            if (child instanceof SchemaNode.Primitive) {
                ColumnPath leafPath = ColumnPath.of(childPath);
                if (leafVectors.containsKey(leafPath)) {
                    out.add(leafPath);
                }
                continue;
            }
            addDescendantLeaves((SchemaNode.Group) child, childPath, leafVectors, out);
        }
    }

    /**
     * Assembles the Arrow (offset-and-child) form of a level-backed list vector by running the eager
     * {@link DremelAssembler} over its stored leaf vectors and whole-stream level windows. The result equals the
     * eagerly assembled vector value-for-value.
     */
    public static ColumnVector toArrowForm(LevelListVector vec) {
        return arrowForm(vec.schema(), vec.groupPath(), vec.leafPaths(), vec::leaf, vec::levels, vec.size());
    }

    /**
     * The Arrow (offset-and-child) form of a level-backed map vector, the {@link #toArrowForm(LevelListVector)} twin.
     */
    public static ColumnVector toArrowForm(LevelMapVector vec) {
        return arrowForm(vec.schema(), vec.groupPath(), vec.leafPaths(), vec::leaf, vec::levels, vec.size());
    }

    private interface LeafLookup {
        ColumnVector get(ColumnPath path);
    }

    private interface LevelLookup {
        LeafLevels get(ColumnPath path);
    }

    private static ColumnVector arrowForm(
            ParquetSchema schema,
            ColumnPath groupPath,
            Set<ColumnPath> leafPaths,
            LeafLookup leafLookup,
            LevelLookup levelLookup,
            int size) {
        Map<ColumnPath, ColumnVector> leaves = HashMap.newHashMap(leafPaths.size());
        Map<ColumnPath, LevelSlice> repLevels = HashMap.newHashMap(leafPaths.size());
        Map<ColumnPath, LevelSlice> defLevels = HashMap.newHashMap(leafPaths.size());
        for (ColumnPath leafPath : leafPaths) {
            leaves.put(leafPath, leafLookup.get(leafPath));
            LeafLevels leafLevels = levelLookup.get(leafPath);
            repLevels.put(leafPath, LevelSlice.ofWhole(leafLevels.repLevels()));
            defLevels.put(leafPath, LevelSlice.ofWhole(leafLevels.defLevels()));
        }
        DremelAssembler dremel = new DremelAssembler(schema, leaves, repLevels, defLevels);
        SchemaNode.Group group = (SchemaNode.Group) schema.find(groupPath).orElseThrow();
        return dremel.assembleField(group, pathParts(groupPath), size);
    }

    private static List<String> pathParts(ColumnPath path) {
        return List.of(path.dot().split("\\."));
    }

    private static List<String> concat(List<String> prefix, String segment) {
        List<String> result = new ArrayList<>(prefix.size() + 1);
        result.addAll(prefix);
        result.add(segment);
        return result;
    }

    /** Holds the shared assembly state for one walk, threading the level windows and the eager assembler. */
    private static final class Assembly {

        private final ParquetSchema schema;
        private final Map<ColumnPath, ColumnVector> leafVectors;
        private final BatchLevels batchLevels;
        private final DremelAssembler dremel;
        private final int numRows;

        Assembly(
                ParquetSchema schema,
                Map<ColumnPath, ColumnVector> leafVectors,
                BatchLevels batchLevels,
                DremelAssembler dremel,
                int numRows) {
            this.schema = schema;
            this.leafVectors = leafVectors;
            this.batchLevels = batchLevels;
            this.dremel = dremel;
            this.numRows = numRows;
        }

        void walkField(
                SchemaNode field,
                List<String> prefix,
                Map<ColumnPath, ColumnVector> result,
                Set<ColumnPath> hiddenLeaves) {
            if (!(field instanceof SchemaNode.Group group)) {
                return;
            }
            List<String> groupPath = concat(prefix, group.name());
            ColumnVector vector = assembleGroup(group, groupPath, numRows);
            if (vector == null) {
                return;
            }
            result.put(ColumnPath.of(groupPath), vector);
            switch (GroupKind.of(group)) {
                case LIST, MAP ->
                    NestedVectorAssembler.markDescendantLeavesHidden(group, groupPath, leafVectors, hiddenLeaves);
                case STRUCT, VARIANT ->
                    NestedVectorAssembler.hideRepeatedDescendantLeaves(group, groupPath, leafVectors, hiddenLeaves);
            }
        }

        /**
         * The level-form vector for {@code group} over {@code numSlots} row-aligned slots, or null when it is empty.
         */
        private ColumnVector assembleGroup(SchemaNode.Group group, List<String> groupPath, int numSlots) {
            return switch (GroupKind.of(group)) {
                case LIST -> levelList(group, groupPath, numSlots);
                case MAP -> levelMap(group, groupPath, numSlots);
                case STRUCT -> levelStruct(group, groupPath, numSlots);
                case VARIANT -> dremel.assembleField(group, groupPath, numSlots);
            };
        }

        private ColumnVector levelList(SchemaNode.Group group, List<String> groupPath, int numSlots) {
            ColumnPath path = ColumnPath.of(groupPath);
            Map<ColumnPath, ColumnVector> leaves = descendantLeaves(group, groupPath);
            if (leaves.isEmpty()) {
                return null;
            }
            Map<ColumnPath, LeafLevels> levels = levelsFor(leaves.keySet());
            Validity validity = groupValidity(group, groupPath, leaves.keySet());
            return LevelListVector.of(schema, path, leaves, levels, validity, numSlots);
        }

        private ColumnVector levelMap(SchemaNode.Group group, List<String> groupPath, int numSlots) {
            ColumnPath path = ColumnPath.of(groupPath);
            Map<ColumnPath, ColumnVector> leaves = descendantLeaves(group, groupPath);
            if (leaves.isEmpty()) {
                return null;
            }
            Map<ColumnPath, LeafLevels> levels = levelsFor(leaves.keySet());
            Validity validity = groupValidity(group, groupPath, leaves.keySet());
            return LevelMapVector.of(schema, path, leaves, levels, validity, numSlots);
        }

        /**
         * A struct keeps the eager wrapper: its row-aligned child leaves stay leaf vectors, a nested non-repeated
         * struct recurses, and a LIST or MAP child becomes a level vector. The validity is the eager per-slot struct
         * validity.
         */
        private ColumnVector levelStruct(SchemaNode.Group group, List<String> groupPath, int numSlots) {
            Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
            for (SchemaNode child : group.children()) {
                List<String> childPath = concat(groupPath, child.name());
                ColumnVector childVector = assembleStructChild(child, childPath, numSlots);
                if (childVector != null) {
                    children.put(ColumnPath.of(child.name()), childVector);
                }
            }
            if (children.isEmpty()) {
                return null;
            }
            Validity validity = dremel.structValidity(group, groupPath, numSlots);
            return new StructVector(children, validity, numSlots);
        }

        private ColumnVector assembleStructChild(SchemaNode child, List<String> childPath, int numSlots) {
            if (child instanceof SchemaNode.Primitive) {
                return leafVectors.get(ColumnPath.of(childPath));
            }
            SchemaNode.Group childGroup = (SchemaNode.Group) child;
            return switch (GroupKind.of(childGroup)) {
                case LIST -> levelList(childGroup, childPath, numSlots);
                case MAP -> levelMap(childGroup, childPath, numSlots);
                case STRUCT -> levelStruct(childGroup, childPath, numSlots);
                case VARIANT -> dremel.assembleField(childGroup, childPath, numSlots);
            };
        }

        private Validity groupValidity(SchemaNode.Group group, List<String> groupPath, Set<ColumnPath> leafPaths) {
            int groupDefLevel = schema.maxLevels(ColumnPath.of(groupPath)).maxDefinitionLevel();
            ColumnPath structureLeaf = firstPresentDescendantLeaf(group, groupPath, leafPaths);
            return batchLevels.groupValidity(structureLeaf, groupDefLevel);
        }

        private Map<ColumnPath, ColumnVector> descendantLeaves(SchemaNode.Group group, List<String> groupPath) {
            Map<ColumnPath, ColumnVector> leaves = new LinkedHashMap<>();
            collectDescendantLeaves(group, groupPath, leaves);
            return leaves;
        }

        private void collectDescendantLeaves(
                SchemaNode.Group group, List<String> groupPath, Map<ColumnPath, ColumnVector> out) {
            for (SchemaNode child : group.children()) {
                List<String> childPath = concat(groupPath, child.name());
                if (child instanceof SchemaNode.Primitive) {
                    ColumnPath leafPath = ColumnPath.of(childPath);
                    ColumnVector leaf = leafVectors.get(leafPath);
                    if (leaf != null) {
                        out.put(leafPath, leaf);
                    }
                    continue;
                }
                collectDescendantLeaves((SchemaNode.Group) child, childPath, out);
            }
        }

        private Map<ColumnPath, LeafLevels> levelsFor(Set<ColumnPath> leafPaths) {
            Map<ColumnPath, LeafLevels> levels = HashMap.newHashMap(leafPaths.size());
            Map<ColumnPath, LeafLevels> batchOwned = batchLevels.leafLevels();
            for (ColumnPath leafPath : leafPaths) {
                LeafLevels leafLevels = batchOwned.get(leafPath);
                if (leafLevels == null) {
                    throw new IllegalStateException("no batch-owned levels for repeated leaf " + leafPath.dot());
                }
                levels.put(leafPath, leafLevels);
            }
            return levels;
        }

        private ColumnPath firstPresentDescendantLeaf(
                SchemaNode.Group group, List<String> groupPath, Set<ColumnPath> leafPaths) {
            for (SchemaNode child : group.children()) {
                List<String> childPath = concat(groupPath, child.name());
                if (child instanceof SchemaNode.Primitive) {
                    ColumnPath leafPath = ColumnPath.of(childPath);
                    if (leafPaths.contains(leafPath)) {
                        return leafPath;
                    }
                    continue;
                }
                ColumnPath nested = firstPresentDescendantLeaf((SchemaNode.Group) child, childPath, leafPaths);
                if (nested != null) {
                    return nested;
                }
            }
            return null;
        }
    }
}
