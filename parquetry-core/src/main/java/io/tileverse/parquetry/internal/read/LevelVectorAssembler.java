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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.LeafLevels;
import io.tileverse.parquetry.columnar.LeafOrdinals;
import io.tileverse.parquetry.columnar.LevelListVector;
import io.tileverse.parquetry.columnar.LevelMapVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.GroupField;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.GroupPlan;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.LeafField;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.ListPlan;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.MapPlan;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.StructFieldPlan;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.StructPlan;
import io.tileverse.parquetry.internal.read.LevelAssemblyPlan.VariantPlan;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * The levels-form counterpart of {@link NestedVectorAssembler#assembleNestedViews}. It covers the projected schema the
 * same way, but a row-aligned LIST or MAP group becomes a {@link LevelListVector} / {@link LevelMapVector} that keeps
 * the dense leaf vectors plus batch-owned level windows and defers the Dremel assembly to a lazy row view, instead of
 * the eager offset-and-child vectors {@link DremelAssembler} builds. A STRUCT or unshredded-Variant group keeps the
 * same eager wrapper, with its LIST and MAP children replaced by level vectors.
 *
 * <p>Which groups those are, and the metadata each one is built from, is a {@link LevelAssemblyPlan}: everything the
 * schema and the batch's leaves decide, resolved ahead of the batch. What a batch adds is its own data - the leaf
 * vectors, the level windows, and the validity read from them.
 *
 * <p>The per-batch level windows for every repeated leaf are copied once into one pooled segment through
 * {@link BatchLevels}; the returned instance is added to {@code acquiredBuffers}, which the caller's batch owns and
 * closes. When the schema has no repeated leaf the empty {@link BatchLevels} acquires no segment.
 *
 * <p>The leaf-hiding rules match the eager walk: a LIST or MAP hides every descendant leaf, a STRUCT keeps its
 * row-aligned child leaves addressable and hides only the leaves descending through a repeated child.
 */
public final class LevelVectorAssembler {

    private LevelVectorAssembler() {}

    /**
     * Walks {@code projectedSchema} and produces a column-vector map keyed by full {@link ColumnPath}, the level-form
     * shape of {@link NestedVectorAssembler#assembleNestedViews}. Resolves the schema against the batch's leaves on
     * every call; a caller reading many batches over one row group resolves a {@link LevelAssemblyPlan} once and passes
     * it instead.
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

        LevelAssemblyPlan plan = LevelAssemblyPlan.of(projectedSchema, leafVectors.keySet());
        return assembleLevelForm(
                plan,
                projectedSchema,
                leafVectors,
                repLevelsByLeaf,
                defLevelsByLeaf,
                numRows,
                allocator,
                acquiredBuffers);
    }

    /**
     * The level-form column-vector map for one batch, assembled from the metadata {@code plan} already resolved for the
     * batch's leaves. The plan must have been resolved against {@code projectedSchema} and the key set of
     * {@code leafVectors}.
     *
     * @param acquiredBuffers receives the per-batch {@link BatchLevels}; the caller's batch closes it
     */
    // S107: the batch's decode inputs plus the resolved plan; a parameter object would only relocate the arity
    @SuppressWarnings("java:S107")
    static Map<ColumnPath, ColumnVector> assembleLevelForm(
            LevelAssemblyPlan plan,
            ParquetSchema projectedSchema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            int numRows,
            DecodeBufferAllocator allocator,
            List<AutoCloseable> acquiredBuffers) {

        BatchLevels batchLevels =
                BatchLevels.build(allocator, repLevelsByLeaf, defLevelsByLeaf, plan.repeatedLeaves(), numRows);
        acquiredBuffers.add(batchLevels);

        DremelAssembler dremel = new DremelAssembler(projectedSchema, leafVectors, repLevelsByLeaf, defLevelsByLeaf);
        Assembly assembly = new Assembly(projectedSchema, leafVectors, batchLevels, dremel, numRows);

        Map<ColumnPath, ColumnVector> result = new HashMap<>(leafVectors);
        Set<ColumnPath> hiddenLeaves = new HashSet<>();
        for (GroupPlan group : plan.topLevelGroups()) {
            assembly.placeGroup(group, result, hiddenLeaves);
        }
        for (ColumnPath leaf : hiddenLeaves) {
            result.remove(leaf);
        }
        return result;
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

    /** Holds the shared assembly state for one batch, threading the level windows and the eager assembler. */
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

        /** Adds one planned group's vector to {@code result}, and the leaves it takes over to {@code hiddenLeaves}. */
        void placeGroup(GroupPlan group, Map<ColumnPath, ColumnVector> result, Set<ColumnPath> hiddenLeaves) {
            ColumnVector vector = assembleGroup(group);
            if (vector == null) {
                return;
            }
            result.put(group.path(), vector);
            hiddenLeaves.addAll(group.hiddenLeaves());
        }

        /** The level-form vector for one planned group over the batch's rows, or null when it is empty. */
        private ColumnVector assembleGroup(GroupPlan group) {
            return switch (group) {
                case ListPlan list -> levelList(list);
                case MapPlan map -> levelMap(map);
                case StructPlan struct -> levelStruct(struct);
                case VariantPlan variantGroup ->
                    dremel.assembleField(variantGroup.group(), variantGroup.groupPath(), numRows);
            };
        }

        private ColumnVector levelList(ListPlan plan) {
            LeafOrdinals leafOrdinals = plan.leafOrdinals();
            Validity validity = batchLevels.groupValidity(plan.validityLeaf(), plan.groupDefLevel());
            return LevelListVector.of(
                    schema,
                    plan.path(),
                    leavesFor(leafOrdinals),
                    levelsFor(leafOrdinals),
                    validity,
                    numRows,
                    plan.meta(),
                    leafOrdinals);
        }

        private ColumnVector levelMap(MapPlan plan) {
            LeafOrdinals leafOrdinals = plan.leafOrdinals();
            Validity validity = batchLevels.groupValidity(plan.validityLeaf(), plan.groupDefLevel());
            return LevelMapVector.of(
                    schema,
                    plan.path(),
                    leavesFor(leafOrdinals),
                    levelsFor(leafOrdinals),
                    validity,
                    numRows,
                    plan.meta(),
                    leafOrdinals);
        }

        /**
         * A struct keeps the eager wrapper: its row-aligned child leaves stay leaf vectors, a nested non-repeated
         * struct recurses, and a LIST or MAP child becomes a level vector. The validity is the eager per-slot struct
         * validity.
         */
        private ColumnVector levelStruct(StructPlan plan) {
            Map<ColumnPath, ColumnVector> children =
                    LinkedHashMap.newLinkedHashMap(plan.fields().size());
            for (StructFieldPlan field : plan.fields()) {
                ColumnVector childVector = structFieldVector(field);
                if (childVector != null) {
                    children.put(field.key(), childVector);
                }
            }
            if (children.isEmpty()) {
                return null;
            }
            Validity validity = dremel.structValidity(plan.structDefLevel(), plan.validityLeaf(), numRows);
            return new StructVector(children, validity, numRows);
        }

        private ColumnVector structFieldVector(StructFieldPlan field) {
            return switch (field) {
                case LeafField leaf -> leafVectors.get(leaf.leafPath());
                case GroupField nested -> assembleGroup(nested.group());
            };
        }

        private Map<ColumnPath, ColumnVector> leavesFor(LeafOrdinals leafOrdinals) {
            int leafCount = leafOrdinals.leafCount();
            Map<ColumnPath, ColumnVector> leaves = HashMap.newHashMap(leafCount);
            for (int ordinal = 0; ordinal < leafCount; ordinal++) {
                ColumnPath leafPath = leafOrdinals.pathAt(ordinal);
                leaves.put(leafPath, leafVectors.get(leafPath));
            }
            return leaves;
        }

        private Map<ColumnPath, LeafLevels> levelsFor(LeafOrdinals leafOrdinals) {
            int leafCount = leafOrdinals.leafCount();
            Map<ColumnPath, LeafLevels> levels = HashMap.newHashMap(leafCount);
            Map<ColumnPath, LeafLevels> batchOwned = batchLevels.leafLevels();
            for (int ordinal = 0; ordinal < leafCount; ordinal++) {
                ColumnPath leafPath = leafOrdinals.pathAt(ordinal);
                LeafLevels leafLevels = batchOwned.get(leafPath);
                if (leafLevels == null) {
                    throw new IllegalStateException("no batch-owned levels for repeated leaf " + leafPath.dot());
                }
                levels.put(leafPath, leafLevels);
            }
            return levels;
        }
    }
}
