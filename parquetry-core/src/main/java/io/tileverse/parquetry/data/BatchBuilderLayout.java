/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.internal.write.ColumnAccumulator;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Wires a write schema into the flat per-leaf arrays and the top-level accumulator tree the batch builder addresses.
 * Computed once at builder construction: the leaf-index arrays ({@code accumulators}, {@code required},
 * {@code indexByPath}) for the index- and path-based setters, the per-leaf chain of enclosing struct accumulators, and
 * the top-level accumulators keyed by column name. The builder takes ownership of these structures.
 */
final class BatchBuilderLayout {

    private final List<ColumnPath> leaves;
    private final Map<ColumnPath, Integer> indexByPath;
    private final ColumnAccumulator[] accumulators;
    private final boolean[] required;
    private final ColumnAccumulator.StructAccumulator[][] leafEnclosingStructs;
    private final Map<String, ColumnAccumulator> topLevel;
    private final boolean[] topLevelRequiredContainer;
    private final List<String> topLevelNames;

    BatchBuilderLayout(ParquetSchema schema) {
        this.leaves = List.copyOf(schema.leafColumns());
        int leafCount = leaves.size();
        this.indexByPath = HashMap.newHashMap(leafCount);
        this.accumulators = new ColumnAccumulator[leafCount];
        this.required = new boolean[leafCount];
        @SuppressWarnings("unchecked")
        ColumnAccumulator.StructAccumulator[][] enclosing = new ColumnAccumulator.StructAccumulator[leafCount][];
        this.leafEnclosingStructs = enclosing;

        List<SchemaNode> topLevelChildren = schema.root().children();
        this.topLevel = LinkedHashMap.newLinkedHashMap(topLevelChildren.size());
        this.topLevelRequiredContainer = new boolean[topLevelChildren.size()];
        this.topLevelNames = new ArrayList<>(topLevelChildren.size());

        // Leaf index counter as we walk top-level children.
        int[] leafIndex = {0};
        int topIdx = 0;
        for (SchemaNode child : topLevelChildren) {
            topLevelNames.add(child.name());
            ColumnAccumulator acc = buildTopLevelAccumulator(child, leafIndex);
            topLevel.put(child.name(), acc);
            topLevelRequiredContainer[topIdx] = ColumnAccumulator.isRequiredContainer(child, acc);
            topIdx++;
        }
    }

    /**
     * Builds an accumulator for one top-level schema node and populates the leaf-index arrays ({@code accumulators},
     * {@code required}, {@code indexByPath}) for each leaf reachable from it.
     */
    private ColumnAccumulator buildTopLevelAccumulator(SchemaNode node, int[] leafIndex) {
        return switch (node) {
            case SchemaNode.Primitive primitive -> {
                rejectRepeated(ColumnPath.of(primitive.name()), primitive);
                PrimitiveKind kind = primitive.kind();
                int width = primitive.typeLength().orElse(0);
                ColumnAccumulator acc = ColumnAccumulator.forKind(kind, width);
                int idx = leafIndex[0]++;
                ColumnPath path = leaves.get(idx);
                accumulators[idx] = acc;
                required[idx] = primitive.repetition() == Repetition.REQUIRED;
                indexByPath.put(path, idx);
                yield acc;
            }
            case SchemaNode.Group group -> buildTopLevelGroupAccumulator(group, leafIndex);
        };
    }

    /**
     * Builds the accumulator for a top-level group. A plain struct registers each descendant leaf for the index-based
     * setters; a list or map column is authored through the scope verbs ({@code beginList}/{@code beginMap}), and its
     * element-aligned descendant leaves are not index-addressable, only consumed to keep the leaf-index counter aligned
     * with {@link ParquetSchema#leafColumns()}.
     */
    private ColumnAccumulator buildTopLevelGroupAccumulator(SchemaNode.Group group, int[] leafIndex) {
        ColumnAccumulator acc = ColumnAccumulator.forNode(group);
        if (acc instanceof ColumnAccumulator.StructAccumulator structAcc) {
            ColumnAccumulator.StructAccumulator[] outerChain = new ColumnAccumulator.StructAccumulator[] {structAcc};
            registerStructLeaves(ColumnPath.of(group.name()), group, structAcc, outerChain, leafIndex);
            return structAcc;
        }
        leafIndex[0] += countDescendantLeaves(group);
        return acc;
    }

    /** Counts the primitive leaves under {@code group}, matching the depth-first order of {@code leafColumns()}. */
    private static int countDescendantLeaves(SchemaNode.Group group) {
        int count = 0;
        for (SchemaNode child : group.children()) {
            count += switch (child) {
                case SchemaNode.Primitive _ -> 1;
                case SchemaNode.Group nested -> countDescendantLeaves(nested);
            };
        }
        return count;
    }

    /**
     * Walks the group's children recursively, registering each leaf in the flat leaf-index arrays and wiring it to the
     * corresponding child accumulator inside the struct hierarchy.
     *
     * <p>{@code enclosingChain} is the sequence of struct accumulators from outermost to innermost enclosing this
     * group. Each primitive leaf records this chain in {@code leafEnclosingStructs}; an index-based setter then calls
     * {@link ColumnAccumulator.StructAccumulator#markPresent()} on all ancestors when a value is authored without an
     * explicit {@code beginStruct/endStruct} scope.
     */
    private void registerStructLeaves(
            ColumnPath groupPath,
            SchemaNode.Group group,
            ColumnAccumulator.StructAccumulator structAcc,
            ColumnAccumulator.StructAccumulator[] enclosingChain,
            int[] leafIndex) {
        for (SchemaNode child : group.children()) {
            switch (child) {
                case SchemaNode.Primitive primitive -> {
                    int idx = leafIndex[0]++;
                    ColumnPath leafPath = leaves.get(idx);
                    ColumnAccumulator childAcc = structAcc.child(primitive.name());
                    accumulators[idx] = childAcc;
                    required[idx] = primitive.repetition() == Repetition.REQUIRED;
                    indexByPath.put(leafPath, idx);
                    leafEnclosingStructs[idx] = enclosingChain;
                }
                case SchemaNode.Group nestedGroup -> {
                    ColumnAccumulator nestedAcc = structAcc.child(nestedGroup.name());
                    registerNestedGroupLeaves(groupPath, nestedGroup, nestedAcc, enclosingChain, leafIndex);
                }
            }
        }
    }

    /**
     * Registers the leaves of a group nested inside a struct. A nested struct recurses, extending the enclosing chain
     * with its accumulator. A nested list or map is authored through the container verbs ({@code beginList} /
     * {@code beginMap}); its element-aligned descendant leaves are not index-addressable, only consumed to keep the
     * leaf-index counter aligned with {@link ParquetSchema#leafColumns()}, exactly as a top-level list or map column is
     * handled.
     */
    private void registerNestedGroupLeaves(
            ColumnPath groupPath,
            SchemaNode.Group nestedGroup,
            ColumnAccumulator nestedAcc,
            ColumnAccumulator.StructAccumulator[] enclosingChain,
            int[] leafIndex) {
        if (nestedAcc instanceof ColumnAccumulator.StructAccumulator nestedStructAcc) {
            ColumnPath nestedPath = appendPath(groupPath, nestedGroup.name());
            ColumnAccumulator.StructAccumulator[] deeperChain =
                    Arrays.copyOf(enclosingChain, enclosingChain.length + 1);
            deeperChain[enclosingChain.length] = nestedStructAcc;
            registerStructLeaves(nestedPath, nestedGroup, nestedStructAcc, deeperChain, leafIndex);
            return;
        }
        leafIndex[0] += countDescendantLeaves(nestedGroup);
    }

    private static ColumnPath appendPath(ColumnPath prefix, String name) {
        List<String> parts = new ArrayList<>(prefix.numParts() + 1);
        for (int i = 0; i < prefix.numParts(); i++) {
            parts.add(prefix.part(i));
        }
        parts.add(name);
        return ColumnPath.of(parts);
    }

    private void rejectRepeated(ColumnPath path, SchemaNode.Primitive leaf) {
        if (leaf.repetition() == Repetition.REPEATED) {
            throw new ParquetWriteException("Repeated leaf columns are not supported by the writer: " + path.dot());
        }
    }

    List<ColumnPath> leaves() {
        return leaves;
    }

    Map<ColumnPath, Integer> indexByPath() {
        return indexByPath;
    }

    ColumnAccumulator[] accumulators() {
        return accumulators;
    }

    boolean[] required() {
        return required;
    }

    ColumnAccumulator.StructAccumulator[][] leafEnclosingStructs() {
        return leafEnclosingStructs;
    }

    Map<String, ColumnAccumulator> topLevel() {
        return topLevel;
    }

    boolean[] topLevelRequiredContainer() {
        return topLevelRequiredContainer;
    }

    List<String> topLevelNames() {
        return topLevelNames;
    }
}
