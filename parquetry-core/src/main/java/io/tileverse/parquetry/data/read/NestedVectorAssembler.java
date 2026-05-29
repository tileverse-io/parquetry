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
package io.tileverse.parquetry.data.read;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Builds nested-shape vectors from per-leaf flat vectors and the structural information in the projected schema.
 *
 * <p>Two free-standing builders for callers that want to wrap a single column:
 *
 * <ul>
 *   <li>{@link #buildList(ColumnVector, int[], int)} turns a child column and its rep-level stream into a
 *       {@link ListVector} with per-row offsets. A leaf entry with {@code repLevel == 0} starts a new top-level row;
 *       higher rep levels continue the current row's list.
 *   <li>{@link #buildMap(ColumnVector, ColumnVector, int[], int)} is the MAP counterpart - two element-aligned child
 *       vectors (keys and values) plus the shared rep-level stream.
 * </ul>
 *
 * <p>The driver entry point {@link #assembleNested} walks the projected schema, wraps every LIST and MAP group node
 * into the appropriate vector, wraps every STRUCT group into a {@link StructVector}, and removes the leaf entries that
 * descend from a LIST or MAP wrapper from the top-level columns map. Hiding the leaves keeps the batch's per-column row
 * counts consistent: a leaf under a LIST is sized at element granularity, not logical-row granularity, so surfacing it
 * alongside flat columns would mismatch the batch's row count contract.
 *
 * <p>Null-vs-empty disambiguation via def levels is deferred: the validity bitmap produced by every wrapper is set
 * all-true. Def-level-driven null handling is not yet implemented; all wrapper vectors are constructed with
 * full-validity bitmaps.
 */
public final class NestedVectorAssembler {

    private NestedVectorAssembler() {}

    /**
     * Builds a {@link ListVector} from a child column and the rep-level stream that defines per-row list boundaries.
     *
     * <p>Each entry in {@code repLevels} corresponds to one element in the child vector. An entry with {@code repLevel
     * == 0} signals the start of a new top-level row; higher rep levels continue the current row's list. The resulting
     * {@link ListVector} has {@code numRows} rows and exposes {@link ListVector#rowOffsetStart} and
     * {@link ListVector#rowOffsetEnd} for slicing the child.
     *
     * @param child the per-element vector that lives inside the list
     * @param repLevels rep-level stream aligned with the child's element count (one entry per child element)
     * @param numRows the number of top-level rows the list covers
     * @return a {@link ListVector} with {@code numRows} row slots and offsets derived from the rep-level boundaries
     */
    public static ListVector buildList(ColumnVector child, int[] repLevels, int numRows) {
        int[] offsets = computeTopLevelListOffsets(repLevels, numRows);
        BitSet validity = allValid(numRows);
        return new ListVector(offsets, child, validity, numRows);
    }

    /**
     * Builds a {@link MapVector} from the two element-aligned child columns (keys and values) and the shared rep-level
     * stream that defines per-row map boundaries.
     *
     * <p>Keys and values share offsets, so the rep-level stream of either child works as input. Callers typically pass
     * the rep stream from the key leaf.
     *
     * @param keys per-element vector for the map keys
     * @param values per-element vector for the map values; must share the size of {@code keys}
     * @param repLevels rep-level stream aligned with the element count (one entry per key/value pair)
     * @param numRows the number of top-level rows the map covers
     * @return a {@link MapVector} with {@code numRows} row slots and offsets derived from the rep-level boundaries
     */
    public static MapVector buildMap(ColumnVector keys, ColumnVector values, int[] repLevels, int numRows) {
        int[] offsets = computeTopLevelListOffsets(repLevels, numRows);
        BitSet validity = allValid(numRows);
        return new MapVector(offsets, keys, values, validity, numRows);
    }

    /**
     * Walks the projected schema and produces a column-vector map keyed by full {@link ColumnPath}:
     *
     * <ul>
     *   <li>Each top-level primitive leaf (no LIST/MAP ancestor) keeps its entry from {@code leafVectors}.
     *   <li>Each STRUCT group gets a {@link StructVector} wrapper at the group's path; its direct child leaves remain
     *       addressable through their leaf paths because struct children share the parent's logical-row granularity.
     *   <li>Each LIST group gets a {@link ListVector} wrapper at the group's path. Every leaf descended from the LIST
     *       group is removed from the result map - those leaves are at element granularity and would mismatch the
     *       batch's logical-row count.
     *   <li>Each MAP group gets a {@link MapVector} wrapper at the group's path; its descendant leaves are removed for
     *       the same reason.
     * </ul>
     *
     * <p>LIST and MAP offsets are computed from the rep-level stream of the first descendant leaf encountered. Deeply
     * nested LIST-of-LIST or MAP-of-LIST structures wrap only at the OUTERMOST LIST / MAP path; the inner structure is
     * not reconstructed in this release because the downstream row-API materializers only require the outer shape to be
     * a {@link java.util.List} / {@link java.util.Map} for null-vs-non-null cell discrimination.
     *
     * @param projectedSchema the projected schema describing the column shape
     * @param leafVectors per-leaf flat vectors keyed by their full column path (as produced by BatchRowGroupReader)
     * @param repLevelsByLeaf rep-level stream for every leaf with {@code maxRep > 0}; missing for flat leaves
     * @param numRows logical row count for the batch
     * @return a new map containing the surviving leaves plus one wrapper per LIST / MAP / STRUCT group in the schema
     */
    public static Map<ColumnPath, ColumnVector> assembleNested(
            ParquetSchema projectedSchema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            int numRows) {
        Map<ColumnPath, ColumnVector> result = new HashMap<>(leafVectors);
        Set<ColumnPath> hiddenLeaves = new HashSet<>();
        List<String> prefix = new ArrayList<>();
        for (SchemaNode child : projectedSchema.root().children()) {
            walkField(child, prefix, leafVectors, repLevelsByLeaf, numRows, result, hiddenLeaves);
        }
        for (ColumnPath leaf : hiddenLeaves) {
            result.remove(leaf);
        }
        return result;
    }

    /**
     * Legacy entry point. Equivalent to calling {@link #assembleNested} with an empty {@code repLevelsByLeaf} map; only
     * STRUCT wrapping is performed since the LIST and MAP paths need rep-level streams. Kept for tests that exercise
     * STRUCT wrapping in isolation.
     */
    public static Map<ColumnPath, ColumnVector> wrapStructGroups(
            ParquetSchema projectedSchema, Map<ColumnPath, ColumnVector> leafVectors, int numRows) {
        return assembleNested(projectedSchema, leafVectors, Map.of(), numRows);
    }

    // --- schema walk ---

    /**
     * Visits one field in the schema. Primitive leaves require no work (they are already in {@code result} at their
     * full path). Group fields dispatch on their {@link GroupKind}:
     *
     * <ul>
     *   <li>LIST or MAP: build the wrapper, place it at the group's path, mark all descendant leaves as hidden, and do
     *       not recurse further. Wrappers under a LIST/MAP are not reconstructed in this release.
     *   <li>STRUCT: recurse into children, then collect the resulting child vectors and build a {@link StructVector}.
     * </ul>
     */
    private static void walkField(
            SchemaNode field,
            List<String> prefix,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            int numRows,
            Map<ColumnPath, ColumnVector> result,
            Set<ColumnPath> hiddenLeaves) {
        if (!(field instanceof SchemaNode.Group group)) {
            return;
        }
        List<String> groupPath = concatPath(prefix, group.name());
        GroupKind kind = classify(group);
        switch (kind) {
            case LIST -> wrapListGroup(group, groupPath, leafVectors, repLevelsByLeaf, numRows, result, hiddenLeaves);
            case MAP -> wrapMapGroup(group, groupPath, leafVectors, repLevelsByLeaf, numRows, result, hiddenLeaves);
            case STRUCT ->
                wrapStructGroup(group, groupPath, leafVectors, repLevelsByLeaf, numRows, result, hiddenLeaves);
        }
    }

    private static void wrapListGroup(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            int numRows,
            Map<ColumnPath, ColumnVector> result,
            Set<ColumnPath> hiddenLeaves) {
        ColumnPath wrapperPath = new ColumnPath(groupPath);
        ColumnPath firstLeafPath = findFirstDescendantLeafPath(group, groupPath, leafVectors);
        if (firstLeafPath == null) {
            return;
        }
        ColumnVector childVec = leafVectors.get(firstLeafPath);
        int[] repLevels = repLevelsByLeaf.get(firstLeafPath);
        ListVector listVec = buildListFromMaybeMissingRepLevels(childVec, repLevels, numRows);
        result.put(wrapperPath, listVec);
        markDescendantLeavesHidden(group, groupPath, leafVectors, hiddenLeaves);
    }

    private static void wrapMapGroup(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            int numRows,
            Map<ColumnPath, ColumnVector> result,
            Set<ColumnPath> hiddenLeaves) {
        ColumnPath wrapperPath = new ColumnPath(groupPath);
        MapKeyValuePaths kv = findMapKeyValuePaths(group, groupPath, leafVectors);
        if (kv == null) {
            return;
        }
        ColumnVector keysVec = leafVectors.get(kv.keyPath());
        ColumnVector valuesVec = leafVectors.get(kv.valuePath());
        int[] repLevels = firstNonNull(repLevelsByLeaf.get(kv.keyPath()), repLevelsByLeaf.get(kv.valuePath()));
        MapVector mapVec = buildMapFromMaybeMissingRepLevels(keysVec, valuesVec, repLevels, numRows);
        result.put(wrapperPath, mapVec);
        markDescendantLeavesHidden(group, groupPath, leafVectors, hiddenLeaves);
    }

    private static void wrapStructGroup(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            int numRows,
            Map<ColumnPath, ColumnVector> result,
            Set<ColumnPath> hiddenLeaves) {
        for (SchemaNode child : group.children()) {
            walkField(child, groupPath, leafVectors, repLevelsByLeaf, numRows, result, hiddenLeaves);
        }
        StructVector structVec = collectStructChildren(group, groupPath, leafVectors, result, numRows);
        if (structVec != null) {
            result.put(new ColumnPath(groupPath), structVec);
        }
    }

    // --- vector construction helpers ---

    /**
     * Builds a {@link ListVector} from the child column and rep-level stream. When the rep-level stream is missing
     * (e.g. the descendant leaf is REQUIRED with maxRep == 0, which is degenerate but technically possible for a
     * projected-out shape), the offsets collapse to {@code [0, childSize]} so the wrapper still has the expected row
     * count and a single all-elements list slice per row is impossible to distinguish from a single big list.
     */
    private static ListVector buildListFromMaybeMissingRepLevels(ColumnVector child, int[] repLevels, int numRows) {
        if (repLevels == null) {
            return new ListVector(degenerateOffsets(numRows, child.size()), child, allValid(numRows), numRows);
        }
        return buildList(child, repLevels, numRows);
    }

    private static MapVector buildMapFromMaybeMissingRepLevels(
            ColumnVector keys, ColumnVector values, int[] repLevels, int numRows) {
        if (repLevels == null) {
            return new MapVector(degenerateOffsets(numRows, keys.size()), keys, values, allValid(numRows), numRows);
        }
        return buildMap(keys, values, repLevels, numRows);
    }

    /**
     * Collects direct-child vectors for a STRUCT group from {@code result} (preferring wrapped versions) and falls back
     * to {@code leafVectors} for primitive children. Returns {@code null} when none of the group's children resolve to
     * a vector in the current batch (e.g. the struct was fully projected out).
     */
    private static StructVector collectStructChildren(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, ColumnVector> result,
            int numRows) {
        Map<ColumnPath, ColumnVector> childVectors = new HashMap<>();
        for (SchemaNode child : group.children()) {
            ColumnPath childRelativePath = ColumnPath.of(child.name());
            ColumnPath childFullPath = new ColumnPath(concatPath(groupPath, child.name()));
            ColumnVector vec = result.getOrDefault(childFullPath, leafVectors.get(childFullPath));
            if (vec != null) {
                childVectors.put(childRelativePath, vec);
            }
        }
        if (childVectors.isEmpty()) {
            return null;
        }
        return new StructVector(childVectors, allValid(numRows), numRows);
    }

    // --- leaf-path helpers ---

    /**
     * Walks the schema subtree rooted at {@code group} and returns the first descendant leaf path that has an entry in
     * {@code leafVectors}, or {@code null} if none of the group's leaves are projected.
     */
    private static ColumnPath findFirstDescendantLeafPath(
            SchemaNode.Group group, List<String> groupPath, Map<ColumnPath, ColumnVector> leafVectors) {
        for (SchemaNode child : group.children()) {
            ColumnPath childPath = new ColumnPath(concatPath(groupPath, child.name()));
            if (child instanceof SchemaNode.Primitive) {
                if (leafVectors.containsKey(childPath)) {
                    return childPath;
                }
                continue;
            }
            ColumnPath nested = findFirstDescendantLeafPath(
                    (SchemaNode.Group) child, concatPath(groupPath, child.name()), leafVectors);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    /**
     * Adds every descendant leaf path of {@code group} that is present in {@code leafVectors} to the
     * {@code hiddenLeaves} set. Used to drop leaf entries that descend from a LIST or MAP wrapper, which sizes its
     * children at element granularity instead of logical-row granularity.
     */
    private static void markDescendantLeavesHidden(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Set<ColumnPath> hiddenLeaves) {
        for (SchemaNode child : group.children()) {
            List<String> childPath = concatPath(groupPath, child.name());
            if (child instanceof SchemaNode.Primitive) {
                ColumnPath leafPath = new ColumnPath(childPath);
                if (leafVectors.containsKey(leafPath)) {
                    hiddenLeaves.add(leafPath);
                }
                continue;
            }
            markDescendantLeavesHidden((SchemaNode.Group) child, childPath, leafVectors, hiddenLeaves);
        }
    }

    /**
     * Resolves the key/value leaf paths for a MAP group. Parquet's canonical shape is {@code <map>.key_value.key} /
     * {@code <map>.key_value.value} but Impala-style files use {@code <map>.map.key} / {@code <map>.map.value}; the
     * middle group's actual child names are queried to handle both. Returns {@code null} if either child cannot be
     * resolved (e.g. one was projected out).
     */
    private static MapKeyValuePaths findMapKeyValuePaths(
            SchemaNode.Group mapGroup, List<String> mapGroupPath, Map<ColumnPath, ColumnVector> leafVectors) {
        if (mapGroup.children().isEmpty()) {
            return null;
        }
        SchemaNode middleChild = mapGroup.children().get(0);
        if (!(middleChild instanceof SchemaNode.Group middle)
                || middle.children().size() < 2) {
            return null;
        }
        List<String> middlePath = concatPath(mapGroupPath, middle.name());
        SchemaNode keyField = middle.children().get(0);
        SchemaNode valueField = middle.children().get(1);
        ColumnPath keyPath = leafPathOrFirstDescendant(keyField, concatPath(middlePath, keyField.name()), leafVectors);
        ColumnPath valuePath =
                leafPathOrFirstDescendant(valueField, concatPath(middlePath, valueField.name()), leafVectors);
        if (keyPath == null || valuePath == null) {
            return null;
        }
        return new MapKeyValuePaths(keyPath, valuePath);
    }

    /**
     * Returns the leaf path of {@code field} when it is a primitive, or its first descendant leaf path when it is a
     * group. Used by MAP wrapping where the key or value may itself be a nested group (e.g. MAP&lt;String,
     * Struct&lt;...&gt;&gt;).
     */
    private static ColumnPath leafPathOrFirstDescendant(
            SchemaNode field, List<String> path, Map<ColumnPath, ColumnVector> leafVectors) {
        if (field instanceof SchemaNode.Primitive) {
            ColumnPath p = new ColumnPath(path);
            return leafVectors.containsKey(p) ? p : null;
        }
        return findFirstDescendantLeafPath((SchemaNode.Group) field, path, leafVectors);
    }

    // --- ListVector offset computation ---

    /**
     * Derives the offset array for a top-level {@link ListVector} from the rep-level stream.
     *
     * <p>Each entry in {@code repLevels} with value {@code 0} marks the start of a new top-level row; higher rep levels
     * stay within the current row's list. The offset for row {@code i} is the index of the first element belonging to
     * that row; the sentinel at position {@code numRows} is {@code repLevels.length}.
     */
    private static int[] computeTopLevelListOffsets(int[] repLevels, int numRows) {
        int[] offsets = new int[numRows + 1];
        int currentRow = -1;
        for (int i = 0; i < repLevels.length; i++) {
            if (repLevels[i] == 0) {
                currentRow++;
                if (currentRow >= numRows) {
                    break;
                }
                offsets[currentRow] = i;
            }
        }
        offsets[numRows] = repLevels.length;
        return offsets;
    }

    /**
     * Builds a degenerate offsets array where every row maps to an empty slice and the sentinel at position
     * {@code numRows} is {@code childSize}. Reached only when the projected schema declares a LIST/MAP group with no
     * rep-level information (rep-level array is empty); valid Parquet input does not exercise this path.
     */
    private static int[] degenerateOffsets(int numRows, int childSize) {
        int[] offsets = new int[numRows + 1];
        offsets[numRows] = childSize;
        return offsets;
    }

    // --- schema predicates ---

    /**
     * Classifies a {@link SchemaNode.Group} as LIST, MAP, or STRUCT.
     *
     * <p>A {@link LogicalType.ListType} annotation makes the group a LIST; a {@link LogicalType.MapType} makes it a
     * MAP. A legacy REPEATED group without a LIST annotation is also treated as LIST-like, matching the Parquet
     * convention used by Hive / older writers. Everything else is a STRUCT.
     */
    private static GroupKind classify(SchemaNode.Group group) {
        Optional<LogicalType> annotation = group.logicalType();
        if (annotation.isPresent()) {
            LogicalType lt = annotation.get();
            if (lt instanceof LogicalType.ListType) {
                return GroupKind.LIST;
            }
            if (lt instanceof LogicalType.MapType) {
                return GroupKind.MAP;
            }
        }
        if (group.repetition() == Repetition.REPEATED) {
            return GroupKind.LIST;
        }
        return GroupKind.STRUCT;
    }

    // --- validity helpers ---

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }

    // --- path helpers ---

    private static List<String> concatPath(List<String> prefix, String segment) {
        List<String> result = new ArrayList<>(prefix.size() + 1);
        result.addAll(prefix);
        result.add(segment);
        return result;
    }

    private static int[] firstNonNull(int[] a, int[] b) {
        return a != null ? a : b;
    }

    // --- ADT ---

    private enum GroupKind {
        LIST,
        MAP,
        STRUCT
    }

    private record MapKeyValuePaths(ColumnPath keyPath, ColumnPath valuePath) {}
}
