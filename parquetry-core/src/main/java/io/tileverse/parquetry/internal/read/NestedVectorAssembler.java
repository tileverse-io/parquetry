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
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
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
 * <p>The driver entry point {@link #assembleNested} walks the projected schema, reconstructs every LIST / MAP / STRUCT
 * group into its vector, and removes the element-granular leaf entries from the top-level columns map. Hiding the
 * element-granular leaves keeps the batch's per-column row counts consistent: a leaf under a LIST is sized at element
 * granularity, not logical-row granularity, and exposing it alongside flat columns would mismatch the batch's row count
 * contract.
 *
 * <p>The recursive reconstruction lives in {@link DremelAssembler}. It restores nesting at every depth: STRUCT wrappers
 * recover per-row validity (null struct versus present struct with null fields), LIST and MAP wrappers recover both
 * per-row validity and the empty-versus-populated distinction, and an explicit null element inside a populated list or
 * map reads back as a present container with a null element in place. {@code List<List<T>>}, {@code Map<K,List<V>>},
 * {@code List<Struct<...>>}, and {@code Struct<List<...>>} all read back fully.
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
        return new ListVector(offsets, child, Validity.allValid(numRows), numRows);
    }

    /**
     * Builds a {@link MapVector} from the two element-aligned child columns (keys and values) and the shared rep-level
     * stream that defines per-row map boundaries.
     *
     * <p>Keys and values share offsets; the rep-level stream of either child works as input. Callers typically pass the
     * rep stream from the key leaf.
     *
     * @param keys per-element vector for the map keys
     * @param values per-element vector for the map values; must share the size of {@code keys}
     * @param repLevels rep-level stream aligned with the element count (one entry per key/value pair)
     * @param numRows the number of top-level rows the map covers
     * @return a {@link MapVector} with {@code numRows} row slots and offsets derived from the rep-level boundaries
     */
    public static MapVector buildMap(ColumnVector keys, ColumnVector values, int[] repLevels, int numRows) {
        int[] offsets = computeTopLevelListOffsets(repLevels, numRows);
        return new MapVector(offsets, keys, values, Validity.allValid(numRows), numRows);
    }

    /**
     * Walks the projected schema and produces a column-vector map keyed by full {@link ColumnPath}:
     *
     * <ul>
     *   <li>Each top-level primitive leaf (no LIST/MAP ancestor) keeps its entry from {@code leafVectors}.
     *   <li>Each STRUCT group gets a {@link StructVector} wrapper at the group's path; its row-aligned child leaves
     *       stay addressable through their leaf paths because struct children share the parent's logical-row
     *       granularity.
     *   <li>Each LIST group gets a {@link ListVector} wrapper at the group's path. Every leaf descended from the LIST
     *       group is removed from the result map - those leaves are at element granularity and would mismatch the
     *       batch's logical-row count.
     *   <li>Each MAP group gets a {@link MapVector} wrapper at the group's path; its descendant leaves are removed for
     *       the same reason.
     * </ul>
     *
     * <p>Nested LIST-of-LIST, MAP-of-LIST, LIST-of-STRUCT, and STRUCT-of-LIST structures are reconstructed at every
     * level (see {@link DremelAssembler}).
     *
     * @param projectedSchema the projected schema describing the column shape
     * @param leafVectors per-leaf flat vectors keyed by their full column path (as produced by BatchRowGroupReader)
     * @param repLevelsByLeaf rep-level stream for every leaf with {@code maxRep > 0}; missing for flat leaves
     * @param defLevelsByLeaf def-level stream for every leaf with {@code maxDef > 0}; missing for required-throughout
     *     leaves. Drives wrapper validity: a leaf entry below a group's definition level marks that group null in the
     *     row.
     * @param numRows logical row count for the batch
     * @return a new map containing the surviving leaves plus one wrapper per LIST / MAP / STRUCT group in the schema
     */
    public static Map<ColumnPath, ColumnVector> assembleNested(
            ParquetSchema projectedSchema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            Map<ColumnPath, int[]> defLevelsByLeaf,
            int numRows) {
        return assembleNestedViews(
                projectedSchema, leafVectors, wholeSlices(repLevelsByLeaf), wholeSlices(defLevelsByLeaf), numRows);
    }

    /**
     * The primary entry point. Each level map is a window view over the producer's page level arrays; callers that hold
     * exactly-sized whole arrays use {@link #assembleNested} instead, which wraps them.
     */
    public static Map<ColumnPath, ColumnVector> assembleNestedViews(
            ParquetSchema projectedSchema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            int numRows) {
        Map<ColumnPath, ColumnVector> result = new HashMap<>(leafVectors);
        Set<ColumnPath> hiddenLeaves = new HashSet<>();
        DremelAssembler dremel = new DremelAssembler(projectedSchema, leafVectors, repLevelsByLeaf, defLevelsByLeaf);
        for (SchemaNode child : projectedSchema.root().children()) {
            walkField(child, new ArrayList<>(), dremel, result, leafVectors, hiddenLeaves, numRows);
        }
        for (ColumnPath leaf : hiddenLeaves) {
            result.remove(leaf);
        }
        return result;
    }

    private static Map<ColumnPath, LevelSlice> wholeSlices(Map<ColumnPath, int[]> levelsByLeaf) {
        Map<ColumnPath, LevelSlice> slices = HashMap.newHashMap(levelsByLeaf.size());
        for (Map.Entry<ColumnPath, int[]> entry : levelsByLeaf.entrySet()) {
            slices.put(entry.getKey(), LevelSlice.ofWhole(entry.getValue()));
        }
        return slices;
    }

    /**
     * Legacy entry point. Equivalent to calling {@link #assembleNested} with empty level-stream maps; only STRUCT
     * wrapping has any input to act on since the LIST and MAP paths need rep-level streams. Kept for tests that
     * exercise STRUCT wrapping in isolation.
     */
    public static Map<ColumnPath, ColumnVector> wrapStructGroups(
            ParquetSchema projectedSchema, Map<ColumnPath, ColumnVector> leafVectors, int numRows) {
        return assembleNested(projectedSchema, leafVectors, Map.of(), Map.of(), numRows);
    }

    // --- schema walk ---

    /**
     * Visits one top-level field. Primitive leaves require no work (they are already in {@code result} at their full
     * path). Group fields are reconstructed by {@link DremelAssembler}:
     *
     * <ul>
     *   <li>LIST or MAP: place the wrapper at the group's path and hide every descendant leaf.
     *   <li>STRUCT: place the {@link StructVector} at the group's path; row-aligned child leaves stay visible, while
     *       leaves descending through a repeated child group are hidden.
     * </ul>
     */
    private static void walkField(
            SchemaNode field,
            List<String> prefix,
            DremelAssembler dremel,
            Map<ColumnPath, ColumnVector> result,
            Map<ColumnPath, ColumnVector> leafVectors,
            Set<ColumnPath> hiddenLeaves,
            int numRows) {
        if (!(field instanceof SchemaNode.Group group)) {
            return;
        }
        List<String> groupPath = concatPath(prefix, group.name());
        ColumnVector vector = dremel.assembleField(group, groupPath, numRows);
        if (vector == null) {
            return;
        }
        result.put(ColumnPath.of(groupPath), vector);
        switch (classify(group)) {
            case LIST, MAP -> markDescendantLeavesHidden(group, groupPath, leafVectors, hiddenLeaves);
            case STRUCT, VARIANT -> hideRepeatedDescendantLeaves(group, groupPath, leafVectors, hiddenLeaves);
        }
    }

    /**
     * Adds every descendant leaf path of {@code group} present in {@code leafVectors} to {@code hiddenLeaves}. Used for
     * LIST and MAP wrappers, whose children are sized at element granularity rather than logical-row granularity.
     */
    private static void markDescendantLeavesHidden(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Set<ColumnPath> hiddenLeaves) {
        for (SchemaNode child : group.children()) {
            List<String> childPath = concatPath(groupPath, child.name());
            if (child instanceof SchemaNode.Primitive) {
                ColumnPath leafPath = ColumnPath.of(childPath);
                if (leafVectors.containsKey(leafPath)) {
                    hiddenLeaves.add(leafPath);
                }
                continue;
            }
            markDescendantLeavesHidden((SchemaNode.Group) child, childPath, leafVectors, hiddenLeaves);
        }
    }

    /**
     * Within a STRUCT, hides only the leaves that descend through a repeated child group. Those leaves are
     * element-granular and live inside the struct's nested LIST / MAP child vector; row-aligned struct leaves stay
     * addressable at their own paths for flat-column access.
     */
    private static void hideRepeatedDescendantLeaves(
            SchemaNode.Group group,
            List<String> groupPath,
            Map<ColumnPath, ColumnVector> leafVectors,
            Set<ColumnPath> hiddenLeaves) {
        for (SchemaNode child : group.children()) {
            if (!(child instanceof SchemaNode.Group childGroup)) {
                continue;
            }
            List<String> childPath = concatPath(groupPath, child.name());
            if (childGroup.repetition() == Repetition.REPEATED || isListOrMap(childGroup)) {
                markDescendantLeavesHidden(childGroup, childPath, leafVectors, hiddenLeaves);
                continue;
            }
            hideRepeatedDescendantLeaves(childGroup, childPath, leafVectors, hiddenLeaves);
        }
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

    // --- schema predicates ---

    private static boolean isListOrMap(SchemaNode.Group group) {
        DremelAssembler.GroupKind kind = classify(group);
        return kind == DremelAssembler.GroupKind.LIST || kind == DremelAssembler.GroupKind.MAP;
    }

    private static DremelAssembler.GroupKind classify(SchemaNode.Group group) {
        return DremelAssembler.classify(group);
    }

    private static List<String> concatPath(List<String> prefix, String segment) {
        List<String> result = new ArrayList<>(prefix.size() + 1);
        result.addAll(prefix);
        result.add(segment);
        return result;
    }
}
