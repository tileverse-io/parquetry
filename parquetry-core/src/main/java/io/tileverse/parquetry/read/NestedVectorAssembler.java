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
package io.tileverse.parquetry.read;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;

/**
 * Builds nested-shape vectors from leaf vectors and the structural information in the projected schema.
 *
 * <p>Two entry points:
 *
 * <ul>
 *   <li>{@link #buildList(ColumnVector, int[], int)} turns a child column and its rep-level stream into a
 *       {@link ListVector} with per-row offsets. A leaf entry with {@code repLevel == 0} starts a new top-level row;
 *       higher rep levels append another element to the current row's list.
 *   <li>{@link #wrapStructGroups(ParquetSchema, Map, int)} walks the projected schema and wraps child leaf vectors
 *       under non-repeated, non-annotated group nodes into {@link StructVector} carriers.
 * </ul>
 *
 * <p>LIST/MAP wrapping via rep-level streams is supported; the {@link #buildList} API is present and tested standalone.
 * Its integration into the {@link BatchRowGroupReader} batch pipeline requires exposing the rep-level stream from
 * {@link BatchColumnReader}, which is a follow-up. For now, the production caller only wraps STRUCT groups, which need
 * no rep-level walk.
 *
 * <p>Null-vs-empty disambiguation via def levels is deferred: the validity bitmap produced by {@link #buildList} is set
 * all-true (every row non-null). Def-level-driven null handling lands in a follow-up once fixtures expose failures.
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
     * <p>The validity bitmap is set all-true in this foundation release. Null-row detection via def levels is deferred
     * to a follow-up.
     *
     * @param child the per-element vector that lives inside the list
     * @param repLevels rep-level stream aligned with the child's element count (one entry per child element)
     * @param numRows the number of top-level rows the list covers
     * @return a {@link ListVector} with {@code numRows} row slots and offsets derived from the rep-level boundaries
     */
    public static ListVector buildList(ColumnVector child, int[] repLevels, int numRows) {
        int[] offsets = computeListOffsets(repLevels, numRows);
        BitSet validity = allValid(numRows);
        return new ListVector(offsets, child, validity, numRows);
    }

    /**
     * Walks the projected schema and wraps the child leaf vectors for non-repeated, non-LIST/MAP group nodes into
     * {@link StructVector} carriers, adding each wrapper to the output map at the group's path.
     *
     * <p>Leaf vectors are left unchanged in {@code leafVectors} so accessors that look up leaf paths directly continue
     * to resolve. Only STRUCT groups (non-repeated {@link Field.Group} nodes with no LIST or MAP annotation) are
     * wrapped here; LIST and MAP groups require rep-level streams that are not yet exposed by {@link BatchColumnReader}
     * and are deferred to a later task.
     *
     * @param projectedSchema the projected schema describing the column shape
     * @param leafVectors per-leaf flat vectors keyed by their full column path (as produced by BatchRowGroupReader)
     * @param numRows row count for all vectors in this batch
     * @return a new map containing all entries from {@code leafVectors} plus one {@link StructVector} per STRUCT group
     *     found in the schema
     */
    public static Map<ColumnPath, ColumnVector> wrapStructGroups(
            ParquetSchema projectedSchema, Map<ColumnPath, ColumnVector> leafVectors, int numRows) {
        Map<ColumnPath, ColumnVector> result = new HashMap<>(leafVectors);
        List<String> prefix = new ArrayList<>();
        for (Field child : projectedSchema.root().children()) {
            collectStructWrappers(child, prefix, leafVectors, numRows, result);
        }
        return result;
    }

    // --- ListVector offset computation ---

    /**
     * Derives the offset array for a {@link ListVector} from the rep-level stream.
     *
     * <p>Each element in {@code repLevels} with value {@code 0} marks the start of a new top-level row. The offset for
     * row {@code i} is the index of the first element belonging to that row; the sentinel at position {@code numRows}
     * is {@code repLevels.length}, the total element count.
     */
    private static int[] computeListOffsets(int[] repLevels, int numRows) {
        int[] offsets = new int[numRows + 1];
        int currentRow = -1;
        for (int i = 0; i < repLevels.length; i++) {
            if (repLevels[i] == 0) {
                currentRow++;
                offsets[currentRow] = i;
            }
        }
        offsets[numRows] = repLevels.length;
        return offsets;
    }

    // --- STRUCT group wrapping ---

    /**
     * Recursively visits the schema tree. When a non-repeated, non-LIST/MAP {@link Field.Group} is found, it collects
     * the child vectors under that group and wraps them in a {@link StructVector}. The walk continues into nested
     * structs to handle arbitrarily deep nesting.
     */
    private static void collectStructWrappers(
            Field field,
            List<String> prefix,
            Map<ColumnPath, ColumnVector> leafVectors,
            int numRows,
            Map<ColumnPath, ColumnVector> out) {
        if (!(field instanceof Field.Group group)) {
            return;
        }
        List<String> groupPrefix = new ArrayList<>(prefix);
        groupPrefix.add(group.name());

        if (isStructGroup(group)) {
            StructVector struct = buildStructVector(group, groupPrefix, leafVectors, numRows, out);
            if (struct != null) {
                out.put(
                        new ColumnPath(new ArrayList<>(
                                prefix.isEmpty() ? List.of(group.name()) : concatPath(prefix, group.name()))),
                        struct);
            }
        } else {
            // LIST/MAP groups: descend into children so that any nested STRUCTs inside still get wrapped.
            for (Field child : group.children()) {
                collectStructWrappers(child, groupPrefix, leafVectors, numRows, out);
            }
        }
    }

    /**
     * Builds a {@link StructVector} for a group by collecting the ColumnVectors for each of its direct children.
     * Children that are themselves struct groups are resolved from {@code out} (which is populated depth-first), so
     * this handles nested structs correctly as long as children are processed before parents.
     *
     * <p>Returns {@code null} when none of the group's children have vectors in the current batch (e.g. the group was
     * fully projected out).
     */
    private static StructVector buildStructVector(
            Field.Group group,
            List<String> groupPrefix,
            Map<ColumnPath, ColumnVector> leafVectors,
            int numRows,
            Map<ColumnPath, ColumnVector> out) {
        // First, recursively wrap any nested STRUCT children so they are available in `out` when we look them up.
        for (Field child : group.children()) {
            collectStructWrappers(child, groupPrefix, leafVectors, numRows, out);
        }

        Map<ColumnPath, ColumnVector> childVectors = new HashMap<>();
        for (Field child : group.children()) {
            ColumnPath childRelativePath = ColumnPath.of(child.name());
            ColumnPath childFullPath = new ColumnPath(concatPath(groupPrefix, child.name()));
            // Prefer a wrapped struct child if present; fall back to the flat leaf vector.
            ColumnVector vec = out.getOrDefault(childFullPath, leafVectors.get(childFullPath));
            if (vec != null) {
                childVectors.put(childRelativePath, vec);
            }
        }

        if (childVectors.isEmpty()) {
            return null;
        }
        BitSet validity = allValid(numRows);
        return new StructVector(childVectors, validity, numRows);
    }

    // --- schema predicates ---

    /**
     * Returns {@code true} when the group is a plain STRUCT: non-repeated and carrying no LIST or MAP annotation.
     *
     * <p>A legacy REPEATED group without a LIST annotation is list-like (the legacy assembler treats it as
     * list-of-struct), so it is excluded here.
     */
    private static boolean isStructGroup(Field.Group group) {
        if (group.repetition() == Repetition.REPEATED) {
            return false;
        }
        Optional<LogicalType> annotation = group.logicalType();
        if (annotation.isEmpty()) {
            return true;
        }
        LogicalType lt = annotation.get();
        return !(lt instanceof LogicalType.ListType) && !(lt instanceof LogicalType.MapType);
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
}
