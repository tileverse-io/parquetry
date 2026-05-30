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

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Reconstructs an arbitrarily deep nested column vector tree from per-leaf flat vectors and their repetition- and
 * definition-level streams, applying the inverse of Dremel record shredding.
 *
 * <p>The unit of work is one top-level repeated or grouped field. {@link #assembleField} dispatches on the field's
 * {@link SchemaNode.Group} kind and builds the tree bottom-up: the element type below a list or map is reconstructed
 * first, then wrapped into a {@link ListVector} / {@link MapVector} whose per-row offsets and validity come from the
 * descendant leaf's level streams. The wrapping repeats once per nesting level, leaving {@code List<List<T>>},
 * {@code Map<K,List<V>>}, {@code List<Struct<...>>}, and {@code Struct<List<...>>} fully restored.
 *
 * <p>Each group kind has its own assembler: {@link Lists}, {@link Maps}, {@link Structs}, and {@link Variants} are
 * inner classes that read the shared leaf and level state through the enclosing instance. Compaction (dropping phantom
 * elements from a child vector) is a pure transform over vectors and index arrays, kept in the static
 * {@link Compaction} helper.
 *
 * <h2>Level arithmetic</h2>
 *
 * For a leaf at max repetition level {@code R} and max definition level {@code D}, every entry in its {@code (rep[],
 * def[])} streams describes one shredded value:
 *
 * <ul>
 *   <li>A repetition level {@code r} marks that a new element began at the list/map nested at repetition level
 *       {@code r}. {@code r == 0} starts a new top-level row; {@code r > parentRepLevel} continues the current parent
 *       element by opening a deeper element.
 *   <li>A definition level {@code d} reports how many optional / repeated ancestors are actually present for that
 *       entry. Comparing {@code d} against a node's own definition level distinguishes null (ancestor absent), empty
 *       (container present, no element), and present-with-element.
 * </ul>
 *
 * <p>A repeated group at repetition level {@code r} partitions the entry stream into parent slots at boundaries where
 * {@code rep[i] < r}; within a slot, an entry with {@code rep[i] <= r} opens a new element of that group. Null and
 * empty slots emit one phantom entry that holds no element; phantom entries are dropped from the compacted child,
 * leaving an empty slot with a genuine zero-length slice and a present element where the deeper structure continues.
 */
final class DremelAssembler {

    private final ParquetSchema schema;
    private final Map<ColumnPath, ColumnVector> leafVectors;
    private final Map<ColumnPath, int[]> repLevelsByLeaf;
    private final Map<ColumnPath, int[]> defLevelsByLeaf;

    private final Lists lists = new Lists();
    private final Maps maps = new Maps();
    private final Structs structs = new Structs();
    private final Variants variants = new Variants();

    DremelAssembler(
            ParquetSchema schema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, int[]> repLevelsByLeaf,
            Map<ColumnPath, int[]> defLevelsByLeaf) {
        this.schema = schema;
        this.leafVectors = leafVectors;
        this.repLevelsByLeaf = repLevelsByLeaf;
        this.defLevelsByLeaf = defLevelsByLeaf;
    }

    /**
     * Builds the vector for one top-level group field over {@code numRows} logical rows. Top-level rows are the parent
     * slots delimited at repetition level {@code 0}.
     */
    ColumnVector assembleField(SchemaNode.Group group, List<String> groupPath, int numRows) {
        return assembleGroup(group, groupPath, 0, numRows);
    }

    /**
     * Builds the vector for {@code group} spanning {@code numSlots} parent slots. A parent slot is one element of the
     * enclosing repeated group (or one logical row at the top level), delimited in the descendant leaf's stream by
     * {@code rep[i] <= parentRepLevel}.
     */
    private ColumnVector assembleGroup(
            SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
        return switch (classify(group)) {
            case LIST -> lists.assemble(group, groupPath, parentRepLevel, numSlots);
            case MAP -> maps.assemble(group, groupPath, parentRepLevel, numSlots);
            case STRUCT -> structs.assemble(group, groupPath, parentRepLevel, numSlots);
            case VARIANT -> variants.assemble(group, groupPath, parentRepLevel, numSlots);
        };
    }

    /**
     * Builds the vector for {@code node} spanning {@code numSlots} parent slots. Primitive leaves are already aligned
     * to the parent's element granularity and are returned as decoded.
     */
    private ColumnVector assembleNode(SchemaNode node, List<String> nodePath, int parentRepLevel, int numSlots) {
        if (node instanceof SchemaNode.Group group) {
            return assembleGroup(group, nodePath, parentRepLevel, numSlots);
        }
        return leafVectors.get(new ColumnPath(nodePath));
    }

    /**
     * Reconstructs a list group: the element type below the repeated child, wrapped with per-row offsets and validity.
     */
    final class Lists {

        ColumnVector assemble(SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            SchemaNode element = elementNode(group);
            List<String> elementPath = elementPath(group, groupPath);
            ColumnPath structureLeaf = findFirstDescendantLeafPath(group, groupPath);
            if (structureLeaf == null) {
                return null;
            }
            RepeatedLayout layout = computeRepeatedLayout(groupPath, structureLeaf, parentRepLevel, numSlots);
            int elementRepLevel = repLevel(repeatedChildPath(groupPath));
            ColumnVector child = assembleNode(element, elementPath, elementRepLevel, layout.elementCount());
            if (child == null) {
                return null;
            }
            ColumnVector compacted = Compaction.compact(child, layout.keptElementIndices());
            return new ListVector(layout.offsets(), compacted, layout.validity(), numSlots);
        }

        /**
         * The element type of a list. The standard three-level encoding wraps the element in a repeated group
         * ({@code repeated group list { <element> }}); the element is that group's single child. The legacy two-level
         * encoding puts a repeated primitive directly under the list group, in which case that primitive is the element
         * itself.
         */
        private SchemaNode elementNode(SchemaNode.Group listGroup) {
            SchemaNode repeated = listGroup.children().get(0);
            return switch (repeated) {
                case SchemaNode.Group wrapper -> wrapper.children().get(0);
                case SchemaNode.Primitive element -> element;
            };
        }

        private List<String> elementPath(SchemaNode.Group listGroup, List<String> listGroupPath) {
            SchemaNode repeated = listGroup.children().get(0);
            return switch (repeated) {
                case SchemaNode.Group wrapper ->
                    concat(
                            concat(listGroupPath, wrapper.name()),
                            wrapper.children().get(0).name());
                case SchemaNode.Primitive element -> concat(listGroupPath, element.name());
            };
        }
    }

    /** Reconstructs a map group from its repeated {@code key_value} entries. */
    final class Maps {

        ColumnVector assemble(SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            SchemaNode.Group keyValue = (SchemaNode.Group) group.children().get(0);
            List<String> keyValuePath = concat(groupPath, keyValue.name());
            SchemaNode keyNode = keyValue.children().get(0);
            SchemaNode valueNode = keyValue.children().get(1);
            List<String> keyPath = concat(keyValuePath, keyNode.name());
            List<String> valuePath = concat(keyValuePath, valueNode.name());

            ColumnPath structureLeaf = leafPathOrFirstDescendant(keyNode, keyPath);
            if (structureLeaf == null) {
                structureLeaf = leafPathOrFirstDescendant(valueNode, valuePath);
            }
            if (structureLeaf == null) {
                return null;
            }
            RepeatedLayout layout = computeRepeatedLayout(groupPath, structureLeaf, parentRepLevel, numSlots);
            int entryRepLevel = repLevel(keyValuePath);
            ColumnVector keys = assembleNode(keyNode, keyPath, entryRepLevel, layout.elementCount());
            ColumnVector values = assembleNode(valueNode, valuePath, entryRepLevel, layout.elementCount());
            if (keys == null || values == null) {
                return null;
            }
            ColumnVector compactedKeys = Compaction.compact(keys, layout.keptElementIndices());
            ColumnVector compactedValues = Compaction.compact(values, layout.keptElementIndices());
            return new MapVector(layout.offsets(), compactedKeys, compactedValues, layout.validity(), numSlots);
        }

        private ColumnPath leafPathOrFirstDescendant(SchemaNode field, List<String> path) {
            if (field instanceof SchemaNode.Primitive) {
                ColumnPath p = new ColumnPath(path);
                return leafVectors.containsKey(p) ? p : null;
            }
            return findFirstDescendantLeafPath((SchemaNode.Group) field, path);
        }
    }

    /** Reconstructs a plain struct group: each child assembled at the same slot granularity, plus per-slot validity. */
    final class Structs {

        ColumnVector assemble(SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            Map<ColumnPath, ColumnVector> children = new HashMap<>();
            for (SchemaNode child : group.children()) {
                List<String> childPath = concat(groupPath, child.name());
                ColumnVector childVec = assembleNode(child, childPath, parentRepLevel, numSlots);
                if (childVec != null) {
                    children.put(ColumnPath.of(child.name()), childVec);
                }
            }
            if (children.isEmpty()) {
                return null;
            }
            BitSet validity = structValidity(group, groupPath, numSlots);
            return new StructVector(children, validity, numSlots);
        }
    }

    /** Reconstructs an unshredded Variant group from its {@code metadata} and {@code value} binary leaves. */
    final class Variants {

        private static final String METADATA_CHILD = "metadata";
        private static final String VALUE_CHILD = "value";
        private static final String TYPED_VALUE_CHILD = "typed_value";

        ColumnVector assemble(SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            rejectShredded(group);
            SchemaNode metadataChild = requireChild(group, METADATA_CHILD);
            SchemaNode valueChild = requireChild(group, VALUE_CHILD);
            BinaryVector metadataVec = (BinaryVector)
                    assembleNode(metadataChild, concat(groupPath, metadataChild.name()), parentRepLevel, numSlots);
            BinaryVector valueVec = (BinaryVector)
                    assembleNode(valueChild, concat(groupPath, valueChild.name()), parentRepLevel, numSlots);
            BitSet validity = structValidity(group, groupPath, numSlots);
            return new VariantVector(metadataVec, valueVec, validity, numSlots);
        }

        private void rejectShredded(SchemaNode.Group group) {
            for (SchemaNode child : group.children()) {
                if (child.name().equals(TYPED_VALUE_CHILD)) {
                    throw new ParquetFormatException("shredded variant is not yet supported");
                }
            }
        }

        private SchemaNode requireChild(SchemaNode.Group group, String name) {
            for (SchemaNode child : group.children()) {
                if (child.name().equals(name)) {
                    return child;
                }
            }
            throw new ParquetFormatException("variant group is missing required child '" + name + "'");
        }
    }

    /**
     * Per-slot struct validity from a row-aligned descendant leaf's def stream. A struct at definition level 0 is
     * always present; otherwise it is present in a slot when the descendant's def for that slot reaches the struct's
     * level. A struct whose only descendants live under a repeated child cannot be addressed by slot index here and
     * stays all present; the enclosing list/map already restores its element-level validity.
     */
    private BitSet structValidity(SchemaNode.Group group, List<String> groupPath, int numSlots) {
        int structDefLevel = maxDef(groupPath);
        if (structDefLevel == 0) {
            return allValid(numSlots);
        }
        ColumnPath descendant = firstRowAlignedDescendantLeafPath(group, groupPath);
        int[] defLevels = descendant == null ? null : defLevelsByLeaf.get(descendant);
        if (defLevels == null) {
            return allValid(numSlots);
        }
        BitSet validity = new BitSet(numSlots);
        int limit = Math.min(numSlots, defLevels.length);
        for (int slot = 0; slot < limit; slot++) {
            if (defLevels[slot] >= structDefLevel) {
                validity.set(slot);
            }
        }
        return validity;
    }

    // --- repeated layout: offsets, validity, phantom removal (shared by lists and maps) ---

    /**
     * Computes the per-slot offsets and validity for a repeated group, plus the element-stream indices that survive
     * phantom removal.
     *
     * <p>The descendant {@code structureLeaf}'s rep/def streams are partitioned into the parent's slots at boundaries
     * {@code rep[i] <= parentRepLevel}; inside a slot, every entry with {@code rep[i] <= thisRepLevel} opens an element
     * of this group. Three states per element entry, read from the def level:
     *
     * <ul>
     *   <li>{@code def < groupDefLevel}: the container is null - validity clear, no element kept.
     *   <li>{@code groupDefLevel <= def < elementDefLevel}: the container is present and empty - validity set, no
     *       element kept.
     *   <li>{@code def >= elementDefLevel}: a real element - kept.
     * </ul>
     */
    private RepeatedLayout computeRepeatedLayout(
            List<String> groupPath, ColumnPath structureLeaf, int parentRepLevel, int numSlots) {
        List<String> repeatedChildPath = repeatedChildPath(groupPath);
        int thisRepLevel = repLevel(repeatedChildPath);
        int groupDefLevel = maxDef(groupPath);
        int elementDefLevel = maxDef(repeatedChildPath);
        int[] rep = repLevelsByLeaf.get(structureLeaf);
        int[] def = defLevelsByLeaf.get(structureLeaf);
        int streamLength = rep == null ? 0 : rep.length;

        RepeatedLayoutBuilder builder = new RepeatedLayoutBuilder(numSlots, streamLength);
        for (int i = 0; i < streamLength; i++) {
            if (rep[i] <= parentRepLevel && !builder.openSlot(def == null || def[i] >= groupDefLevel)) {
                break;
            }
            if (rep[i] <= thisRepLevel) {
                builder.addElement(def == null || def[i] >= elementDefLevel);
            }
        }
        return builder.build();
    }

    /**
     * Per-slot offsets and validity for a repeated group, the surviving child-element indices, and the total element
     * count the child recursion produced (kept plus phantom elements). The kept indices drive compaction of the child
     * vector down to the elements an enclosing container keeps.
     */
    @SuppressWarnings("java:S6218") // internal layout carrier, never compared by value
    private record RepeatedLayout(int[] offsets, BitSet validity, int[] keptElementIndices, int elementCount) {}

    /**
     * Accumulates the per-slot offsets, validity, and surviving element indices for one repeated group in a single pass
     * over its descendant level stream.
     */
    private static final class RepeatedLayoutBuilder {

        private final int numSlots;
        private final int[] offsets;
        private final BitSet validity;
        private final int[] kept;
        private int keptCount;
        private int elementCount;
        private int slot = -1;
        private int elementOrdinal;

        RepeatedLayoutBuilder(int numSlots, int streamLength) {
            this.numSlots = numSlots;
            this.offsets = new int[numSlots + 1];
            this.validity = new BitSet(numSlots);
            this.kept = new int[streamLength];
        }

        /** Opens a parent slot; returns false once the slots are exhausted, signalling the caller to stop. */
        boolean openSlot(boolean present) {
            slot++;
            if (slot >= numSlots) {
                return false;
            }
            offsets[slot] = elementCount;
            if (present) {
                validity.set(slot);
            }
            return true;
        }

        void addElement(boolean present) {
            if (present) {
                kept[keptCount++] = elementOrdinal;
                elementCount++;
            }
            elementOrdinal++;
        }

        RepeatedLayout build() {
            for (int s = slot + 1; s <= numSlots; s++) {
                offsets[s] = elementCount;
            }
            return new RepeatedLayout(offsets, validity, java.util.Arrays.copyOf(kept, keptCount), elementOrdinal);
        }
    }

    // --- level / path helpers ---

    private int repLevel(List<String> groupPath) {
        return schema.maxLevels(new ColumnPath(groupPath)).maxRepetitionLevel();
    }

    private int maxDef(List<String> groupPath) {
        return schema.maxLevels(new ColumnPath(groupPath)).maxDefinitionLevel();
    }

    /**
     * The path of the repeated child that emits a list's or map's elements. For the standard three-level list and for
     * maps this is the inner repeated group ({@code list} or {@code key_value}); for the legacy two-level list it is
     * the repeated primitive directly under the list group. Its repetition level delimits elements; its definition
     * level reports whether at least one element is present.
     */
    private List<String> repeatedChildPath(List<String> listOrMapGroupPath) {
        SchemaNode node = schema.find(new ColumnPath(listOrMapGroupPath)).orElseThrow();
        SchemaNode repeated = ((SchemaNode.Group) node).children().get(0);
        return concat(listOrMapGroupPath, repeated.name());
    }

    private ColumnPath findFirstDescendantLeafPath(SchemaNode.Group group, List<String> groupPath) {
        for (SchemaNode child : group.children()) {
            List<String> childPath = concat(groupPath, child.name());
            if (child instanceof SchemaNode.Primitive) {
                ColumnPath leafPath = new ColumnPath(childPath);
                if (leafVectors.containsKey(leafPath)) {
                    return leafPath;
                }
                continue;
            }
            ColumnPath nested = findFirstDescendantLeafPath((SchemaNode.Group) child, childPath);
            if (nested != null) {
                return nested;
            }
        }
        return null;
    }

    private ColumnPath firstRowAlignedDescendantLeafPath(SchemaNode.Group group, List<String> groupPath) {
        for (SchemaNode child : group.children()) {
            ColumnPath found = rowAlignedLeafUnder(child, concat(groupPath, child.name()));
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * The first row-aligned leaf under {@code child}, or null when {@code child} contributes none: a primitive absent
     * from {@code leafVectors}, or a repeated group (whose leaves are element-aligned, not row-aligned).
     */
    private ColumnPath rowAlignedLeafUnder(SchemaNode child, List<String> childPath) {
        if (child instanceof SchemaNode.Primitive) {
            ColumnPath leafPath = new ColumnPath(childPath);
            return leafVectors.containsKey(leafPath) ? leafPath : null;
        }
        SchemaNode.Group childGroup = (SchemaNode.Group) child;
        if (childGroup.repetition() == Repetition.REPEATED) {
            return null;
        }
        return firstRowAlignedDescendantLeafPath(childGroup, childPath);
    }

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }

    private static List<String> concat(List<String> prefix, String segment) {
        List<String> result = new ArrayList<>(prefix.size() + 1);
        result.addAll(prefix);
        result.add(segment);
        return result;
    }

    // --- classification ---

    static GroupKind classify(SchemaNode.Group group) {
        Optional<LogicalType> annotation = group.logicalType();
        if (annotation.isPresent()) {
            LogicalType lt = annotation.get();
            if (lt instanceof LogicalType.Variant) {
                return GroupKind.VARIANT;
            }
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

    enum GroupKind {
        LIST,
        MAP,
        STRUCT,
        VARIANT
    }

    /**
     * Drops phantom elements from an assembled child, keeping only the elements an enclosing container retains. A pure
     * transform over vectors and index arrays: it reads no leaf or level state, recursing into nested
     * {@link ListVector} / {@link MapVector} / {@link StructVector} / {@link VariantVector} children by gathering their
     * per-row state at the kept indices.
     */
    private static final class Compaction {

        private Compaction() {}

        static ColumnVector compact(ColumnVector child, int[] keptIndices) {
            if (child == null) {
                return null;
            }
            if (keptIndices.length == child.size()) {
                return child;
            }
            return switch (child) {
                case IntVector v -> {
                    int[] ints = gatherInts(v, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield IntVector.materialized(ints, validity);
                }
                case LongVector v -> {
                    long[] longs = gatherLongs(v, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield LongVector.materialized(longs, validity);
                }
                case FloatVector v -> {
                    float[] floats = gatherFloats(v, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield FloatVector.materialized(floats, validity);
                }
                case DoubleVector v -> {
                    double[] doubles = gatherDoubles(v, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield DoubleVector.materialized(doubles, validity);
                }
                case BooleanVector v -> {
                    boolean[] booleans = gatherBooleans(v, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield BooleanVector.materialized(booleans, validity);
                }
                case BinaryVector v -> {
                    MemorySegment[] segments = gatherSegments(v::get, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield BinaryVector.materialized(segments, validity);
                }
                case FixedLenBinaryVector v -> {
                    MemorySegment[] segments = gatherSegments(v::get, keptIndices);
                    int byteWidth = v.byteWidth();
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield FixedLenBinaryVector.materialized(segments, byteWidth, validity);
                }
                case Int96Vector v -> {
                    MemorySegment[] segments = gatherSegments(v::get, keptIndices);
                    BitSet validity = gatherValidity(v, keptIndices);
                    yield Int96Vector.materialized(segments, validity);
                }
                case ListVector v -> compactList(v, keptIndices);
                case MapVector v -> compactMap(v, keptIndices);
                case StructVector v -> compactStruct(v, keptIndices);
                case VariantVector v -> compactVariant(v, keptIndices);
            };
        }

        private static VariantVector compactVariant(VariantVector v, int[] keptIndices) {
            BinaryVector metadata = (BinaryVector) compact(v.metadataColumn(), keptIndices);
            BinaryVector value = (BinaryVector) compact(v.valueColumn(), keptIndices);
            return new VariantVector(metadata, value, gatherValidity(v, keptIndices), keptIndices.length);
        }

        private static ListVector compactList(ListVector v, int[] keptIndices) {
            ChildGather gather = gatherNestedRows(v::rowOffsetStart, v::rowOffsetEnd, keptIndices);
            ColumnVector compactedChild = compact(v.child(), gather.childIndices());
            return new ListVector(gather.offsets(), compactedChild, gatherValidity(v, keptIndices), keptIndices.length);
        }

        private static MapVector compactMap(MapVector v, int[] keptIndices) {
            ChildGather gather = gatherNestedRows(v::rowOffsetStart, v::rowOffsetEnd, keptIndices);
            ColumnVector compactedKeys = compact(v.keys(), gather.childIndices());
            ColumnVector compactedValues = compact(v.values(), gather.childIndices());
            return new MapVector(
                    gather.offsets(),
                    compactedKeys,
                    compactedValues,
                    gatherValidity(v, keptIndices),
                    keptIndices.length);
        }

        private static StructVector compactStruct(StructVector v, int[] keptIndices) {
            Map<ColumnPath, ColumnVector> children = new HashMap<>();
            for (Map.Entry<ColumnPath, ColumnVector> entry : v.children().entrySet()) {
                children.put(entry.getKey(), compact(entry.getValue(), keptIndices));
            }
            return new StructVector(children, gatherValidity(v, keptIndices), keptIndices.length);
        }

        /**
         * Reindexes a nested container's offsets to the kept parent rows, collecting the child-element indices each
         * kept row points at into a flat, contiguous index list.
         */
        private static ChildGather gatherNestedRows(
                java.util.function.IntUnaryOperator startOf,
                java.util.function.IntUnaryOperator endOf,
                int[] keptIndices) {
            int[] offsets = new int[keptIndices.length + 1];
            List<Integer> childIndices = new ArrayList<>();
            int running = 0;
            for (int i = 0; i < keptIndices.length; i++) {
                offsets[i] = running;
                int start = startOf.applyAsInt(keptIndices[i]);
                int end = endOf.applyAsInt(keptIndices[i]);
                for (int e = start; e < end; e++) {
                    childIndices.add(e);
                }
                running += end - start;
            }
            offsets[keptIndices.length] = running;
            int[] childIndexArray = new int[childIndices.size()];
            for (int i = 0; i < childIndexArray.length; i++) {
                childIndexArray[i] = childIndices.get(i);
            }
            return new ChildGather(offsets, childIndexArray);
        }

        @SuppressWarnings("java:S6218") // internal gather carrier, never compared by value
        private record ChildGather(int[] offsets, int[] childIndices) {}

        private static int[] gatherInts(IntVector v, int[] keptIndices) {
            int[] out = new int[keptIndices.length];
            for (int i = 0; i < keptIndices.length; i++) {
                out[i] = v.get(keptIndices[i]);
            }
            return out;
        }

        private static long[] gatherLongs(LongVector v, int[] keptIndices) {
            long[] out = new long[keptIndices.length];
            for (int i = 0; i < keptIndices.length; i++) {
                out[i] = v.get(keptIndices[i]);
            }
            return out;
        }

        private static float[] gatherFloats(FloatVector v, int[] keptIndices) {
            float[] out = new float[keptIndices.length];
            for (int i = 0; i < keptIndices.length; i++) {
                out[i] = v.get(keptIndices[i]);
            }
            return out;
        }

        private static double[] gatherDoubles(DoubleVector v, int[] keptIndices) {
            double[] out = new double[keptIndices.length];
            for (int i = 0; i < keptIndices.length; i++) {
                out[i] = v.get(keptIndices[i]);
            }
            return out;
        }

        private static boolean[] gatherBooleans(BooleanVector v, int[] keptIndices) {
            boolean[] out = new boolean[keptIndices.length];
            for (int i = 0; i < keptIndices.length; i++) {
                out[i] = v.get(keptIndices[i]);
            }
            return out;
        }

        private static MemorySegment[] gatherSegments(IntFunction<MemorySegment> getter, int[] keptIndices) {
            MemorySegment[] out = new MemorySegment[keptIndices.length];
            for (int i = 0; i < keptIndices.length; i++) {
                out[i] = getter.apply(keptIndices[i]);
            }
            return out;
        }

        private static BitSet gatherValidity(ColumnVector v, int[] keptIndices) {
            BitSet source = v.validity();
            BitSet out = new BitSet(keptIndices.length);
            for (int i = 0; i < keptIndices.length; i++) {
                if (source.get(keptIndices[i])) {
                    out.set(i);
                }
            }
            return out;
        }
    }
}
