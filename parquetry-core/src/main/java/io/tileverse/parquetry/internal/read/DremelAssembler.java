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
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.Compaction;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ArrayInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ObjectInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ScalarInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.TypedInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.GroupKind;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;

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
    private final Map<ColumnPath, LevelSlice> repLevelsByLeaf;
    private final Map<ColumnPath, LevelSlice> defLevelsByLeaf;

    private final Lists lists = new Lists();
    private final Maps maps = new Maps();
    private final Structs structs = new Structs();
    private final Variants variants = new Variants();

    DremelAssembler(
            ParquetSchema schema,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf) {
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
        return switch (GroupKind.of(group)) {
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
        return leafVectors.get(ColumnPath.of(nodePath));
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
                ColumnPath p = ColumnPath.of(path);
                return leafVectors.containsKey(p) ? p : null;
            }
            return findFirstDescendantLeafPath((SchemaNode.Group) field, path);
        }
    }

    /** Reconstructs a plain struct group: each child assembled at the same slot granularity, plus per-slot validity. */
    final class Structs {

        ColumnVector assemble(SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            Map<ColumnPath, ColumnVector> children = new LinkedHashMap<>();
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
            Validity validity = structValidity(group, groupPath, numSlots);
            return new StructVector(children, validity, numSlots);
        }
    }

    /**
     * Reconstructs a Variant group. An unshredded group has only {@code metadata} and {@code value} binary leaves; a
     * shredded group additionally has a {@code typed_value} child whose subtree shreds scalars, object fields, or array
     * elements into their own leaf vectors.
     */
    final class Variants {

        private static final String METADATA_CHILD = "metadata";
        private static final String VALUE_CHILD = "value";
        private static final String TYPED_VALUE_CHILD = "typed_value";

        ColumnVector assemble(SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            if (findChild(group, TYPED_VALUE_CHILD) != null) {
                return assembleShredded(group, groupPath, parentRepLevel, numSlots);
            }
            return assembleUnshredded(group, groupPath, parentRepLevel, numSlots);
        }

        private ColumnVector assembleUnshredded(
                SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            SchemaNode metadataChild = requireChild(group, METADATA_CHILD);
            SchemaNode valueChild = requireChild(group, VALUE_CHILD);
            BinaryVector metadataVec = (BinaryVector)
                    assembleNode(metadataChild, concat(groupPath, metadataChild.name()), parentRepLevel, numSlots);
            BinaryVector valueVec = (BinaryVector)
                    assembleNode(valueChild, concat(groupPath, valueChild.name()), parentRepLevel, numSlots);
            Validity validity = structValidity(group, groupPath, numSlots);
            return new VariantVector(metadataVec, valueVec, validity, numSlots);
        }

        private ColumnVector assembleShredded(
                SchemaNode.Group group, List<String> groupPath, int parentRepLevel, int numSlots) {
            SchemaNode metadataChild = requireChild(group, METADATA_CHILD);
            BinaryVector metadataVec = (BinaryVector)
                    assembleNode(metadataChild, concat(groupPath, METADATA_CHILD), parentRepLevel, numSlots);
            BinaryVector valueVec = optionalBinaryChild(group, groupPath, VALUE_CHILD, parentRepLevel, numSlots);

            ShreddedVariant model = ShreddedVariant.classify(group);
            SchemaNode typedValueChild = requireChild(group, TYPED_VALUE_CHILD);
            TypedInput typed =
                    buildTyped(typedValueChild, concat(groupPath, TYPED_VALUE_CHILD), parentRepLevel, numSlots);
            VariantInput root = new VariantInput(valueVec, typed);
            Validity validity = structValidity(group, groupPath, numSlots);
            return new ShreddedVariantVector(metadataVec, model, root, validity, numSlots);
        }

        private TypedInput buildTyped(SchemaNode typedValueNode, List<String> path, int parentRepLevel, int numSlots) {
            if (typedValueNode instanceof SchemaNode.Primitive) {
                return new ScalarInput(assembleNode(typedValueNode, path, parentRepLevel, numSlots));
            }
            SchemaNode.Group typedGroup = (SchemaNode.Group) typedValueNode;
            if (isArrayTypedValue(typedGroup)) {
                return buildArray(typedGroup, path, parentRepLevel, numSlots);
            }
            return buildObject(typedGroup, path, parentRepLevel, numSlots);
        }

        /**
         * Builds an array typed_value: per-row offsets and presence from the descendant leaf's level streams, plus the
         * element {@code {value, typed_value}} input compacted to the elements the rows keep. Mirrors {@link Lists} so
         * the offsets and validity match the list reconstruction the rest of the reader produces.
         */
        private ArrayInput buildArray(SchemaNode.Group listGroup, List<String> path, int parentRepLevel, int numSlots) {
            SchemaNode.Group elementGroup = (SchemaNode.Group) lists.elementNode(listGroup);
            List<String> elementPath = lists.elementPath(listGroup, path);
            ColumnPath structureLeaf = findFirstDescendantLeafPath(listGroup, path);
            if (structureLeaf == null) {
                return emptyArray(numSlots);
            }
            RepeatedLayout layout = computeRepeatedLayout(path, structureLeaf, parentRepLevel, numSlots);
            int elementRepLevel = repLevel(repeatedChildPath(path));
            VariantInput rawElement = buildField(elementGroup, elementPath, elementRepLevel, layout.elementCount());
            VariantInput compactedElement = compactInput(rawElement, layout.keptElementIndices());
            return new ArrayInput(layout.offsets(), layout.validity(), compactedElement);
        }

        /**
         * An array with no present descendant leaf: every row is a present-but-empty array. The offsets are all zero
         * and the element input holds no slots.
         */
        private ArrayInput emptyArray(int numSlots) {
            int[] offsets = new int[numSlots + 1];
            VariantInput element = new VariantInput(null, null);
            return new ArrayInput(offsets, Validity.allValid(numSlots), element);
        }

        private boolean isArrayTypedValue(SchemaNode.Group typedGroup) {
            return typedGroup.logicalType().orElse(null) instanceof LogicalType.ListType;
        }

        private ObjectInput buildObject(
                SchemaNode.Group typedGroup, List<String> path, int parentRepLevel, int numSlots) {
            Validity presence = structValidity(typedGroup, path, numSlots);
            Map<String, VariantInput> fields = new LinkedHashMap<>();
            for (SchemaNode fieldNode : typedGroup.children()) {
                SchemaNode.Group fieldGroup = (SchemaNode.Group) fieldNode;
                VariantInput fieldInput =
                        buildField(fieldGroup, concat(path, fieldGroup.name()), parentRepLevel, numSlots);
                fields.put(fieldGroup.name(), fieldInput);
            }
            return new ObjectInput(presence, fields);
        }

        private VariantInput buildField(
                SchemaNode.Group fieldGroup, List<String> fieldPath, int parentRepLevel, int numSlots) {
            BinaryVector fieldValue = optionalBinaryChild(fieldGroup, fieldPath, VALUE_CHILD, parentRepLevel, numSlots);
            SchemaNode typedValueChild = findChild(fieldGroup, TYPED_VALUE_CHILD);
            TypedInput fieldTyped = typedValueChild == null
                    ? null
                    : buildTyped(typedValueChild, concat(fieldPath, TYPED_VALUE_CHILD), parentRepLevel, numSlots);
            return new VariantInput(fieldValue, fieldTyped);
        }

        /**
         * Drops phantom elements from an element input subtree, keeping only the slots {@code keptIndices} retains. The
         * element's {@code value} leaf and its typed representation are each compacted at the same kept indices; the
         * recursion mirrors {@link Compaction} so that a nested array re-indexes its offsets and gathers its own kept
         * child elements.
         */
        private VariantInput compactInput(VariantInput input, int[] keptIndices) {
            BinaryVector value =
                    input.value() == null ? null : (BinaryVector) Compaction.compact(input.value(), keptIndices);
            TypedInput typed = compactTyped(input.typed(), keptIndices);
            return new VariantInput(value, typed);
        }

        private TypedInput compactTyped(TypedInput typed, int[] keptIndices) {
            return switch (typed) {
                case null -> null;
                case ScalarInput(ColumnVector vector) -> new ScalarInput(Compaction.compact(vector, keptIndices));
                case ObjectInput object -> compactObject(object, keptIndices);
                case ArrayInput array -> compactArray(array, keptIndices);
            };
        }

        private ObjectInput compactObject(ObjectInput object, int[] keptIndices) {
            Validity presence = Compaction.gatherValidity(object.presence(), keptIndices);
            Map<String, VariantInput> fields = new LinkedHashMap<>();
            for (Map.Entry<String, VariantInput> field : object.fields().entrySet()) {
                fields.put(field.getKey(), compactInput(field.getValue(), keptIndices));
            }
            return new ObjectInput(presence, fields);
        }

        private ArrayInput compactArray(ArrayInput array, int[] keptIndices) {
            Compaction.OffsetGather gather = Compaction.gatherOffsets(array.offsets(), keptIndices);
            Validity presence = Compaction.gatherValidity(array.presence(), keptIndices);
            VariantInput element = compactInput(array.element(), gather.childIndices());
            return new ArrayInput(gather.offsets(), presence, element);
        }

        private BinaryVector optionalBinaryChild(
                SchemaNode.Group group, List<String> groupPath, String name, int parentRepLevel, int numSlots) {
            SchemaNode child = findChild(group, name);
            if (child == null) {
                return null;
            }
            return (BinaryVector) assembleNode(child, concat(groupPath, name), parentRepLevel, numSlots);
        }

        private SchemaNode requireChild(SchemaNode.Group group, String name) {
            SchemaNode child = findChild(group, name);
            if (child == null) {
                throw new ParquetFormatException("variant group is missing required child '" + name + "'");
            }
            return child;
        }

        private SchemaNode findChild(SchemaNode.Group group, String name) {
            for (SchemaNode child : group.children()) {
                if (child.name().equals(name)) {
                    return child;
                }
            }
            return null;
        }
    }

    /**
     * Per-slot struct validity, read from a descendant leaf's def stream. A struct at definition level 0 is always
     * present; otherwise it is present in a slot when the descendant's def level at the slot's first stream entry
     * reaches the struct's level. Prefers a slot-aligned descendant (one stream entry per slot); a struct whose only
     * leaves live under a deeper repeated node reads any descendant leaf instead, partitioning its stream into slots at
     * the group's own max repetition level.
     */
    Validity structValidity(SchemaNode.Group group, List<String> groupPath, int numSlots) {
        int structDefLevel = maxDef(groupPath);
        if (structDefLevel == 0) {
            return Validity.allValid(numSlots);
        }
        ColumnPath descendant = firstRowAlignedDescendantLeaf(group, groupPath, leafVectors.keySet());
        if (descendant == null) {
            descendant = findFirstDescendantLeafPath(group, groupPath);
        }
        return structValidity(structDefLevel, descendant, repLevel(groupPath), numSlots);
    }

    /**
     * Per-slot struct validity from an already resolved definition level and descendant leaf. Slot {@code k} reads the
     * def level of the {@code k}-th stream entry with {@code rep <= slotBoundaryRepLevel}; for a slot-aligned leaf
     * every entry qualifies and the walk degenerates to entry-per-slot. A {@code null} descendant or an absent def
     * stream leaves the struct all present (there is no level to read, e.g. an all-REQUIRED chain).
     */
    Validity structValidity(int structDefLevel, ColumnPath descendantLeaf, int slotBoundaryRepLevel, int numSlots) {
        if (structDefLevel == 0) {
            return Validity.allValid(numSlots);
        }
        LevelSlice defLevels = descendantLeaf == null ? null : defLevelsByLeaf.get(descendantLeaf);
        if (defLevels == null) {
            return Validity.allValid(numSlots);
        }
        LevelSlice repLevels = repLevelsByLeaf.get(descendantLeaf);
        BitSet validity = new BitSet(numSlots);
        int slot = 0;
        for (int i = 0; i < defLevels.length() && slot < numSlots; i++) {
            if (repLevels != null && repLevels.at(i) > slotBoundaryRepLevel) {
                continue;
            }
            if (defLevels.at(i) >= structDefLevel) {
                validity.set(slot);
            }
            slot++;
        }
        return Validity.of(validity, numSlots);
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
        LevelSlice rep = repLevelsByLeaf.get(structureLeaf);
        LevelSlice def = defLevelsByLeaf.get(structureLeaf);
        int streamLength = rep == null ? 0 : rep.length();

        RepeatedLayoutBuilder builder = new RepeatedLayoutBuilder(numSlots, streamLength);
        for (int i = 0; i < streamLength; i++) {
            if (rep.at(i) <= parentRepLevel && !builder.openSlot(def == null || def.at(i) >= groupDefLevel)) {
                break;
            }
            if (rep.at(i) <= thisRepLevel) {
                builder.addElement(def == null || def.at(i) >= elementDefLevel);
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
    private record RepeatedLayout(int[] offsets, Validity validity, int[] keptElementIndices, int elementCount) {}

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
            return new RepeatedLayout(
                    offsets, Validity.of(validity, numSlots), java.util.Arrays.copyOf(kept, keptCount), elementOrdinal);
        }
    }

    // --- level / path helpers ---

    private int repLevel(List<String> groupPath) {
        return schema.maxLevels(ColumnPath.of(groupPath)).maxRepetitionLevel();
    }

    private int maxDef(List<String> groupPath) {
        return schema.maxLevels(ColumnPath.of(groupPath)).maxDefinitionLevel();
    }

    /**
     * The path of the repeated child that emits a list's or map's elements. For the standard three-level list and for
     * maps this is the inner repeated group ({@code list} or {@code key_value}); for the legacy two-level list it is
     * the repeated primitive directly under the list group. Its repetition level delimits elements; its definition
     * level reports whether at least one element is present.
     */
    private List<String> repeatedChildPath(List<String> listOrMapGroupPath) {
        SchemaNode node = schema.find(ColumnPath.of(listOrMapGroupPath)).orElseThrow();
        SchemaNode repeated = ((SchemaNode.Group) node).children().get(0);
        return concat(listOrMapGroupPath, repeated.name());
    }

    private ColumnPath findFirstDescendantLeafPath(SchemaNode.Group group, List<String> groupPath) {
        for (SchemaNode child : group.children()) {
            List<String> childPath = concat(groupPath, child.name());
            if (child instanceof SchemaNode.Primitive) {
                ColumnPath leafPath = ColumnPath.of(childPath);
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

    /**
     * The leaf whose definition levels report whether {@code group} is present in a slot: the first descendant leaf in
     * {@code presentLeaves} that shares the group's logical-row granularity. Null when the group has none.
     */
    static ColumnPath firstRowAlignedDescendantLeaf(
            SchemaNode.Group group, List<String> groupPath, Set<ColumnPath> presentLeaves) {
        for (SchemaNode child : group.children()) {
            ColumnPath found = rowAlignedLeafUnder(child, concat(groupPath, child.name()), presentLeaves);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    /**
     * The first row-aligned leaf under {@code child}, or null when {@code child} contributes none: a primitive absent
     * from {@code presentLeaves}, or a repeated group (whose leaves are element-aligned, not row-aligned).
     */
    private static ColumnPath rowAlignedLeafUnder(
            SchemaNode child, List<String> childPath, Set<ColumnPath> presentLeaves) {
        if (child instanceof SchemaNode.Primitive) {
            ColumnPath leafPath = ColumnPath.of(childPath);
            return presentLeaves.contains(leafPath) ? leafPath : null;
        }
        SchemaNode.Group childGroup = (SchemaNode.Group) child;
        if (childGroup.repetition() == Repetition.REPEATED) {
            return null;
        }
        return firstRowAlignedDescendantLeaf(childGroup, childPath, presentLeaves);
    }

    private static List<String> concat(List<String> prefix, String segment) {
        List<String> result = new ArrayList<>(prefix.size() + 1);
        result.addAll(prefix);
        result.add(segment);
        return result;
    }
}
