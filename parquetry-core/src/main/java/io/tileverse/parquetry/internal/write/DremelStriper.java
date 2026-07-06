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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Shreds an eager nested {@link ParquetRecordBatch} into per-leaf {@link StripedLeaf} streams, the write-side inverse
 * of the read-side Dremel assembly. Each {@link ParquetSchema#leafColumns() leaf} becomes one {@code StripedLeaf}
 * holding the leaf's materialized values plus its repetition- and definition-level streams.
 *
 * <p>A non-repeated leaf (a flat REQUIRED / OPTIONAL column or one nested only through STRUCT levels) participates in
 * exactly one entry per top-level row and every repetition level is zero. A repeated leaf (a list or map element)
 * participates in one entry per element occurrence, plus one phantom entry per null or empty container; its repetition
 * levels record where each element begins.
 *
 * <h2>Definition-level arithmetic</h2>
 *
 * For one entry the definition level counts the present OPTIONAL and REPEATED levels along the leaf's chain, from the
 * root down to the first absent or empty one. A present OPTIONAL ancestor or a non-empty REPEATED ancestor adds one and
 * the descent continues; the first null OPTIONAL ancestor or empty REPEATED ancestor adds nothing and stops. REQUIRED
 * ancestors add nothing and never stop the descent. The leaf's own OPTIONAL-ness is the last step: a present optional
 * leaf reaches {@code maxDef} and its value is materialized; a null one stops one short.
 *
 * <h2>Repetition-level synthesis</h2>
 *
 * The first entry produced under a freshly opened parent slot inherits the parent's repetition level; each later
 * sibling at a repeated level emits that repeated level. An entry's repetition level is therefore the level of the
 * shallowest repeated ancestor at which the entry continues an already-open container, generalizing to arbitrary list /
 * map / struct nesting. This inverts {@code DremelAssembler.computeRepeatedLayout}, which partitions the entry stream
 * into slots at {@code rep <= parentRepLevel} and opens elements at {@code rep <= thisRepLevel}.
 */
public final class DremelStriper {

    private final ParquetSchema schema;

    public DremelStriper(ParquetSchema schema) {
        this.schema = schema;
    }

    /** Shreds every leaf of {@code batch} into its value and level streams, one {@link StripedLeaf} per leaf column. */
    public List<StripedLeaf> stripe(ParquetRecordBatch batch) {
        List<StripedLeaf> striped = new ArrayList<>();
        for (ColumnPath leafPath : schema.leafColumns()) {
            striped.add(stripeLeaf(leafPath, batch));
        }
        return striped;
    }

    private StripedLeaf stripeLeaf(ColumnPath leafPath, ParquetRecordBatch batch) {
        LevelMaxima maxima = schema.maxLevels(leafPath);
        if (maxima.maxRepetitionLevel() == 0) {
            return stripeNonRepeated(leafPath, maxima, batch);
        }
        return stripeRepeated(leafPath, maxima, batch);
    }

    // --- non-repeated leaves: one entry per top-level row, repetition levels all zero ---

    /**
     * Shreds one non-repeated leaf over {@code rowCount} rows. Every row contributes one definition level; a row whose
     * definition level reaches {@code maxDef} also contributes its value. Repetition levels are all zero.
     */
    private StripedLeaf stripeNonRepeated(ColumnPath leafPath, LevelMaxima maxima, ParquetRecordBatch batch) {
        LeafChain chain = navigate(leafPath, batch);
        int rowCount = batch.rowCount();
        int maxDef = maxima.maxDefinitionLevel();
        int[] defLevels = new int[rowCount];
        int[] repLevels = new int[rowCount];
        int[] presentRows = new int[rowCount];
        int valueCount = 0;
        for (int row = 0; row < rowCount; row++) {
            int def = definitionLevel(chain, row);
            defLevels[row] = def;
            if (def == maxDef) {
                presentRows[valueCount] = row;
                valueCount++;
            }
        }
        ColumnVector values = gather(chain.leafValues(), presentRows, valueCount);
        return new StripedLeaf(leafPath, values, repLevels, defLevels, valueCount, maxima.maxRepetitionLevel(), maxDef);
    }

    /**
     * The definition level of one row: how many OPTIONAL levels along the leaf's chain are present, from the root down
     * to the first null one. Each present OPTIONAL ancestor adds one; the first null OPTIONAL ancestor stops the climb;
     * the leaf's own presence is the final OPTIONAL step.
     */
    private int definitionLevel(LeafChain chain, int row) {
        int def = 0;
        for (OptionalAncestor ancestor : chain.optionalAncestors()) {
            if (ancestor.validity().isNull(row)) {
                return def;
            }
            def++;
        }
        if (chain.leafIsOptional() && chain.leafValues().isNull(row)) {
            return def;
        }
        return def + (chain.leafIsOptional() ? 1 : 0);
    }

    /**
     * The top-level column vector the leaf descends from. A shredded Variant column is presented as its struct view (a
     * {@link StructVector} of {@code metadata} / {@code value} / {@code typed_value}) so the existing struct and list
     * navigation descends it like any other nested column.
     */
    private ColumnVector topVector(ColumnPath leafPath, ParquetRecordBatch batch) {
        ColumnVector top = batch.columns().get(ColumnPath.of(leafPath.part(0)));
        if (top instanceof ShreddedVariantVector shredded) {
            return shredded.asStructView();
        }
        return top;
    }

    /**
     * Walks the batch from the top-level column down the children chain to the leaf's value vector, recording each
     * enclosing OPTIONAL group's validity (a STRUCT or an unshredded Variant group). Children key on their relative
     * (single-name) path, matching the assembler.
     */
    private LeafChain navigate(ColumnPath leafPath, ParquetRecordBatch batch) {
        List<OptionalAncestor> optionalAncestors = new ArrayList<>();
        ColumnVector current = topVector(leafPath, batch);
        SchemaNode node = childOfRoot(leafPath.part(0));
        recordIfOptionalStruct(node, current, optionalAncestors);
        for (int i = 1; i < leafPath.numParts(); i++) {
            String childName = leafPath.part(i);
            current = childVector(current, childName);
            node = childOf(node, childName);
            recordIfOptionalStruct(node, current, optionalAncestors);
        }
        boolean leafIsOptional = node.repetition() == Repetition.OPTIONAL;
        return new LeafChain(optionalAncestors, current, leafIsOptional);
    }

    private void recordIfOptionalStruct(SchemaNode node, ColumnVector vector, List<OptionalAncestor> ancestors) {
        if (node instanceof SchemaNode.Group && node.repetition() == Repetition.OPTIONAL) {
            ancestors.add(new OptionalAncestor(vector.validity()));
        }
    }

    private ColumnVector childVector(ColumnVector parent, String childName) {
        if (parent instanceof StructVector struct) {
            return struct.children().get(ColumnPath.of(childName));
        }
        if (parent instanceof VariantVector variant) {
            return variantChild(variant, childName);
        }
        throw new IllegalStateException("expected a struct vector while navigating to child '" + childName
                + "' but found " + parent.getClass().getSimpleName());
    }

    /**
     * The {@code metadata} or {@code value} binary leaf of an unshredded Variant group. The read path assembles those
     * two leaves into a {@link VariantVector}; the write path descends back into them by name to stripe each one.
     */
    private ColumnVector variantChild(VariantVector variant, String childName) {
        return switch (childName) {
            case "metadata" -> variant.metadataColumn();
            case "value" -> variant.valueColumn();
            default ->
                throw new IllegalStateException(
                        "a Variant group has no child '" + childName + "'; expected 'metadata' or 'value'");
        };
    }

    /**
     * The leaf vector's value chain: each enclosing OPTIONAL group's validity, the leaf value vector itself, and
     * whether the leaf is itself OPTIONAL.
     */
    private record LeafChain(
            List<OptionalAncestor> optionalAncestors, ColumnVector leafValues, boolean leafIsOptional) {}

    /** One enclosing OPTIONAL group, reduced to the per-row presence mask the definition-level climb reads. */
    private record OptionalAncestor(Validity validity) {}

    // --- repeated leaves: one entry per element occurrence plus phantom entries for null / empty containers ---

    /**
     * Shreds one repeated leaf by walking its schema chain against the eager vectors, emitting the rep / def streams
     * and gathering the materialized element values. Each top-level row opens one parent slot at repetition level zero;
     * the recursion descends list / map / struct levels, synthesizing repetition levels per the Dremel rule.
     */
    private StripedLeaf stripeRepeated(ColumnPath leafPath, LevelMaxima maxima, ParquetRecordBatch batch) {
        List<Step> chain = leafChain(leafPath);
        ColumnVector topVector = topVector(leafPath, batch);
        StripeBuilder builder = new StripeBuilder();
        int rowCount = batch.rowCount();
        for (int row = 0; row < rowCount; row++) {
            emit(chain, 0, topVector, row, 0, 0, builder);
        }
        ColumnVector leafValues = navigateToLeafVector(chain, topVector);
        ColumnVector values = gather(leafValues, builder.keptOrdinals(), builder.valueCount());
        return new StripedLeaf(
                leafPath,
                values,
                builder.repLevels(),
                builder.defLevels(),
                builder.valueCount(),
                maxima.maxRepetitionLevel(),
                maxima.maxDefinitionLevel());
    }

    /**
     * Emits the stream entries for one element of the level at {@code stepIndex}, recursing into deeper levels.
     * {@code vector} is the vector of this level's node, {@code index} the element's position within it,
     * {@code repToEmit} the repetition level the first produced entry must emit, and {@code defSoFar} the definition
     * level accumulated for the present levels above.
     */
    private void emit(
            List<Step> chain,
            int stepIndex,
            ColumnVector vector,
            int index,
            int repToEmit,
            int defSoFar,
            StripeBuilder builder) {
        Step step = chain.get(stepIndex);
        switch (step) {
            case LeafValueStep leaf -> emitLeaf(leaf, vector, index, repToEmit, defSoFar, builder);
            case StructStep _ -> emitStruct(chain, stepIndex, vector, index, repToEmit, defSoFar, builder);
            case RepeatedStep _ -> emitRepeated(chain, stepIndex, vector, index, repToEmit, defSoFar, builder);
        }
    }

    /**
     * Emits the final leaf entry. A REQUIRED leaf always materializes its value at the definition level inherited from
     * the enclosing element. An OPTIONAL leaf adds one final definition step when present and materializes its value; a
     * null OPTIONAL leaf stops one short and materializes nothing.
     */
    private void emitLeaf(
            LeafValueStep step,
            ColumnVector leafValues,
            int index,
            int repToEmit,
            int defSoFar,
            StripeBuilder builder) {
        if (!step.optional()) {
            builder.addEntry(repToEmit, defSoFar, index);
            return;
        }
        boolean present = leafValues.validity().isValid(index);
        int def = defSoFar + (present ? 1 : 0);
        builder.addEntry(repToEmit, def, present ? index : -1);
    }

    /**
     * Emits the entries for one struct slot. A null OPTIONAL struct adds nothing to the definition level and stops the
     * descent at the struct's level; a present struct adds one when OPTIONAL and the descent continues into the single
     * child on the leaf's chain.
     */
    private void emitStruct(
            List<Step> chain,
            int stepIndex,
            ColumnVector vector,
            int index,
            int repToEmit,
            int defSoFar,
            StripeBuilder builder) {
        StructStep step = (StructStep) chain.get(stepIndex);
        StructVector struct = asStruct(vector, step);
        if (step.optional() && struct.validity().isNull(index)) {
            builder.addEntry(repToEmit, defSoFar, -1);
            return;
        }
        int childDef = defSoFar + (step.optional() ? 1 : 0);
        ColumnVector childVector = struct.children().get(ColumnPath.of(step.childName()));
        emit(chain, stepIndex + 1, childVector, index, repToEmit, childDef, builder);
    }

    /**
     * Emits the entries for one element of a repeated list or map level. The eager model collapses the logical list /
     * map group and its repeated child group into one {@link ListVector} / {@link MapVector}; this single step
     * therefore accounts for both definition contributions: a present OPTIONAL outer group adds one, and a non-empty
     * repeated child adds one more. A null container emits one phantom entry at the definition level above the outer
     * group; an empty container emits one phantom entry at the outer group's level; a populated container emits one
     * child element per slot, the first inheriting {@code repToEmit} and the rest emitting this level's repetition
     * level.
     */
    private void emitRepeated(
            List<Step> chain,
            int stepIndex,
            ColumnVector vector,
            int index,
            int repToEmit,
            int defSoFar,
            StripeBuilder builder) {
        RepeatedStep step = (RepeatedStep) chain.get(stepIndex);
        requireRepeatedContainer(vector, step);
        if (vector.validity().isNull(index)) {
            if (!step.groupOptional()) {
                throw new ParquetWriteException(
                        "Required repeated container '" + step.path().dot() + "' is null at row " + index
                                + "; a REQUIRED list or map must be present (possibly empty)");
            }
            builder.addEntry(repToEmit, defSoFar, -1);
            return;
        }
        int groupDef = defSoFar + (step.groupOptional() ? 1 : 0);
        int start = rangeStart(vector, index);
        int end = rangeEnd(vector, index);
        if (start == end) {
            builder.addEntry(repToEmit, groupDef, -1);
            return;
        }
        ColumnVector childVector = step.childVector(vector);
        int elementDef = groupDef + 1;
        for (int element = start; element < end; element++) {
            int repForElement = element == start ? repToEmit : step.repLevel();
            emit(chain, stepIndex + 1, childVector, element, repForElement, elementDef, builder);
        }
    }

    private void requireRepeatedContainer(ColumnVector vector, RepeatedStep step) {
        if (vector instanceof ListVector || vector instanceof MapVector) {
            return;
        }
        throw new IllegalStateException("expected a list or map vector for repeated leaf under '"
                + step.path().dot() + "' but found " + describe(vector));
    }

    private int rangeStart(ColumnVector container, int index) {
        return switch (container) {
            case ListVector list -> list.rowOffsetStart(index);
            case MapVector map -> map.rowOffsetStart(index);
            default -> throw new IllegalStateException("not a repeated container: " + describe(container));
        };
    }

    private int rangeEnd(ColumnVector container, int index) {
        return switch (container) {
            case ListVector list -> list.rowOffsetEnd(index);
            case MapVector map -> map.rowOffsetEnd(index);
            default -> throw new IllegalStateException("not a repeated container: " + describe(container));
        };
    }

    private StructVector asStruct(ColumnVector vector, StructStep step) {
        if (vector instanceof StructVector struct) {
            return struct;
        }
        throw new IllegalStateException(
                "expected a struct vector for '" + step.path().dot() + "' but found " + describe(vector));
    }

    /** Descends the eager vectors along the leaf's chain to recover the flat leaf value vector. */
    private ColumnVector navigateToLeafVector(List<Step> chain, ColumnVector topVector) {
        ColumnVector current = topVector;
        for (Step step : chain) {
            switch (step) {
                case LeafValueStep _ -> {
                    return current;
                }
                case StructStep struct ->
                    current = asStruct(current, struct).children().get(ColumnPath.of(struct.childName()));
                case RepeatedStep repeated -> current = repeated.childVector(current);
            }
        }
        return current;
    }

    // --- leaf schema chain: one step per addressable vector level from the top-level field down to the leaf ---

    /**
     * The leaf's chain of vector-addressable steps. STRUCT levels and the leaf each form one step; a list or map level
     * collapses its logical group and repeated child group into a single {@link RepeatedStep}, matching the eager
     * vector model where one {@link ListVector} / {@link MapVector} embodies both.
     */
    private List<Step> leafChain(ColumnPath leafPath) {
        List<Step> chain = new ArrayList<>();
        List<String> path = new ArrayList<>();
        SchemaNode node = childOfRoot(leafPath.part(0));
        path.add(leafPath.part(0));
        int part = 1;
        while (true) {
            if (node instanceof SchemaNode.Group group && isRepeatedContainer(group)) {
                chain.add(repeatedStep(group, path, leafPath, part));
                node = descendRepeated(group, leafPath, path, part);
                part = path.size();
                continue;
            }
            if (part >= leafPath.numParts()) {
                chain.add(new LeafValueStep(ColumnPath.of(path), node.repetition() == Repetition.OPTIONAL));
                return chain;
            }
            String childName = leafPath.part(part);
            chain.add(new StructStep(ColumnPath.of(path), node.repetition() == Repetition.OPTIONAL, childName));
            node = childOf(node, childName);
            path.add(childName);
            part++;
        }
    }

    private boolean isRepeatedContainer(SchemaNode.Group group) {
        return isList(group) || isMap(group);
    }

    private boolean isList(SchemaNode.Group group) {
        if (group.logicalType().orElse(null) instanceof LogicalType.ListType) {
            return true;
        }
        return group.logicalType().isEmpty() && group.repetition() == Repetition.REPEATED;
    }

    private boolean isMap(SchemaNode.Group group) {
        return group.logicalType().orElse(null) instanceof LogicalType.MapType;
    }

    /**
     * Resolves the element node of a list or map group and advances {@code path} past the collapsed repeated child
     * group. For a list the element is the single descendant of the repeated child; for a map it is the key or value
     * the leaf descends into. The legacy two-level list whose repeated child is itself the primitive element is handled
     * by treating that primitive as the element.
     */
    private SchemaNode descendRepeated(SchemaNode.Group group, ColumnPath leafPath, List<String> path, int part) {
        SchemaNode repeatedChild = group.children().get(0);
        path.add(repeatedChild.name());
        if (repeatedChild instanceof SchemaNode.Primitive) {
            return repeatedChild;
        }
        SchemaNode.Group repeatedGroup = (SchemaNode.Group) repeatedChild;
        int elementPart = part + 1;
        String elementName = leafPath.part(elementPart);
        SchemaNode element = childOf(repeatedGroup, elementName);
        path.add(elementName);
        return element;
    }

    private RepeatedStep repeatedStep(SchemaNode.Group group, List<String> groupPath, ColumnPath leafPath, int part) {
        SchemaNode repeatedChild = group.children().get(0);
        List<String> repeatedChildPath = new ArrayList<>(groupPath);
        repeatedChildPath.add(repeatedChild.name());
        int repLevel = schema.maxLevels(ColumnPath.of(repeatedChildPath)).maxRepetitionLevel();
        boolean groupOptional = group.repetition() == Repetition.OPTIONAL;
        boolean map = isMap(group);
        boolean keyBranch = map && isKeyBranch(group, leafPath, part);
        return new RepeatedStep(ColumnPath.of(groupPath), groupOptional, repLevel, map, keyBranch);
    }

    /**
     * Whether the leaf descends through a map's key branch rather than its value branch. The leaf path segment one past
     * the {@code key_value} group selects key or value.
     */
    private boolean isKeyBranch(SchemaNode.Group mapGroup, ColumnPath leafPath, int part) {
        SchemaNode.Group keyValue = (SchemaNode.Group) mapGroup.children().get(0);
        String keyName = keyValue.children().get(0).name();
        String descended = leafPath.part(part + 1);
        return descended.equals(keyName);
    }

    /** A step that addresses one nested vector level along the leaf's chain. */
    private sealed interface Step permits LeafValueStep, StructStep, RepeatedStep {}

    /** The leaf value level: a present OPTIONAL leaf contributes a final definition step and materializes its value. */
    private record LeafValueStep(ColumnPath path, boolean optional) implements Step {}

    /**
     * A STRUCT level: a present OPTIONAL struct contributes a definition step; the descent continues into one child.
     */
    private record StructStep(ColumnPath path, boolean optional, String childName) implements Step {}

    /**
     * A list or map level, collapsing the logical group and its repeated child group. {@code groupOptional} marks the
     * outer group as a definition step; {@code repLevel} is the repeated child's repetition level, emitted by continued
     * elements; {@code map} and {@code keyBranch} select the key or value child vector of a map.
     */
    private record RepeatedStep(ColumnPath path, boolean groupOptional, int repLevel, boolean map, boolean keyBranch)
            implements Step {

        ColumnVector childVector(ColumnVector container) {
            if (container instanceof ListVector list) {
                return list.child();
            }
            MapVector mapVector = (MapVector) container;
            return keyBranch ? mapVector.keys() : mapVector.values();
        }
    }

    private SchemaNode childOfRoot(String name) {
        return childOf(schema.root(), name);
    }

    private SchemaNode childOf(SchemaNode parent, String name) {
        SchemaNode.Group group = (SchemaNode.Group) parent;
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return child;
            }
        }
        throw new IllegalStateException("schema has no child '" + name + "' under '" + group.name() + "'");
    }

    private String describe(ColumnVector vector) {
        return vector == null ? "null" : vector.getClass().getSimpleName();
    }

    /**
     * Accumulates one repeated leaf's rep / def streams and the source ordinals of its materialized values in a single
     * pass over the eager vectors.
     */
    private static final class StripeBuilder {

        private int[] reps = new int[16];
        private int[] defs = new int[16];
        private int[] kept = new int[16];
        private int entryCount;
        private int valueCount;

        void addEntry(int rep, int def, int keptOrdinal) {
            ensureEntryCapacity();
            reps[entryCount] = rep;
            defs[entryCount] = def;
            entryCount++;
            if (keptOrdinal >= 0) {
                ensureValueCapacity();
                kept[valueCount] = keptOrdinal;
                valueCount++;
            }
        }

        int[] repLevels() {
            return java.util.Arrays.copyOf(reps, entryCount);
        }

        int[] defLevels() {
            return java.util.Arrays.copyOf(defs, entryCount);
        }

        int[] keptOrdinals() {
            return java.util.Arrays.copyOf(kept, valueCount);
        }

        int valueCount() {
            return valueCount;
        }

        private void ensureEntryCapacity() {
            if (entryCount == reps.length) {
                reps = java.util.Arrays.copyOf(reps, reps.length * 2);
                defs = java.util.Arrays.copyOf(defs, defs.length * 2);
            }
        }

        private void ensureValueCapacity() {
            if (valueCount == kept.length) {
                kept = java.util.Arrays.copyOf(kept, kept.length * 2);
            }
        }
    }

    // --- value gathering: shared by both paths ---

    /**
     * Gathers the {@code count} values at {@code sourceOrdinals} into a fresh, contiguous vector. This drops null cells
     * and any value an absent ancestor kept out of the stream, leaving only the values a column-chunk writer emits.
     */
    private ColumnVector gather(ColumnVector source, int[] sourceOrdinals, int count) {
        int[] kept = new int[count];
        System.arraycopy(sourceOrdinals, 0, kept, 0, count);
        return switch (source) {
            case IntVector v -> IntVector.materialized(gatherInts(v, kept), Validity.allValid(count));
            case LongVector v -> LongVector.materialized(gatherLongs(v, kept), Validity.allValid(count));
            case FloatVector v -> FloatVector.materialized(gatherFloats(v, kept), Validity.allValid(count));
            case DoubleVector v -> DoubleVector.materialized(gatherDoubles(v, kept), Validity.allValid(count));
            case BooleanVector v -> BooleanVector.materialized(gatherBooleans(v, kept), Validity.allValid(count));
            case BinaryVector v -> BinaryVector.materialized(gatherSegments(v::get, kept), Validity.allValid(count));
            case FixedLenBinaryVector v ->
                FixedLenBinaryVector.materialized(
                        gatherSegments(v::get, kept), v.byteWidth(), Validity.allValid(count));
            case Int96Vector _ ->
                throw new UnsupportedOperationException(
                        "INT96 is not supported for nested leaf striping; use INT64 with a timestamp logical type");
            default ->
                throw new UnsupportedOperationException(
                        "striping a " + source.getClass().getSimpleName() + " leaf is not supported");
        };
    }

    private int[] gatherInts(IntVector v, int[] kept) {
        int[] out = new int[kept.length];
        for (int i = 0; i < kept.length; i++) {
            out[i] = v.valueAt(kept[i]);
        }
        return out;
    }

    private long[] gatherLongs(LongVector v, int[] kept) {
        long[] out = new long[kept.length];
        for (int i = 0; i < kept.length; i++) {
            out[i] = v.valueAt(kept[i]);
        }
        return out;
    }

    private float[] gatherFloats(FloatVector v, int[] kept) {
        float[] out = new float[kept.length];
        for (int i = 0; i < kept.length; i++) {
            out[i] = v.valueAt(kept[i]);
        }
        return out;
    }

    private double[] gatherDoubles(DoubleVector v, int[] kept) {
        double[] out = new double[kept.length];
        for (int i = 0; i < kept.length; i++) {
            out[i] = v.valueAt(kept[i]);
        }
        return out;
    }

    private boolean[] gatherBooleans(BooleanVector v, int[] kept) {
        boolean[] out = new boolean[kept.length];
        for (int i = 0; i < kept.length; i++) {
            out[i] = v.valueAt(kept[i]);
        }
        return out;
    }

    private MemorySegment[] gatherSegments(IntFunction<MemorySegment> getter, int[] kept) {
        MemorySegment[] out = new MemorySegment[kept.length];
        for (int i = 0; i < kept.length; i++) {
            out[i] = getter.apply(kept[i]);
        }
        return out;
    }
}
