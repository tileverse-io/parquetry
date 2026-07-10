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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.MapVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.TypedInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.columnar.VariantVector;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.VariantShred;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.GroupKind;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;
import io.tileverse.parquetry.variant.Variant;

/**
 * Growable, single-column write buffer. The batch builder stages one cell per row through the typed {@code set*}
 * setters (or {@link #setNull()}), closes each row with {@link #endRow()}, and finally turns the buffered cells into an
 * immutable {@link ColumnVector} via {@link #freeze()}.
 *
 * <p>Each accumulator accepts exactly one Java type matching its Parquet kind. A setter that does not match the
 * column's kind fails with a {@link ParquetWriteException}, catching authoring mistakes at the call site rather than
 * producing a corrupt column.
 */
public sealed interface ColumnAccumulator
        permits ColumnAccumulator.BooleanAccumulator,
                ColumnAccumulator.IntAccumulator,
                ColumnAccumulator.LongAccumulator,
                ColumnAccumulator.FloatAccumulator,
                ColumnAccumulator.DoubleAccumulator,
                ColumnAccumulator.BinaryAccumulator,
                ColumnAccumulator.FixedLenBinaryAccumulator,
                ColumnAccumulator.StructAccumulator,
                ColumnAccumulator.ListAccumulator,
                ColumnAccumulator.MapAccumulator,
                ColumnAccumulator.VariantAccumulator,
                ColumnAccumulator.ShreddedVariantAccumulator {

    /**
     * Creates an accumulator for the given primitive kind. {@code byteWidth} applies only to
     * {@link PrimitiveKind#FIXED_LEN_BYTE_ARRAY} and is ignored otherwise.
     */
    static ColumnAccumulator forKind(PrimitiveKind kind, int byteWidth) {
        return switch (kind) {
            case BOOLEAN -> new BooleanAccumulator();
            case INT32 -> new IntAccumulator();
            case INT64 -> new LongAccumulator();
            case FLOAT -> new FloatAccumulator();
            case DOUBLE -> new DoubleAccumulator();
            case BYTE_ARRAY -> new BinaryAccumulator();
            case FIXED_LEN_BYTE_ARRAY -> new FixedLenBinaryAccumulator(byteWidth);
            case INT96 -> throw new ParquetWriteException("INT96 columns are not supported by the writer");
        };
    }

    /**
     * Creates an accumulator for a struct group field, recursively creating child accumulators for each field in the
     * group.
     */
    static StructAccumulator forGroup(SchemaNode.Group group) {
        List<SchemaNode> children = group.children();
        Map<String, ColumnAccumulator> childAccumulators = LinkedHashMap.newLinkedHashMap(children.size());
        Set<String> requiredContainerChildren = LinkedHashSet.newLinkedHashSet(children.size());
        for (SchemaNode child : children) {
            ColumnAccumulator childAccumulator = forNode(child);
            childAccumulators.put(child.name(), childAccumulator);
            if (isRequiredContainer(child, childAccumulator)) {
                requiredContainerChildren.add(child.name());
            }
        }
        boolean optional = group.repetition() == Repetition.OPTIONAL;
        return new StructAccumulator(childAccumulators, requiredContainerChildren, optional);
    }

    /**
     * Whether a schema node is a REQUIRED list, map, or Variant container. Such a column is authored through a scope
     * verb (beginList / beginMap / setVariant) rather than an index setter; the builder's leaf-indexed required check
     * never reaches it, and the row close validates these separately.
     */
    static boolean isRequiredContainer(SchemaNode child, ColumnAccumulator acc) {
        boolean container = acc instanceof ColumnAccumulator.ListAccumulator
                || acc instanceof ColumnAccumulator.MapAccumulator
                || acc instanceof ColumnAccumulator.VariantAccumulator
                || acc instanceof ColumnAccumulator.ShreddedVariantAccumulator;
        return container && child.repetition() == Repetition.REQUIRED;
    }

    /**
     * Creates an accumulator for any schema node: a primitive leaf, a list group, a map group, or a plain struct group.
     * List and map groups are recognized structurally (a {@link LogicalType.ListType}/{@link LogicalType.MapType}
     * annotation, or a bare repeated group / repeated primitive forming a legacy two-level list), matching the read
     * path's classification.
     */
    static ColumnAccumulator forNode(SchemaNode node) {
        return switch (node) {
            case SchemaNode.Primitive primitive -> {
                if (primitive.repetition() == Repetition.REPEATED) {
                    throw new ParquetWriteException(
                            "Repeated leaf columns are not supported by the writer: " + primitive.name());
                }
                yield forKind(primitive.kind(), primitive.typeLength().orElse(0));
            }
            case SchemaNode.Group group -> forGroupNode(group);
        };
    }

    private static ColumnAccumulator forGroupNode(SchemaNode.Group group) {
        return switch (GroupKind.of(group)) {
            case VARIANT -> forVariant(group);
            case LIST -> forList(group);
            case MAP -> forMap(group);
            case STRUCT -> forGroup(group);
        };
    }

    /**
     * Creates the accumulator for a Variant group. A group with a {@code typed_value} child is a shredded Variant whose
     * typed subtree the read path classifies into a {@link ShreddedVariant}; it accumulates through a
     * {@link ShreddedVariantAccumulator}. A group without one is an unshredded Variant accumulating through a
     * {@link VariantAccumulator}. The detection mirrors the read path's {@code DremelAssembler}.
     */
    private static ColumnAccumulator forVariant(SchemaNode.Group group) {
        if (hasChildNamed(group, "typed_value")) {
            return forShreddedVariant(group);
        }
        return forUnshreddedVariant(group);
    }

    /**
     * Creates an accumulator for an unshredded Variant group: the {@code metadata} and {@code value} binary leaves
     * staged together per row. The two children are not recursed into as struct fields; the Variant column is authored
     * through the builder's Variant verb.
     */
    private static VariantAccumulator forUnshreddedVariant(SchemaNode.Group group) {
        boolean optional = group.repetition() == Repetition.OPTIONAL;
        return new VariantAccumulator(new BinaryAccumulator(), new BinaryAccumulator(), optional);
    }

    /**
     * Creates an accumulator for a shredded Variant group: a {@code metadata} leaf, a residual {@code value} leaf, and
     * the typed subtree classified into a {@link ShreddedVariant} model. Each per-row Variant shreds against the model
     * into typed leaves plus the residual; the Variant column is authored through the builder's Variant verb.
     */
    private static ShreddedVariantAccumulator forShreddedVariant(SchemaNode.Group group) {
        ShreddedVariant model = ShreddedVariant.classify(group);
        boolean optional = group.repetition() == Repetition.OPTIONAL;
        return new ShreddedVariantAccumulator(model, optional);
    }

    private static boolean hasChildNamed(SchemaNode.Group group, String name) {
        for (SchemaNode child : group.children()) {
            if (child.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static ListAccumulator forList(SchemaNode.Group listGroup) {
        SchemaNode element = listElementNode(listGroup);
        ColumnAccumulator elementAccumulator = forElementNode(element);
        rejectShreddedVariantElement(elementAccumulator, listGroup.name());
        boolean optional = listGroup.repetition() == Repetition.OPTIONAL;
        return new ListAccumulator(elementAccumulator, optional);
    }

    /**
     * Rejects a shredded Variant as a list or map element, matching the read path, which cannot reconstruct a shredded
     * Variant nested under a list or map. An unshredded Variant element is allowed.
     */
    private static void rejectShreddedVariantElement(ColumnAccumulator element, String containerColumn) {
        if (element instanceof ShreddedVariantAccumulator) {
            throw new ParquetWriteException(
                    "a shredded Variant nested under a list or map is not supported: " + containerColumn);
        }
    }

    /**
     * Builds the accumulator for a list or map element. A REPEATED primitive element is the legacy two-level encoding
     * whose repetition belongs to the enclosing list, not the value; it accumulates as a scalar of its kind. Any other
     * element follows the general {@link #forNode} classification.
     */
    private static ColumnAccumulator forElementNode(SchemaNode element) {
        if (element instanceof SchemaNode.Primitive primitive && primitive.repetition() == Repetition.REPEATED) {
            return forKind(primitive.kind(), primitive.typeLength().orElse(0));
        }
        return forNode(element);
    }

    /**
     * The element type of a list. A bare repeated group or repeated primitive directly under the list group is the
     * legacy two-level encoding, whose repeated node is the element. The standard three-level encoding wraps the
     * element in a repeated group ({@code repeated group list { <element> }}); the element is that group's single
     * child.
     */
    private static SchemaNode listElementNode(SchemaNode.Group listGroup) {
        SchemaNode repeated = listGroup.children().get(0);
        if (repeated instanceof SchemaNode.Primitive) {
            return repeated;
        }
        SchemaNode.Group repeatedGroup = (SchemaNode.Group) repeated;
        if (repeatedGroup.repetition() != Repetition.REPEATED) {
            return repeated;
        }
        return repeatedGroup.children().get(0);
    }

    private static MapAccumulator forMap(SchemaNode.Group mapGroup) {
        SchemaNode.Group keyValue = (SchemaNode.Group) mapGroup.children().get(0);
        SchemaNode keyNode = keyValue.children().get(0);
        SchemaNode valueNode = keyValue.children().get(1);
        ColumnAccumulator keyAccumulator = forNode(keyNode);
        ColumnAccumulator valueAccumulator = forNode(valueNode);
        rejectShreddedVariantElement(keyAccumulator, mapGroup.name());
        rejectShreddedVariantElement(valueAccumulator, mapGroup.name());
        boolean optional = mapGroup.repetition() == Repetition.OPTIONAL;
        return new MapAccumulator(keyAccumulator, valueAccumulator, optional);
    }

    default void setBoolean(boolean value) {
        throw wrongSetter("boolean");
    }

    default void setInt(int value) {
        throw wrongSetter("int");
    }

    default void setLong(long value) {
        throw wrongSetter("long");
    }

    default void setFloat(float value) {
        throw wrongSetter("float");
    }

    default void setDouble(double value) {
        throw wrongSetter("double");
    }

    default void setBinary(MemorySegment value) {
        throw wrongSetter("binary");
    }

    /** Stages the current row as null. */
    void setNull();

    /**
     * Whether a container column (struct, list, map, or Variant) has been authored present for the current row. Always
     * true for a flat column, whose presence is set by the index setters and whose required-leaf check the builder runs
     * separately. The builder uses this to reject an unset REQUIRED container column.
     */
    default boolean isPendingPresent() {
        return true;
    }

    /** Closes the current row, committing the staged cell or a null into the column buffer. */
    void endRow();

    /**
     * Turns the buffered cells into an immutable column vector. The accumulator may be reused for a new batch after
     * {@link #clear()}; the returned vector is independent of the accumulator's buffers.
     */
    ColumnVector freeze();

    /**
     * Resets the row count and validity for a new batch while keeping the backing arrays at their current capacity.
     * This lets a writer-bound builder reuse one set of accumulators across auto-flush batches without re-allocating.
     */
    void clear();

    private ParquetWriteException wrongSetter(String javaType) {
        return new ParquetWriteException("Column does not accept a " + javaType + " value");
    }

    /**
     * A defensive copy of the validity bits. {@link Validity#of(BitSet, int)} takes ownership of and may mutate the
     * BitSet it is given; because accumulators reuse their {@code valid} field across batches after {@link #clear()},
     * each {@code freeze()} hands {@code Validity} a copy rather than the live field.
     */
    private static BitSet copyValidity(BitSet valid) {
        return (BitSet) valid.clone();
    }

    final class BooleanAccumulator implements ColumnAccumulator {
        private boolean[] values = new boolean[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private boolean staged;

        @Override
        public void setBoolean(boolean value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return BooleanVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class IntAccumulator implements ColumnAccumulator {
        private int[] values = new int[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private int staged;

        @Override
        public void setInt(int value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return IntVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class LongAccumulator implements ColumnAccumulator {
        private long[] values = new long[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private long staged;

        @Override
        public void setLong(long value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return LongVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class FloatAccumulator implements ColumnAccumulator {
        private float[] values = new float[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private float staged;

        @Override
        public void setFloat(float value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return FloatVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    final class DoubleAccumulator implements ColumnAccumulator {
        private double[] values = new double[16];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private double staged;

        @Override
        public void setDouble(double value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            if (rows == values.length) {
                values = Arrays.copyOf(values, values.length * 2);
            }
            if (pending) {
                values[rows] = staged;
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            return DoubleVector.materialized(Arrays.copyOf(values, rows), Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            valid.clear();
        }
    }

    /**
     * Buffers variable-length binary cells into one growable backing array rather than one wrapper segment per cell.
     * The per-row {@code offsets} delimit each cell's bytes; a null row repeats the running offset as a zero-length
     * run, matching {@link BinaryVector}'s consolidated layout.
     */
    final class BinaryAccumulator implements ColumnAccumulator {
        private byte[] backing = new byte[64];
        private int[] offsets = new int[17];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private MemorySegment staged;

        @Override
        public void setBinary(MemorySegment value) {
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            ensureOffsetsCapacity(rows + 1);
            int start = offsets[rows];
            if (pending) {
                int length = Math.toIntExact(staged.byteSize());
                ensureBackingCapacity(start + length);
                MemorySegment.copy(staged, 0L, MemorySegment.ofArray(backing), start, length);
                offsets[rows + 1] = start + length;
                valid.set(rows);
            } else {
                offsets[rows + 1] = start;
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            int usedBytes = offsets[rows];
            MemorySegment backingSegment = MemorySegment.ofArray(Arrays.copyOf(backing, usedBytes));
            IntSequence rowOffsets = IntSequence.of(Arrays.copyOf(offsets, rows + 1));
            return BinaryVector.of(backingSegment, rowOffsets, Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            rows = 0;
            pending = false;
            offsets[0] = 0;
            valid.clear();
        }

        private void ensureOffsetsCapacity(int neededLength) {
            if (neededLength >= offsets.length) {
                offsets = Arrays.copyOf(offsets, Math.max(offsets.length * 2, neededLength + 1));
            }
        }

        private void ensureBackingCapacity(int neededLength) {
            if (neededLength > backing.length) {
                int grown = backing.length * 2;
                backing = Arrays.copyOf(backing, Math.max(grown, neededLength));
            }
        }
    }

    /**
     * Buffers fixed-width binary cells into one growable backing array, each cell occupying its {@code byteWidth} slot
     * in row order. A null row leaves its slot zeroed, matching {@link FixedLenBinaryVector}'s full-slot layout.
     */
    final class FixedLenBinaryAccumulator implements ColumnAccumulator {
        private final int byteWidth;
        private byte[] backing = new byte[64];
        private final BitSet valid = new BitSet();
        private int rows;
        private boolean pending;
        private MemorySegment staged;

        FixedLenBinaryAccumulator(int byteWidth) {
            this.byteWidth = byteWidth;
        }

        @Override
        public void setBinary(MemorySegment value) {
            if (value.byteSize() != byteWidth) {
                throw new ParquetWriteException(
                        "fixed-length column width " + byteWidth + " does not match value width " + value.byteSize());
            }
            this.staged = value;
            this.pending = true;
        }

        @Override
        public void setNull() {
            this.pending = false;
        }

        @Override
        public void endRow() {
            int slotStart = slotBytes();
            ensureBackingCapacity(slotStart + byteWidth);
            if (pending) {
                MemorySegment.copy(staged, 0L, MemorySegment.ofArray(backing), slotStart, byteWidth);
                valid.set(rows);
            }
            rows++;
            pending = false;
        }

        @Override
        public ColumnVector freeze() {
            MemorySegment backingSegment = MemorySegment.ofArray(Arrays.copyOf(backing, slotBytes()));
            return FixedLenBinaryVector.of(backingSegment, byteWidth, Validity.of(copyValidity(valid), rows));
        }

        @Override
        public void clear() {
            zeroSlots();
            rows = 0;
            pending = false;
            valid.clear();
        }

        /**
         * Zeroes the slots a previous batch wrote, because a reused backing must present null slots as zero (a null row
         * does not write its slot in {@link #endRow()}).
         */
        private void zeroSlots() {
            Arrays.fill(backing, 0, slotBytes(), (byte) 0);
        }

        private int slotBytes() {
            return Math.multiplyExact(rows, byteWidth);
        }

        private void ensureBackingCapacity(int neededLength) {
            if (neededLength > backing.length) {
                int grown = backing.length * 2;
                backing = Arrays.copyOf(backing, Math.max(grown, neededLength));
            }
        }
    }

    /**
     * Buffers a struct group column: one child accumulator per field, plus a presence mask tracking which rows have a
     * non-null struct. When the struct is REQUIRED, all rows are always present and no mask is kept.
     *
     * <p>{@link #freeze()} produces a {@link StructVector} whose children are keyed by their single-name relative
     * {@link ColumnPath} and whose validity reflects the per-row presence mask.
     */
    final class StructAccumulator implements ColumnAccumulator {
        private final Map<String, ColumnAccumulator> childAccumulators;
        private final Set<String> requiredContainerChildren;
        private final boolean optional;
        private final BitSet present = new BitSet();
        private int rows;
        // Null structs need to advance all child accumulators with setNull/endRow.
        private boolean pendingPresent;

        StructAccumulator(
                Map<String, ColumnAccumulator> childAccumulators,
                Set<String> requiredContainerChildren,
                boolean optional) {
            this.childAccumulators = childAccumulators;
            this.requiredContainerChildren = requiredContainerChildren;
            this.optional = optional;
        }

        /** Returns the child accumulator for the named field, for use by the builder's struct scope. */
        public ColumnAccumulator child(String fieldName) {
            ColumnAccumulator child = childAccumulators.get(fieldName);
            if (child == null) {
                throw new ParquetWriteException("No such struct field: " + fieldName);
            }
            return child;
        }

        /** Returns an unmodifiable view of the child accumulators map, keyed by field name. */
        public Map<String, ColumnAccumulator> children() {
            return childAccumulators;
        }

        /** Marks the current row as a present (non-null) struct. Called by the builder's endStruct. */
        public void markPresent() {
            this.pendingPresent = true;
        }

        /**
         * Returns whether the struct has been marked present for the current row. Used by the batch builder to decide
         * whether a required leaf inside this struct is reachable (and therefore mandatory) for the row being closed.
         */
        @Override
        public boolean isPendingPresent() {
            return pendingPresent;
        }

        @Override
        public void setNull() {
            this.pendingPresent = false;
        }

        @Override
        public void endRow() {
            if (pendingPresent) {
                rejectUnsetRequiredContainerChildren();
                present.set(rows);
            }
            // Always advance children: a null struct row needs null entries in every child.
            for (ColumnAccumulator child : childAccumulators.values()) {
                if (!pendingPresent) {
                    child.setNull();
                }
                child.endRow();
            }
            rows++;
            pendingPresent = false;
        }

        /**
         * Rejects a present struct that left a REQUIRED list, map, or Variant child unset. An unset REQUIRED container
         * is a programming error, matching an unset required flat leaf; a present-empty container authored with
         * beginList/endList (or beginMap/endMap) is valid. When the struct is absent the children are nulled by this
         * accumulator and the check is skipped; a nested present struct validates its own required containers in turn.
         */
        private void rejectUnsetRequiredContainerChildren() {
            for (String childName : requiredContainerChildren) {
                if (!childAccumulators.get(childName).isPendingPresent()) {
                    throw new ParquetWriteException("Required column '" + childName + "' was not set");
                }
            }
        }

        @Override
        public ColumnVector freeze() {
            Map<ColumnPath, ColumnVector> childVectors = LinkedHashMap.newLinkedHashMap(childAccumulators.size());
            for (Map.Entry<String, ColumnAccumulator> entry : childAccumulators.entrySet()) {
                childVectors.put(ColumnPath.of(entry.getKey()), entry.getValue().freeze());
            }
            Validity validity = optional ? Validity.of((BitSet) present.clone(), rows) : Validity.allValid(rows);
            return new StructVector(childVectors, validity, rows);
        }

        @Override
        public void clear() {
            for (ColumnAccumulator child : childAccumulators.values()) {
                child.clear();
            }
            present.clear();
            rows = 0;
            pendingPresent = false;
        }
    }

    /**
     * Buffers a list column. The single element accumulator buffers list elements (not rows): each
     * {@link #endElement()} advances the element accumulator by one element and grows the element count. A row's
     * {@link #endRow()} writes the running element count as that row's end offset and sets the presence bit when the
     * list is present.
     *
     * <p>The three list states differ only in the element count and presence bit they leave for the row: a populated
     * list advances the element count and is present; an empty list keeps the count unchanged and is present; a null
     * list keeps the count unchanged and is absent. When the list is REQUIRED, every row is present and no mask is
     * kept.
     *
     * <p>{@link #freeze()} produces a {@link ListVector} whose {@code offsets} array has one entry per row plus a final
     * total, whose child is the frozen element vector, and whose validity reflects the per-row presence mask.
     */
    final class ListAccumulator implements ColumnAccumulator {
        private final ColumnAccumulator elementAccumulator;
        private final boolean optional;
        private int[] offsets = new int[17];
        private final BitSet present = new BitSet();
        private int rows;
        private int elementCount;
        private boolean pendingPresent;

        ListAccumulator(ColumnAccumulator elementAccumulator, boolean optional) {
            this.elementAccumulator = elementAccumulator;
            this.optional = optional;
        }

        /** Returns the element accumulator, for the builder's list scope to route element setters into. */
        public ColumnAccumulator element() {
            return elementAccumulator;
        }

        /** Marks the current row as a present (non-null) list. Called by the builder's beginList. */
        public void markPresent() {
            this.pendingPresent = true;
        }

        @Override
        public boolean isPendingPresent() {
            return pendingPresent;
        }

        /** Commits one buffered element into the element accumulator and advances the running element count. */
        public void endElement() {
            elementAccumulator.endRow();
            elementCount++;
        }

        @Override
        public void setNull() {
            this.pendingPresent = false;
        }

        @Override
        public void endRow() {
            ensureOffsetsCapacity(rows + 1);
            if (pendingPresent || !optional) {
                present.set(rows);
            }
            offsets[rows + 1] = elementCount;
            rows++;
            pendingPresent = false;
        }

        @Override
        public ColumnVector freeze() {
            ColumnVector child = elementAccumulator.freeze();
            Validity validity = optional ? Validity.of((BitSet) present.clone(), rows) : Validity.allValid(rows);
            return new ListVector(Arrays.copyOf(offsets, rows + 1), child, validity, rows);
        }

        @Override
        public void clear() {
            elementAccumulator.clear();
            offsets[0] = 0;
            present.clear();
            rows = 0;
            elementCount = 0;
            pendingPresent = false;
        }

        private void ensureOffsetsCapacity(int neededLength) {
            if (neededLength >= offsets.length) {
                offsets = Arrays.copyOf(offsets, Math.max(offsets.length * 2, neededLength + 1));
            }
        }
    }

    /**
     * Buffers a map column. The key and value accumulators buffer map entries (not rows) sharing one offsets array and
     * presence mask: each {@link #endEntry()} advances both accumulators by one entry and grows the entry count. A
     * row's {@link #endRow()} writes the running entry count as that row's end offset and sets the presence bit when
     * the map is present.
     *
     * <p>The empty, populated, and null states behave exactly as in {@link ListAccumulator}; the only difference is the
     * two parallel child accumulators advanced together per entry.
     *
     * <p>{@link #freeze()} produces a {@link MapVector} whose keys and values share the per-row offsets and whose
     * validity reflects the per-row presence mask.
     */
    final class MapAccumulator implements ColumnAccumulator {
        private final ColumnAccumulator keyAccumulator;
        private final ColumnAccumulator valueAccumulator;
        private final boolean optional;
        private int[] offsets = new int[17];
        private final BitSet present = new BitSet();
        private int rows;
        private int entryCount;
        private boolean pendingPresent;

        MapAccumulator(ColumnAccumulator keyAccumulator, ColumnAccumulator valueAccumulator, boolean optional) {
            this.keyAccumulator = keyAccumulator;
            this.valueAccumulator = valueAccumulator;
            this.optional = optional;
        }

        /** Returns the key accumulator, for the builder's map scope to route key setters into. */
        public ColumnAccumulator key() {
            return keyAccumulator;
        }

        /** Returns the value accumulator, for the builder's map scope to route value setters into. */
        public ColumnAccumulator value() {
            return valueAccumulator;
        }

        /** Marks the current row as a present (non-null) map. Called by the builder's beginMap. */
        public void markPresent() {
            this.pendingPresent = true;
        }

        @Override
        public boolean isPendingPresent() {
            return pendingPresent;
        }

        /** Commits one buffered entry into the key and value accumulators and advances the running entry count. */
        public void endEntry() {
            keyAccumulator.endRow();
            valueAccumulator.endRow();
            entryCount++;
        }

        @Override
        public void setNull() {
            this.pendingPresent = false;
        }

        @Override
        public void endRow() {
            ensureOffsetsCapacity(rows + 1);
            if (pendingPresent || !optional) {
                present.set(rows);
            }
            offsets[rows + 1] = entryCount;
            rows++;
            pendingPresent = false;
        }

        @Override
        public ColumnVector freeze() {
            ColumnVector keys = keyAccumulator.freeze();
            ColumnVector values = valueAccumulator.freeze();
            Validity validity = optional ? Validity.of((BitSet) present.clone(), rows) : Validity.allValid(rows);
            return new MapVector(Arrays.copyOf(offsets, rows + 1), keys, values, validity, rows);
        }

        @Override
        public void clear() {
            keyAccumulator.clear();
            valueAccumulator.clear();
            offsets[0] = 0;
            present.clear();
            rows = 0;
            entryCount = 0;
            pendingPresent = false;
        }

        private void ensureOffsetsCapacity(int neededLength) {
            if (neededLength >= offsets.length) {
                offsets = Arrays.copyOf(offsets, Math.max(offsets.length * 2, neededLength + 1));
            }
        }
    }

    /**
     * Buffers an unshredded Variant column: the per-row {@code metadata} and {@code value} binary leaves plus a
     * presence mask tracking which rows hold a non-null Variant. When the Variant group is REQUIRED, all rows are
     * present and no mask is kept.
     *
     * <p>A present row stages its metadata and value bytes into the two child accumulators through
     * {@link #setVariant(MemorySegment, MemorySegment)}; a null row leaves the presence bit clear and pushes a null
     * into both children. {@link #endRow()} advances both children every row, mirroring {@link StructAccumulator}.
     *
     * <p>{@link #freeze()} produces a {@link VariantVector} whose metadata and value columns are the two frozen
     * {@link BinaryVector}s and whose validity reflects the per-row presence mask.
     */
    final class VariantAccumulator implements ColumnAccumulator {
        private final BinaryAccumulator metadataAccumulator;
        private final BinaryAccumulator valueAccumulator;
        private final boolean optional;
        private final BitSet present = new BitSet();
        private int rows;
        private boolean pendingPresent;

        VariantAccumulator(
                BinaryAccumulator metadataAccumulator, BinaryAccumulator valueAccumulator, boolean optional) {
            this.metadataAccumulator = metadataAccumulator;
            this.valueAccumulator = valueAccumulator;
            this.optional = optional;
        }

        /** Stages the current row's Variant: its metadata and value bytes into the two children, marking it present. */
        public void setVariant(MemorySegment metadata, MemorySegment value) {
            metadataAccumulator.setBinary(metadata);
            valueAccumulator.setBinary(value);
            this.pendingPresent = true;
        }

        @Override
        public boolean isPendingPresent() {
            return pendingPresent;
        }

        @Override
        public void setNull() {
            this.pendingPresent = false;
        }

        @Override
        public void endRow() {
            if (pendingPresent) {
                present.set(rows);
            } else {
                metadataAccumulator.setNull();
                valueAccumulator.setNull();
            }
            metadataAccumulator.endRow();
            valueAccumulator.endRow();
            rows++;
            pendingPresent = false;
        }

        @Override
        public ColumnVector freeze() {
            BinaryVector metadataVector = (BinaryVector) metadataAccumulator.freeze();
            BinaryVector valueVector = (BinaryVector) valueAccumulator.freeze();
            Validity validity = optional ? Validity.of((BitSet) present.clone(), rows) : Validity.allValid(rows);
            return new VariantVector(metadataVector, valueVector, validity, rows);
        }

        @Override
        public void clear() {
            metadataAccumulator.clear();
            valueAccumulator.clear();
            present.clear();
            rows = 0;
            pendingPresent = false;
        }
    }

    /**
     * Buffers a shredded Variant column: a {@code metadata} leaf, a {@code value} residual leaf, and the typed subtree
     * classified from the schema into a {@link ShreddedVariant} model. Each present row's {@link Variant} shreds
     * against the model into typed leaves plus a residual through a {@link ShreddedVariantShredder}; a null row leaves
     * the presence bit clear and pushes a null into every leaf. The child accumulators advance once per row, mirroring
     * {@link VariantAccumulator} and {@link StructAccumulator} so no leaf desyncs from the row count.
     *
     * <p>{@link #freeze()} assembles the read-side {@link ShreddedVariantVector} tree from the frozen leaves: the
     * residual leaf and the typed subtree form the root {@link VariantInput}, and the typed accumulator freezes to the
     * matching {@link TypedInput}. The assembly is the write inverse of the read path's {@code DremelAssembler}.
     */
    final class ShreddedVariantAccumulator implements ColumnAccumulator {
        private final ShreddedVariant model;
        private final boolean optional;
        private final BinaryAccumulator metadataAccumulator = new BinaryAccumulator();
        private final NodeAccumulator root;
        private final ShreddedVariantShredder shredder = new ShreddedVariantShredder();
        private final BitSet present = new BitSet();
        private int rows;
        private boolean pendingPresent;

        ShreddedVariantAccumulator(ShreddedVariant model, boolean optional) {
            this.model = model;
            this.optional = optional;
            this.root = NodeAccumulator.forModel(model);
        }

        /**
         * Stages the current row's Variant: its metadata bytes into the metadata leaf, and its shredded representation
         * into the residual leaf and the typed leaves, marking the row present.
         */
        public void setVariant(Variant variant) {
            metadataAccumulator.setBinary(variant.metadata().rawSegment());
            VariantShred shred = shredder.shred(model, variant, variant.metadata());
            root.stage(shred);
            this.pendingPresent = true;
        }

        @Override
        public boolean isPendingPresent() {
            return pendingPresent;
        }

        @Override
        public void setNull() {
            this.pendingPresent = false;
        }

        @Override
        public void endRow() {
            if (pendingPresent) {
                present.set(rows);
            } else {
                metadataAccumulator.setNull();
                root.stageNull();
            }
            metadataAccumulator.endRow();
            root.endRow();
            rows++;
            pendingPresent = false;
        }

        @Override
        public ColumnVector freeze() {
            BinaryVector metadataVector = (BinaryVector) metadataAccumulator.freeze();
            VariantInput rootInput = root.freeze();
            Validity validity = optional ? Validity.of((BitSet) present.clone(), rows) : Validity.allValid(rows);
            return new ShreddedVariantVector(metadataVector, model, rootInput, validity, rows);
        }

        @Override
        public void clear() {
            metadataAccumulator.clear();
            root.clear();
            present.clear();
            rows = 0;
            pendingPresent = false;
        }
    }
}
