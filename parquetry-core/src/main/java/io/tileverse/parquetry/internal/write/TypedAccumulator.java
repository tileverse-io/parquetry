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
package io.tileverse.parquetry.internal.write;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tileverse.parquetry.columnar.ShreddedVariantVector.ArrayInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ObjectInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.ScalarInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.TypedInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ArrayShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.FieldShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ObjectShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ScalarShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.VariantShred;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant;

/**
 * Buffers a shredded node's {@code typed_value} representation: a scalar leaf, an object group, or an array. The write
 * counterpart of the read-side {@link TypedInput}, built once from the {@link ShreddedVariant} model.
 */
sealed interface TypedAccumulator permits ScalarTypedAccumulator, ObjectTypedAccumulator, ArrayTypedAccumulator {

    static TypedAccumulator forModel(ShreddedVariant model) {
        return switch (model) {
            case ShreddedVariant.Scalar scalar -> new ScalarTypedAccumulator(scalar);
            case ShreddedVariant.ShreddedObject object -> new ObjectTypedAccumulator(object);
            case ShreddedVariant.ShreddedArray(ShreddedVariant.Field element) -> new ArrayTypedAccumulator(element);
        };
    }

    /** Stages this row's typed contribution from the shred. Never called with a shred whose type is absent. */
    void stage(VariantShred shred);

    /** Stages this row as absent: no typed value here. */
    void stageNull();

    void endRow();

    TypedInput freeze();

    void clear();
}

/**
 * A scalar typed_value: one primitive leaf whose physical box the shredder produced. A present scalar shred stages its
 * physical box into the leaf through the setter matching its boxed type; an absent or non-matching scalar leaves the
 * leaf null this row.
 */
final class ScalarTypedAccumulator implements TypedAccumulator {
    private final ColumnAccumulator leaf;

    ScalarTypedAccumulator(ShreddedVariant.Scalar model) {
        SchemaNode.Primitive primitive = model.typedValue();
        this.leaf = ColumnAccumulator.forKind(
                primitive.kind(), primitive.typeLength().orElse(0));
    }

    @Override
    public void stage(VariantShred shred) {
        ScalarShred scalar = (ScalarShred) shred;
        if (scalar.hasTyped()) {
            stagePhysical(scalar.typed());
        } else {
            leaf.setNull();
        }
    }

    /** Routes a physical box into the leaf through the setter matching its boxed type. */
    private void stagePhysical(Object physical) {
        switch (physical) {
            case Boolean value -> leaf.setBoolean(value);
            case Integer value -> leaf.setInt(value);
            case Long value -> leaf.setLong(value);
            case Float value -> leaf.setFloat(value);
            case Double value -> leaf.setDouble(value);
            case MemorySegment value -> leaf.setBinary(value);
            default ->
                throw new ParquetWriteException(
                        "unexpected shredded scalar box: " + physical.getClass().getName());
        }
    }

    @Override
    public void stageNull() {
        leaf.setNull();
    }

    @Override
    public void endRow() {
        leaf.endRow();
    }

    @Override
    public TypedInput freeze() {
        return new ScalarInput(leaf.freeze());
    }

    @Override
    public void clear() {
        leaf.clear();
    }
}

/**
 * An object typed_value group: a per-row presence mask plus one node accumulator per shredded field. A present object
 * shred sets the presence bit and stages each field's shred; an absent object leaves the bit clear and nulls every
 * field, mirroring the read-side {@link ObjectInput} the assembler builds.
 */
final class ObjectTypedAccumulator implements TypedAccumulator {
    private final Map<String, NodeAccumulator> fields;
    private final BitSet present = new BitSet();
    private int rows;
    private boolean pendingPresent;

    ObjectTypedAccumulator(ShreddedVariant.ShreddedObject model) {
        this.fields = LinkedHashMap.newLinkedHashMap(model.fields().size());
        for (Map.Entry<String, ShreddedVariant.Field> entry : model.fields().entrySet()) {
            fields.put(entry.getKey(), NodeAccumulator.forField(entry.getValue()));
        }
    }

    @Override
    public void stage(VariantShred shred) {
        ObjectShred object = (ObjectShred) shred;
        this.pendingPresent = true;
        for (Map.Entry<String, NodeAccumulator> entry : fields.entrySet()) {
            FieldShred field = object.fields().get(entry.getKey());
            entry.getValue().stageField(field == null ? new FieldShred(null, null) : field);
        }
    }

    @Override
    public void stageNull() {
        this.pendingPresent = false;
        for (NodeAccumulator field : fields.values()) {
            field.stageNull();
        }
    }

    @Override
    public void endRow() {
        if (pendingPresent) {
            present.set(rows);
        }
        for (NodeAccumulator field : fields.values()) {
            field.endRow();
        }
        rows++;
        pendingPresent = false;
    }

    @Override
    public TypedInput freeze() {
        Map<String, VariantInput> fieldInputs = LinkedHashMap.newLinkedHashMap(fields.size());
        for (Map.Entry<String, NodeAccumulator> entry : fields.entrySet()) {
            fieldInputs.put(entry.getKey(), entry.getValue().freeze());
        }
        Validity presence = Validity.of((BitSet) present.clone(), rows);
        return new ObjectInput(presence, fieldInputs);
    }

    @Override
    public void clear() {
        for (NodeAccumulator field : fields.values()) {
            field.clear();
        }
        present.clear();
        rows = 0;
        pendingPresent = false;
    }
}

/**
 * An array typed_value: per-row offsets into the element node, plus a per-row presence mask. A present array shred sets
 * the presence bit and stages each element through the element node, advancing the running element count; an absent
 * array leaves the bit clear and advances no elements, mirroring the read-side {@link ArrayInput}.
 */
final class ArrayTypedAccumulator implements TypedAccumulator {
    private final NodeAccumulator element;
    private int[] offsets = new int[17];
    private final BitSet present = new BitSet();
    private int rows;
    private int elementCount;
    private boolean pendingPresent;

    ArrayTypedAccumulator(ShreddedVariant.Field elementModel) {
        this.element = NodeAccumulator.forField(elementModel);
    }

    @Override
    public void stage(VariantShred shred) {
        ArrayShred array = (ArrayShred) shred;
        this.pendingPresent = true;
        for (VariantShred elementShred : array.elements()) {
            element.stage(elementShred);
            element.endRow();
            elementCount++;
        }
    }

    @Override
    public void stageNull() {
        this.pendingPresent = false;
    }

    @Override
    public void endRow() {
        ensureOffsetsCapacity(rows + 1);
        if (pendingPresent) {
            present.set(rows);
        }
        offsets[rows + 1] = elementCount;
        rows++;
        pendingPresent = false;
    }

    @Override
    public TypedInput freeze() {
        VariantInput elementInput = element.freeze();
        Validity presence = Validity.of((BitSet) present.clone(), rows);
        return new ArrayInput(Arrays.copyOf(offsets, rows + 1), presence, elementInput);
    }

    @Override
    public void clear() {
        element.clear();
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
