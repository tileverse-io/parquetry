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

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.TypedInput;
import io.tileverse.parquetry.columnar.ShreddedVariantVector.VariantInput;
import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ArrayShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.FieldShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ObjectShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.ScalarShred;
import io.tileverse.parquetry.internal.variant.ShreddedVariantShredder.VariantShred;
import io.tileverse.parquetry.variant.ShreddedVariant;

/**
 * Buffers one shredded node's inputs: an optional residual {@code value} leaf plus an optional typed representation,
 * the write counterpart of the read-side {@link VariantInput}. The root node and each object field or array element is
 * a node. A node with a typed scalar that did not match keeps the whole encoded value in its residual leaf, mirroring
 * how the shredder lifts a non-matching scalar to the sibling {@code value} leaf.
 */
final class NodeAccumulator {
    private final ColumnAccumulator.BinaryAccumulator valueAccumulator;
    private final TypedAccumulator typed;

    private NodeAccumulator(ColumnAccumulator.BinaryAccumulator valueAccumulator, TypedAccumulator typed) {
        this.valueAccumulator = valueAccumulator;
        this.typed = typed;
    }

    /** A root node from a model: a residual leaf plus the typed accumulator the model classifies into. */
    static NodeAccumulator forModel(ShreddedVariant model) {
        return new NodeAccumulator(new ColumnAccumulator.BinaryAccumulator(), TypedAccumulator.forModel(model));
    }

    /**
     * A field or element node from a {@link ShreddedVariant.Field} model: a residual leaf present when the model has a
     * {@code value} leaf, and a typed accumulator present when the model nests a {@code typed_value}.
     */
    static NodeAccumulator forField(ShreddedVariant.Field fieldModel) {
        ColumnAccumulator.BinaryAccumulator value =
                fieldModel.value() == null ? null : new ColumnAccumulator.BinaryAccumulator();
        TypedAccumulator typed =
                fieldModel.typedValue() == null ? null : TypedAccumulator.forModel(fieldModel.typedValue());
        return new NodeAccumulator(value, typed);
    }

    /** Stages a whole shred at this node: its residual into the value leaf, its typed portion into the typed slot. */
    void stage(VariantShred shred) {
        stage(residualOf(shred), typedOf(shred));
    }

    /**
     * Stages a field's shred: the field's residual into the value leaf and its nested typed shred into the typed slot.
     */
    void stageField(FieldShred field) {
        stage(field.value(), field.typedValue());
    }

    private void stage(MemorySegment residual, VariantShred typedShred) {
        if (valueAccumulator != null) {
            stageResidual(residual);
        } else {
            rejectUnstorableResidual(residual);
        }
        if (typed != null) {
            stageTyped(typedShred);
        }
    }

    /**
     * A node without a residual {@code value} leaf can only store a conforming value: one fully captured by the typed
     * slot, leaving a null residual. A non-null residual here is a non-conforming value the schema gives no place to
     * keep. Storing it would silently drop the value; fail loudly instead.
     */
    private void rejectUnstorableResidual(MemorySegment residual) {
        if (residual != null) {
            throw new ParquetWriteException("a non-conforming shredded Variant field value cannot be stored because "
                    + "the shredded field has no value residual leaf for a non-conforming value");
        }
    }

    private void stageResidual(MemorySegment residual) {
        if (residual == null) {
            valueAccumulator.setNull();
        } else {
            valueAccumulator.setBinary(residual);
        }
    }

    private void stageTyped(VariantShred typedShred) {
        if (typedShred == null) {
            typed.stageNull();
        } else {
            typed.stage(typedShred);
        }
    }

    void stageNull() {
        if (valueAccumulator != null) {
            valueAccumulator.setNull();
        }
        if (typed != null) {
            typed.stageNull();
        }
    }

    void endRow() {
        if (valueAccumulator != null) {
            valueAccumulator.endRow();
        }
        if (typed != null) {
            typed.endRow();
        }
    }

    VariantInput freeze() {
        BinaryVector value = valueAccumulator == null ? null : (BinaryVector) valueAccumulator.freeze();
        TypedInput typedInput = typed == null ? null : typed.freeze();
        return new VariantInput(value, typedInput);
    }

    void clear() {
        if (valueAccumulator != null) {
            valueAccumulator.clear();
        }
        if (typed != null) {
            typed.clear();
        }
    }

    /**
     * The residual a node's value leaf stores, the same slot the read path reads each node's residual from: a
     * non-matching scalar's encoded value, a present object's non-shredded fields encoded as an object, or an absent
     * object's or array's whole encoded value. A matching scalar and a fully shredded present object or array have a
     * null residual.
     */
    private static MemorySegment residualOf(VariantShred shred) {
        return switch (shred) {
            case ScalarShred scalar -> scalar.residualValue();
            case ObjectShred object -> object.residualValue();
            case ArrayShred array -> array.residualValue();
        };
    }

    /** The typed portion of a shred, or null when the shred has no typed contribution this row. */
    private static VariantShred typedOf(VariantShred shred) {
        return switch (shred) {
            case ScalarShred scalar -> scalar.hasTyped() ? scalar : null;
            case ObjectShred object -> object.present() ? object : null;
            case ArrayShred array -> array.present() ? array : null;
        };
    }
}
