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
package io.tileverse.parquetry.internal.variant;

import java.lang.foreign.MemorySegment;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.variant.ShreddedVariant;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantMetadata;
import io.tileverse.parquetry.variant.VariantScalarValues;

/**
 * Reconstructs the canonical unshredded Variant value bytes for one shredded column row. A {@link ShreddedVariant}
 * model, classified once from the schema, says how each node is shredded; a per-row {@link NodeReader} provides that
 * node's leaf data. The result is the value buffer a conformant Variant writer would have produced for the unshredded
 * value.
 *
 * <p>The model and the reader are walked together. A scalar node emits its typed scalar (or its unshredded value, or a
 * Variant null) as canonical bytes; an object node merges the unshredded {@code value} fields with the shredded
 * {@code typed_value} fields into one ordered object encoded against the shared metadata dictionary.
 */
public final class ShreddedVariantReconstructor {

    /** Per-row access to one shredded node's leaves. Implemented by the vector layer in a later step. */
    public interface NodeReader {

        /** The unshredded value bytes for this node at this row, or null when the value leaf is absent or null. */
        MemorySegment value();

        /** True when this node's scalar typed_value is present (non-null) this row. */
        boolean hasTypedScalar();

        /**
         * The scalar typed_value this row, boxed as the read path boxes it:
         * Integer/Long/Float/Double/Boolean/MemorySegment.
         */
        Object typedScalar();

        /** True when this node's object typed_value group is present this row. */
        boolean hasTypedObject();

        /**
         * Readers for an object typed_value's fields (keyed by field name), or null when the object typed_value is
         * absent this row.
         */
        Map<String, NodeReader> typedObjectFields();

        /** Readers for an array typed_value's elements, or null when absent this row. */
        List<NodeReader> typedArrayElements();
    }

    /** How conflicts between an unshredded value and a shredded typed_value are resolved. */
    public enum Leniency {
        STRICT
    }

    private static final MemorySegment NULL_VALUE =
            MemorySegment.ofArray(new byte[] {0x00}).asReadOnly();

    private final VariantMetadata metadata;
    private final Leniency leniency;

    public ShreddedVariantReconstructor(VariantMetadata metadata, Leniency leniency) {
        this.metadata = metadata;
        this.leniency = leniency;
    }

    /** Reconstruct the canonical value bytes for a top-level shredded variant node. */
    public MemorySegment reconstruct(ShreddedVariant model, NodeReader reader) {
        return reconstructNode(reader, model);
    }

    private MemorySegment reconstructNode(NodeReader reader, ShreddedVariant typedModel) {
        if (typedModel == null) {
            return reconstructUntyped(reader);
        }
        return switch (typedModel) {
            case ShreddedVariant.Scalar scalar -> reconstructScalar(reader, scalar);
            case ShreddedVariant.ShreddedObject object -> reconstructObject(reader, object);
            case ShreddedVariant.ShreddedArray(ShreddedVariant.Field element) -> reconstructArray(reader, element);
        };
    }

    private MemorySegment reconstructUntyped(NodeReader reader) {
        MemorySegment value = reader.value();
        if (value != null) {
            return value;
        }
        return NULL_VALUE;
    }

    private MemorySegment reconstructScalar(NodeReader reader, ShreddedVariant.Scalar scalar) {
        MemorySegment value = reader.value();
        boolean hasValue = value != null;
        boolean hasTyped = reader.hasTypedScalar();
        if (hasValue && hasTyped) {
            throw new ParquetFormatException("Invalid variant, conflicting value and typed_value");
        }
        if (hasTyped) {
            return VariantScalarValues.encode(scalar, reader.typedScalar());
        }
        if (hasValue) {
            return value;
        }
        return NULL_VALUE;
    }

    private MemorySegment reconstructArray(NodeReader reader, ShreddedVariant.Field element) {
        List<NodeReader> elements = reader.typedArrayElements();
        MemorySegment value = reader.value();
        if (elements == null) {
            return value != null ? value : NULL_VALUE;
        }
        if (value != null) {
            throw new ParquetFormatException("Invalid variant, conflicting value and typed_value");
        }
        VariantEncoder encoder = new VariantEncoder().startArray();
        for (NodeReader elementReader : elements) {
            encoder.addEncoded(reconstructNode(elementReader, element.typedValue()));
        }
        encoder.endArray();
        return encoder.encodeValue(metadata);
    }

    private MemorySegment reconstructObject(NodeReader reader, ShreddedVariant.ShreddedObject object) {
        if (!reader.hasTypedObject()) {
            return reconstructUnshreddedObjectRow(reader);
        }
        Map<String, MemorySegment> shreddedFields = new LinkedHashMap<>();
        mergeShreddedFields(reader, object, shreddedFields);

        MemorySegment value = reader.value();
        if (value == null) {
            return encodeObject(shreddedFields);
        }
        return mergeResidualValueIntoShreddedObject(value, object, shreddedFields);
    }

    /**
     * The shredded object group is absent this row, which means the row is not a shredded object. The unshredded
     * {@code value} stands alone as the row's whole value (a scalar, an array, or a SQL null).
     */
    private MemorySegment reconstructUnshreddedObjectRow(NodeReader reader) {
        MemorySegment value = reader.value();
        return value != null ? value : NULL_VALUE;
    }

    /**
     * The shredded object group is present this row. The row is a shredded object and its residual {@code value} must
     * itself be an object whose field names do not collide with the schema's shredded fields.
     */
    private MemorySegment mergeResidualValueIntoShreddedObject(
            MemorySegment value, ShreddedVariant.ShreddedObject object, Map<String, MemorySegment> shreddedFields) {
        Variant unshredded = Variant.of(value, metadata);
        if (unshredded.type() != Variant.Type.OBJECT) {
            throw new ParquetFormatException("Invalid variant, non-object value with shredded fields");
        }
        return encodeObject(mergeUnshreddedObjectFields(unshredded, object, shreddedFields));
    }

    private Map<String, MemorySegment> mergeUnshreddedObjectFields(
            Variant unshredded, ShreddedVariant.ShreddedObject object, Map<String, MemorySegment> shreddedFields) {
        Map<String, MemorySegment> merged = new LinkedHashMap<>();
        List<String> names = unshredded.fieldNames();
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            rejectResidualFieldOnShreddedName(object, name);
            MemorySegment fieldValue = unshredded.boundedFieldValueAtIndex(i);
            merged.put(name, fieldValue);
        }
        for (Map.Entry<String, MemorySegment> shredded : shreddedFields.entrySet()) {
            mergeShreddedField(merged, shredded.getKey(), Optional.of(shredded.getValue()));
        }
        return merged;
    }

    /**
     * A residual {@code value} object must only hold fields the schema does not shred. A residual field whose name is a
     * declared shredded field is a conflict whether or not that shredded field is present at this row.
     */
    private void rejectResidualFieldOnShreddedName(ShreddedVariant.ShreddedObject object, String name) {
        if (object.fields().containsKey(name)) {
            throw new ParquetFormatException("Invalid variant, conflicting value and typed_value for field: " + name);
        }
    }

    private void mergeShreddedFields(
            NodeReader reader, ShreddedVariant.ShreddedObject object, Map<String, MemorySegment> merged) {
        Map<String, NodeReader> fieldReaders = reader.typedObjectFields();
        for (Map.Entry<String, ShreddedVariant.Field> entry : object.fields().entrySet()) {
            String name = entry.getKey();
            NodeReader fieldReader = fieldReaders.get(name);
            if (fieldReader == null) {
                continue;
            }
            Optional<MemorySegment> fieldValue = reconstructField(entry.getValue(), fieldReader);
            mergeShreddedField(merged, name, fieldValue);
        }
    }

    private void mergeShreddedField(
            Map<String, MemorySegment> merged, String name, Optional<MemorySegment> fieldValue) {
        if (fieldValue.isEmpty()) {
            return;
        }
        if (merged.containsKey(name)) {
            onObjectFieldConflict(name);
        }
        merged.put(name, fieldValue.get());
    }

    private Optional<MemorySegment> reconstructField(ShreddedVariant.Field fieldModel, NodeReader fieldReader) {
        ShreddedVariant typed = fieldModel.typedValue();
        if (isFieldAbsent(typed, fieldReader)) {
            return Optional.empty();
        }
        return Optional.of(reconstructNode(fieldReader, typed));
    }

    private boolean isFieldAbsent(ShreddedVariant typed, NodeReader fieldReader) {
        if (fieldReader.value() != null) {
            return false;
        }
        return isTypedValueAbsent(typed, fieldReader);
    }

    private boolean isTypedValueAbsent(ShreddedVariant typed, NodeReader fieldReader) {
        return switch (typed) {
            case null -> true;
            case ShreddedVariant.Scalar ignored -> !fieldReader.hasTypedScalar();
            case ShreddedVariant.ShreddedObject ignored -> !fieldReader.hasTypedObject();
            case ShreddedVariant.ShreddedArray ignored -> fieldReader.typedArrayElements() == null;
        };
    }

    private MemorySegment encodeObject(Map<String, MemorySegment> merged) {
        VariantEncoder encoder = new VariantEncoder().startObject();
        for (Map.Entry<String, MemorySegment> field : merged.entrySet()) {
            encoder.field(field.getKey()).addEncoded(field.getValue());
        }
        encoder.endObject();
        return encoder.encodeValue(metadata);
    }

    private void onObjectFieldConflict(String fieldName) {
        if (leniency == Leniency.STRICT) {
            throw new ParquetFormatException(
                    "Invalid variant, conflicting value and typed_value for field: " + fieldName);
        }
    }
}
