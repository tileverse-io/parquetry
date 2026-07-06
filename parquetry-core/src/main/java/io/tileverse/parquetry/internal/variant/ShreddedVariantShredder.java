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
package io.tileverse.parquetry.internal.variant;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.tileverse.parquetry.variant.ShreddedVariant;
import io.tileverse.parquetry.variant.Variant;
import io.tileverse.parquetry.variant.VariantMetadata;

/**
 * Shreds one {@link Variant} value against a {@link ShreddedVariant} model into the per-row write representation a
 * batch accumulator stages into per-leaf columns. The inverse of {@link ShreddedVariantReconstructor}: where the
 * reconstructor merges typed leaves and the unshredded {@code value} residual back into one value buffer, the shredder
 * splits one value buffer into typed leaves plus a residual.
 *
 * <p>Each model node maps to a {@link VariantShred} node holding at most one of a typed representation or an unshredded
 * {@code value} residual, never both. A scalar whose Variant type matches the typed slot decodes into the typed leaf
 * and leaves the residual null; a non-matching scalar keeps its whole encoded value in the residual. An object present
 * as a shredded object routes its shredded fields into typed leaves and collects every non-shredded field into a
 * residual object encoded against the input value's metadata dictionary; the same dictionary the caller writes verbatim
 * to the {@code metadata} leaf, never rebuilt here. An array present as a shredded array routes every element through
 * the element model.
 */
public final class ShreddedVariantShredder {

    /** The per-row shredded representation of one model node. */
    public sealed interface VariantShred permits ScalarShred, ObjectShred, ArrayShred {}

    /**
     * A shredded scalar node. At most one of {@code typed} and {@code residualValue} is set: {@code typed} holds the
     * physical box when the Variant scalar matched the typed slot; {@code residualValue} holds the whole encoded
     * Variant value when it did not. Both null encodes a Variant null.
     */
    public record ScalarShred(Object typed, MemorySegment residualValue) implements VariantShred {

        public boolean hasTyped() {
            return typed != null;
        }
    }

    /**
     * A shredded object node. {@code present} marks the typed object group present this row, which holds only when the
     * Variant value is an object. When present, {@code fields} holds the per-field shreds and {@code residualValue}
     * holds the non-shredded fields as an encoded object (or null when there are none). When absent,
     * {@code residualValue} holds the whole encoded Variant value and {@code fields} is empty.
     */
    public record ObjectShred(boolean present, Map<String, FieldShred> fields, MemorySegment residualValue)
            implements VariantShred {

        public ObjectShred {
            fields = Map.copyOf(fields);
        }
    }

    /**
     * A shredded array node. {@code present} marks the typed array group present this row, which holds when the Variant
     * value is an array. When present, {@code elements} holds the per-element shreds in order and {@code residualValue}
     * is null. When absent, {@code residualValue} holds the whole encoded Variant value and {@code elements} is empty.
     */
    public record ArrayShred(boolean present, List<VariantShred> elements, MemorySegment residualValue)
            implements VariantShred {

        public ArrayShred {
            elements = List.copyOf(elements);
        }
    }

    /**
     * One shredded object field, mirroring {@link ShreddedVariant.Field}: a {@code value} leaf plus a nested
     * {@code typed_value} shred, each present or absent. An absent field (missing from the Variant object) is both a
     * null {@code value} and an absent {@code typedValue}.
     */
    public record FieldShred(MemorySegment value, VariantShred typedValue) {

        static FieldShred absent() {
            return new FieldShred(null, null);
        }
    }

    /**
     * Shreds {@code value} against {@code model} into its per-row write representation. The {@code metadata} dictionary
     * is the input value's own dictionary, written verbatim to the {@code metadata} leaf and reused here to encode any
     * residual object; it is never rebuilt.
     */
    public VariantShred shred(ShreddedVariant model, Variant value, VariantMetadata metadata) {
        return switch (model) {
            case ShreddedVariant.Scalar scalar -> shredScalar(scalar, value);
            case ShreddedVariant.ShreddedObject object -> shredObject(object, value, metadata);
            case ShreddedVariant.ShreddedArray(ShreddedVariant.Field element) -> shredArray(element, value, metadata);
        };
    }

    private ScalarShred shredScalar(ShreddedVariant.Scalar scalar, Variant value) {
        if (value.type() == Variant.Type.NULL) {
            return new ScalarShred(null, null);
        }
        VariantScalarDecoder.Decoded decoded = VariantScalarDecoder.decode(scalar, value);
        return switch (decoded) {
            case VariantScalarDecoder.Matched(Object physical) -> new ScalarShred(physical, null);
            case VariantScalarDecoder.Mismatch ignored -> new ScalarShred(null, value.value());
        };
    }

    private ObjectShred shredObject(ShreddedVariant.ShreddedObject object, Variant value, VariantMetadata metadata) {
        if (value.type() != Variant.Type.OBJECT) {
            return absentObject(value);
        }
        Map<String, FieldShred> fields = shredObjectFields(object, value, metadata);
        MemorySegment residual = encodeResidualObject(object, value, metadata);
        return new ObjectShred(true, fields, residual);
    }

    private ObjectShred absentObject(Variant value) {
        return new ObjectShred(false, Map.of(), value.value());
    }

    private Map<String, FieldShred> shredObjectFields(
            ShreddedVariant.ShreddedObject object, Variant objectValue, VariantMetadata metadata) {
        Map<String, FieldShred> fields = new LinkedHashMap<>();
        for (Map.Entry<String, ShreddedVariant.Field> entry : object.fields().entrySet()) {
            String name = entry.getKey();
            fields.put(name, shredField(entry.getValue(), objectValue.getField(name), metadata));
        }
        return fields;
    }

    /**
     * Shreds one object field or array element. A field missing from the Variant object is absent. A field whose model
     * declares a nested {@code typed_value} recurses into it; a field declaring only a {@code value} leaf keeps its
     * whole encoded value there. A nested value that does not match its typed slot is lifted to the field's
     * {@code value} leaf rather than nested, mirroring how the reconstructor reads a field's residual from the sibling
     * {@code value} leaf.
     */
    private FieldShred shredField(ShreddedVariant.Field fieldModel, Variant fieldValue, VariantMetadata metadata) {
        if (fieldValue == null) {
            return FieldShred.absent();
        }
        ShreddedVariant typedModel = fieldModel.typedValue();
        if (typedModel == null) {
            return new FieldShred(fieldValue.value(), null);
        }
        return fieldShredOf(typedModel, fieldValue, metadata);
    }

    private FieldShred fieldShredOf(ShreddedVariant typedModel, Variant fieldValue, VariantMetadata metadata) {
        VariantShred nested = shred(typedModel, fieldValue, metadata);
        if (isAbsent(nested)) {
            return new FieldShred(residualOf(nested), null);
        }
        return new FieldShred(null, nested);
    }

    private static boolean isAbsent(VariantShred shred) {
        return switch (shred) {
            case ScalarShred scalar -> !scalar.hasTyped();
            case ObjectShred object -> !object.present();
            case ArrayShred array -> !array.present();
        };
    }

    private static MemorySegment residualOf(VariantShred shred) {
        return switch (shred) {
            case ScalarShred scalar -> scalar.residualValue();
            case ObjectShred object -> object.residualValue();
            case ArrayShred array -> array.residualValue();
        };
    }

    /**
     * Encodes the Variant object's non-shredded fields into a residual {@code value} object against the input metadata
     * dictionary, or returns null when every field is shredded. The shredded field names are excluded; the residual
     * must never collide with a shredded name, matching the reconstructor's merge contract.
     */
    private MemorySegment encodeResidualObject(
            ShreddedVariant.ShreddedObject object, Variant objectValue, VariantMetadata metadata) {
        List<String> names = objectValue.fieldNames();
        VariantEncoder encoder = new VariantEncoder().startObject();
        boolean any = false;
        for (int i = 0; i < names.size(); i++) {
            String name = names.get(i);
            if (object.fields().containsKey(name)) {
                continue;
            }
            encoder.field(name).addEncoded(objectValue.boundedFieldValueAtIndex(i));
            any = true;
        }
        if (!any) {
            return null;
        }
        encoder.endObject();
        return encoder.encodeValue(metadata);
    }

    private ArrayShred shredArray(ShreddedVariant.Field element, Variant value, VariantMetadata metadata) {
        if (value.type() != Variant.Type.ARRAY) {
            return new ArrayShred(false, List.of(), value.value());
        }
        List<VariantShred> elements = shredArrayElements(element, value, metadata);
        return new ArrayShred(true, elements, null);
    }

    private List<VariantShred> shredArrayElements(
            ShreddedVariant.Field element, Variant arrayValue, VariantMetadata metadata) {
        int count = arrayValue.numElements();
        List<VariantShred> elements = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            elements.add(shredElement(element, arrayValue.getElement(i), metadata));
        }
        return elements;
    }

    /**
     * Shreds one array element. The reconstructor reads each element directly through the element's {@code typed_value}
     * model and its {@code value} leaf, not through a field wrapper; an element is therefore its own
     * {@link VariantShred}. An element declaring only a {@code value} leaf keeps its whole encoded value as a residual
     * scalar shred.
     */
    private VariantShred shredElement(ShreddedVariant.Field element, Variant elementValue, VariantMetadata metadata) {
        ShreddedVariant typedModel = element.typedValue();
        if (typedModel == null) {
            return new ScalarShred(null, elementValue.value());
        }
        return shred(typedModel, elementValue, metadata);
    }
}
