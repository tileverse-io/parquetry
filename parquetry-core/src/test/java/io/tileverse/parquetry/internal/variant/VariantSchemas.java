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

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Builds {@code SchemaNode.Group} variant-column fixtures the classifier tests need. */
final class VariantSchemas {

    static SchemaNode.Group scalarInt64Group() {
        SchemaNode.Primitive typedValue = primitive("typed_value", PrimitiveKind.INT64, Optional.empty());
        return variantGroup(typedValue);
    }

    static SchemaNode.Group scalarUnsignedInt32Group() {
        Optional<LogicalType> unsigned = Optional.of(new LogicalType.IntType((byte) 32, false));
        SchemaNode.Primitive typedValue = primitive("typed_value", PrimitiveKind.INT32, unsigned);
        return variantGroup(typedValue);
    }

    static SchemaNode.Group scalarByteArrayDecimalGroup() {
        Optional<LogicalType> decimal = Optional.of(new LogicalType.Decimal(9, 38));
        SchemaNode.Primitive typedValue = primitive("typed_value", PrimitiveKind.BYTE_ARRAY, decimal);
        return variantGroup(typedValue);
    }

    static SchemaNode.Group fixedLen4Group() {
        SchemaNode.Primitive typedValue = new SchemaNode.Primitive(
                "typed_value",
                Repetition.OPTIONAL,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(4),
                Optional.empty(),
                -1);
        return variantGroup(typedValue);
    }

    static SchemaNode.Group objectTwoScalarFieldsGroup() {
        SchemaNode.Group idField = scalarField("id", PrimitiveKind.INT64, Optional.empty());
        Optional<LogicalType> string = Optional.of(new LogicalType.StringType());
        SchemaNode.Group nameField = scalarField("name", PrimitiveKind.BYTE_ARRAY, string);
        SchemaNode.Group typedValue = new SchemaNode.Group(
                "typed_value", Repetition.OPTIONAL, List.of(idField, nameField), Optional.empty(), -1);
        return variantGroup(typedValue);
    }

    static SchemaNode.Group arrayOfInt32Group() {
        SchemaNode.Group element = valueAndTypedValue(
                "element", Repetition.REQUIRED, primitive("typed_value", PrimitiveKind.INT32, Optional.empty()));
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        SchemaNode.Group typedValue = new SchemaNode.Group(
                "typed_value", Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
        return variantGroup(typedValue);
    }

    static SchemaNode.Group objectWithPrimitiveFieldGroup() {
        SchemaNode.Primitive primitiveField = primitive("id", PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Group typedValue =
                new SchemaNode.Group("typed_value", Repetition.OPTIONAL, List.of(primitiveField), Optional.empty(), -1);
        return variantGroup(typedValue);
    }

    static SchemaNode.Group objectFieldWithNeitherValueNorTypedValueGroup() {
        SchemaNode.Primitive stray = primitive("stray", PrimitiveKind.BOOLEAN, Optional.empty());
        SchemaNode.Group emptyField =
                new SchemaNode.Group("id", Repetition.REQUIRED, List.of(stray), Optional.empty(), -1);
        SchemaNode.Group typedValue =
                new SchemaNode.Group("typed_value", Repetition.OPTIONAL, List.of(emptyField), Optional.empty(), -1);
        return variantGroup(typedValue);
    }

    private static SchemaNode.Group scalarField(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return valueAndTypedValue(name, Repetition.REQUIRED, primitive("typed_value", kind, logicalType));
    }

    private static SchemaNode.Group valueAndTypedValue(
            String name, Repetition repetition, SchemaNode.Primitive typedValue) {
        SchemaNode.Primitive value = primitive("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return new SchemaNode.Group(name, repetition, List.of(value, typedValue), Optional.empty(), -1);
    }

    private static SchemaNode.Group variantGroup(SchemaNode typedValue) {
        SchemaNode.Primitive metadata = new SchemaNode.Primitive(
                "metadata", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Primitive value = primitive("value", PrimitiveKind.BYTE_ARRAY, Optional.empty());
        return new SchemaNode.Group(
                "v",
                Repetition.OPTIONAL,
                List.of(metadata, value, typedValue),
                Optional.of(new LogicalType.Variant()),
                -1);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, Repetition.OPTIONAL, kind, OptionalInt.empty(), logicalType, -1);
    }

    private VariantSchemas() {}
}
