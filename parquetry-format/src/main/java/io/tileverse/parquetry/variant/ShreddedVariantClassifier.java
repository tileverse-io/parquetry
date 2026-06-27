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
package io.tileverse.parquetry.variant;

import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_BINARY;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_BOOLEAN_TRUE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DATE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DECIMAL16;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DECIMAL4;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DECIMAL8;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_DOUBLE;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_FLOAT;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT16;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT32;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT64;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_INT8;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_STRING;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIME;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_NTZ_MICROS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_NTZ_NANOS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_TZ_MICROS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_TIMESTAMP_TZ_NANOS;
import static io.tileverse.parquetry.variant.VariantScalarTypeIds.TYPE_UUID;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.LogicalType.Decimal;
import io.tileverse.parquetry.format.LogicalType.IntType;
import io.tileverse.parquetry.format.LogicalType.TimeUnit;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.variant.ShreddedVariant.Field;
import io.tileverse.parquetry.variant.ShreddedVariant.Scalar;
import io.tileverse.parquetry.variant.ShreddedVariant.ShreddedArray;
import io.tileverse.parquetry.variant.ShreddedVariant.ShreddedObject;

/** Classifies a Parquet Variant column's typed_value subtree into a {@link ShreddedVariant} from the schema alone. */
final class ShreddedVariantClassifier {

    private static final String TYPED_VALUE = "typed_value";
    private static final String VALUE = "value";

    static ShreddedVariant classify(SchemaNode.Group variantGroup) {
        SchemaNode typedValue = childNamed(variantGroup, TYPED_VALUE)
                .orElseThrow(() ->
                        new ParquetFormatException("Variant group has no typed_value child: " + variantGroup.name()));
        return classifyTypedValue(typedValue);
    }

    private static ShreddedVariant classifyTypedValue(SchemaNode node) {
        return switch (node) {
            case SchemaNode.Primitive primitive -> new Scalar(primitive, variantTypeIdFor(primitive));
            case SchemaNode.Group group -> classifyTypedValueGroup(group);
        };
    }

    private static ShreddedVariant classifyTypedValueGroup(SchemaNode.Group group) {
        if (isList(group)) {
            return new ShreddedArray(classifyField(elementGroup(group)));
        }
        return classifyObject(group);
    }

    private static ShreddedVariant classifyObject(SchemaNode.Group group) {
        Map<String, Field> fields = new LinkedHashMap<>();
        for (SchemaNode child : group.children()) {
            fields.put(child.name(), classifyField(fieldGroup(child)));
        }
        return new ShreddedObject(fields);
    }

    private static SchemaNode.Group fieldGroup(SchemaNode child) {
        SchemaNode.Group group = asFieldGroup(child);
        requireRequiredFieldGroup(group);
        return group;
    }

    private static SchemaNode.Group asFieldGroup(SchemaNode child) {
        return switch (child) {
            case SchemaNode.Group group -> group;
            case SchemaNode.Primitive primitive ->
                throw new ParquetFormatException(
                        "Shredded Variant object field must be a value/typed_value group, found primitive: "
                                + primitive.name());
        };
    }

    /**
     * The spec requires every shredded object field to be a required group. An optional field group cannot distinguish
     * a field that is absent at a row from one whose shredded value is null, which the reconstruction algorithm relies
     * on.
     */
    private static void requireRequiredFieldGroup(SchemaNode.Group group) {
        if (group.repetition() != Repetition.REQUIRED) {
            throw new ParquetFormatException("Shredded Variant object field must be a required group: " + group.name());
        }
    }

    private static Field classifyField(SchemaNode.Group node) {
        SchemaNode.Primitive value = fieldValue(node);
        ShreddedVariant typedValue = childNamed(node, TYPED_VALUE)
                .map(ShreddedVariantClassifier::classifyTypedValue)
                .orElse(null);
        if (value == null && typedValue == null) {
            throw new ParquetFormatException(
                    "Shredded Variant field has neither a value nor a typed_value child: " + node.name());
        }
        return new Field(value, typedValue);
    }

    private static SchemaNode.Primitive fieldValue(SchemaNode.Group node) {
        SchemaNode value = childNamed(node, VALUE).orElse(null);
        return switch (value) {
            case null -> null;
            case SchemaNode.Primitive primitive -> primitive;
            case SchemaNode.Group group ->
                throw new ParquetFormatException(
                        "Shredded Variant value child must be a primitive leaf, found group: " + group.name());
        };
    }

    private static boolean isList(SchemaNode.Group group) {
        return group.logicalType()
                .map(logicalType -> logicalType instanceof LogicalType.ListType)
                .orElse(false);
    }

    /**
     * The element group of a list. The standard three-level encoding wraps the element in a repeated group
     * ({@code repeated group list { <element> }}); the element is that group's single child. The legacy two-level
     * encoding puts the repeated group directly under the list group, in which case that group is the element itself.
     */
    private static SchemaNode.Group elementGroup(SchemaNode.Group listGroup) {
        SchemaNode repeated = listGroup.children().get(0);
        return switch (repeated) {
            case SchemaNode.Group wrapper ->
                (SchemaNode.Group) wrapper.children().get(0);
            case SchemaNode.Primitive element ->
                throw new ParquetFormatException(
                        "Shredded Variant array element must be a value/typed_value group, found primitive: "
                                + element.name());
        };
    }

    private static Optional<SchemaNode> childNamed(SchemaNode.Group group, String name) {
        return group.children().stream()
                .filter(child -> child.name().equals(name))
                .findFirst();
    }

    private static int variantTypeIdFor(SchemaNode.Primitive primitive) {
        Optional<LogicalType> logicalType = primitive.logicalType();
        return switch (primitive.kind()) {
            case BOOLEAN -> TYPE_BOOLEAN_TRUE;
            case INT32 -> int32TypeId(primitive, logicalType);
            case INT64 -> int64TypeId(primitive, logicalType);
            case FLOAT -> TYPE_FLOAT;
            case DOUBLE -> TYPE_DOUBLE;
            case BYTE_ARRAY -> byteArrayTypeId(primitive, logicalType);
            case FIXED_LEN_BYTE_ARRAY -> fixedLenTypeId(primitive, logicalType);
            case INT96 -> throw unsupported(primitive);
        };
    }

    private static int int32TypeId(SchemaNode.Primitive primitive, Optional<LogicalType> logicalType) {
        if (logicalType.isEmpty()) {
            return TYPE_INT32;
        }
        return switch (logicalType.get()) {
            case IntType(byte bitWidth, boolean isSigned) when isSigned -> signedInt32TypeId(primitive, bitWidth);
            case Decimal ignored -> TYPE_DECIMAL4;
            case LogicalType.DateType ignored -> TYPE_DATE;
            default -> throw unsupported(primitive);
        };
    }

    private static int signedInt32TypeId(SchemaNode.Primitive primitive, byte bitWidth) {
        return switch (bitWidth) {
            case 8 -> TYPE_INT8;
            case 16 -> TYPE_INT16;
            case 32 -> TYPE_INT32;
            default -> throw unsupported(primitive);
        };
    }

    private static int int64TypeId(SchemaNode.Primitive primitive, Optional<LogicalType> logicalType) {
        if (logicalType.isEmpty()) {
            return TYPE_INT64;
        }
        return switch (logicalType.get()) {
            case IntType(byte bitWidth, boolean isSigned) when isSigned && bitWidth == 64 -> TYPE_INT64;
            case Decimal ignored -> TYPE_DECIMAL8;
            case LogicalType.Time time -> timeTypeId(primitive, time);
            case LogicalType.Timestamp timestamp -> timestampTypeId(primitive, timestamp);
            default -> throw unsupported(primitive);
        };
    }

    private static int timeTypeId(SchemaNode.Primitive primitive, LogicalType.Time time) {
        if (time.unit() == TimeUnit.MICROS) {
            return TYPE_TIME;
        }
        throw unsupported(primitive);
    }

    private static int timestampTypeId(SchemaNode.Primitive primitive, LogicalType.Timestamp timestamp) {
        boolean utc = timestamp.isAdjustedToUTC();
        return switch (timestamp.unit()) {
            case MICROS -> utc ? TYPE_TIMESTAMP_TZ_MICROS : TYPE_TIMESTAMP_NTZ_MICROS;
            case NANOS -> utc ? TYPE_TIMESTAMP_TZ_NANOS : TYPE_TIMESTAMP_NTZ_NANOS;
            case MILLIS -> throw unsupported(primitive);
        };
    }

    private static int byteArrayTypeId(SchemaNode.Primitive primitive, Optional<LogicalType> logicalType) {
        if (logicalType.isEmpty()) {
            return TYPE_BINARY;
        }
        return switch (logicalType.get()) {
            case LogicalType.StringType ignored -> TYPE_STRING;
            case LogicalType.Decimal ignored -> TYPE_DECIMAL16;
            default -> throw unsupported(primitive);
        };
    }

    private static int fixedLenTypeId(SchemaNode.Primitive primitive, Optional<LogicalType> logicalType) {
        if (logicalType.isEmpty()) {
            throw unsupported(primitive);
        }
        return switch (logicalType.get()) {
            case LogicalType.UuidType ignored -> TYPE_UUID;
            case Decimal ignored -> TYPE_DECIMAL16;
            default -> throw unsupported(primitive);
        };
    }

    private static ParquetFormatException unsupported(SchemaNode.Primitive primitive) {
        return new ParquetFormatException("Unsupported shredded value type: " + describe(primitive));
    }

    private static String describe(SchemaNode.Primitive primitive) {
        StringBuilder description = new StringBuilder(primitive.kind().name());
        appendTypeLength(description, primitive);
        appendLogicalType(description, primitive.logicalType());
        return description.toString();
    }

    private static void appendTypeLength(StringBuilder description, SchemaNode.Primitive primitive) {
        if (primitive.kind() == PrimitiveKind.FIXED_LEN_BYTE_ARRAY
                && primitive.typeLength().isPresent()) {
            description.append('(').append(primitive.typeLength().getAsInt()).append(')');
        }
    }

    private static void appendLogicalType(StringBuilder description, Optional<LogicalType> logicalType) {
        logicalType.ifPresent(present -> description.append(' ').append(describeLogicalType(present)));
    }

    private static String describeLogicalType(LogicalType logicalType) {
        return switch (logicalType) {
            case IntType(byte bitWidth, boolean isSigned) -> "INT(" + bitWidth + "," + isSigned + ")";
            case LogicalType.Time time -> "TIME(" + time.unit() + ")";
            case LogicalType.Timestamp timestamp -> "TIMESTAMP(" + timestamp.unit() + ")";
            default -> logicalType.getClass().getSimpleName();
        };
    }

    private ShreddedVariantClassifier() {}
}
