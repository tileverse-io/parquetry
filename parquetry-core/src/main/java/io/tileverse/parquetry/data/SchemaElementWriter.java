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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.ConvertedType;
import io.tileverse.parquetry.format.FieldRepetitionType;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.PhysicalType;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Flattens a {@link ParquetSchema} tree into the depth-first {@link SchemaElement} list the Thrift footer carries.
 *
 * <p>Inverse of {@link io.tileverse.parquetry.schema.SchemaBuilder}: each group becomes one element with
 * {@code numChildren}, followed in pre-order by its children's elements; each primitive becomes one element with
 * {@code type} and, when applicable, {@code typeLength}. The root group surfaces as a node named {@code "schema"} when
 * the source schema's root is anonymous, matching the convention the parquet-format writers in the wild use.
 */
final class SchemaElementWriter {

    private SchemaElementWriter() {}

    static List<SchemaElement> flatten(ParquetSchema schema) {
        List<SchemaElement> out = new ArrayList<>();
        appendField(schema.root(), out, true);
        return out;
    }

    private static void appendField(SchemaNode field, List<SchemaElement> out, boolean isRoot) {
        switch (field) {
            case SchemaNode.Primitive primitive -> out.add(toPrimitiveElement(primitive));
            case SchemaNode.Group group -> {
                out.add(toGroupElement(group, isRoot));
                for (SchemaNode child : group.children()) {
                    appendField(child, out, false);
                }
            }
        }
    }

    private static SchemaElement toGroupElement(SchemaNode.Group group, boolean isRoot) {
        String name = isRoot && group.name().isEmpty() ? "schema" : group.name();
        Optional<FieldRepetitionType> repetition = isRoot ? Optional.empty() : Optional.of(mapRepetition(group));
        OptionalInt fieldId = group.fieldId() >= 0 ? OptionalInt.of(group.fieldId()) : OptionalInt.empty();
        Optional<LogicalType> logicalType = group.logicalType();
        return new SchemaElement(
                Optional.empty(),
                OptionalInt.empty(),
                repetition,
                name,
                OptionalInt.of(group.children().size()),
                legacyConvertedType(logicalType),
                OptionalInt.empty(),
                OptionalInt.empty(),
                logicalType,
                fieldId);
    }

    private static SchemaElement toPrimitiveElement(SchemaNode.Primitive primitive) {
        OptionalInt fieldId = primitive.fieldId() >= 0 ? OptionalInt.of(primitive.fieldId()) : OptionalInt.empty();
        Optional<LogicalType> logicalType = primitive.logicalType();
        return new SchemaElement(
                Optional.of(toPhysicalType(primitive.kind())),
                primitive.typeLength(),
                Optional.of(mapRepetition(primitive)),
                primitive.name(),
                OptionalInt.empty(),
                legacyConvertedType(logicalType),
                decimalScale(logicalType),
                decimalPrecision(logicalType),
                logicalType,
                fieldId);
    }

    /**
     * Backfills the deprecated {@link ConvertedType} from a node's modern {@link LogicalType} so readers that honor
     * only the legacy annotation (duckdb, older parquet-mr) still resolve strings, lists, maps, and the other
     * pre-logical-type kinds. Inverse of {@link io.tileverse.parquetry.schema.SchemaBuilder}'s read-side backfill.
     */
    private static Optional<ConvertedType> legacyConvertedType(Optional<LogicalType> logicalType) {
        return logicalType.flatMap(SchemaElementWriter::legacyEquivalent);
    }

    private static Optional<ConvertedType> legacyEquivalent(LogicalType logicalType) {
        return switch (logicalType) {
            case LogicalType.StringType _ -> Optional.of(ConvertedType.UTF8);
            case LogicalType.EnumType _ -> Optional.of(ConvertedType.ENUM);
            case LogicalType.JsonType _ -> Optional.of(ConvertedType.JSON);
            case LogicalType.BsonType _ -> Optional.of(ConvertedType.BSON);
            case LogicalType.ListType _ -> Optional.of(ConvertedType.LIST);
            case LogicalType.MapType _ -> Optional.of(ConvertedType.MAP);
            case LogicalType.DateType _ -> Optional.of(ConvertedType.DATE);
            case LogicalType.Decimal _ -> Optional.of(ConvertedType.DECIMAL);
            case LogicalType.IntType(byte bitWidth, boolean isSigned) -> intConvertedType(bitWidth, isSigned);
            case LogicalType.Time(boolean isAdjustedToUtc, LogicalType.TimeUnit unit) ->
                utcAdjustedConvertedType(isAdjustedToUtc, unit, ConvertedType.TIME_MILLIS, ConvertedType.TIME_MICROS);
            case LogicalType.Timestamp(boolean isAdjustedToUtc, LogicalType.TimeUnit unit) ->
                utcAdjustedConvertedType(
                        isAdjustedToUtc, unit, ConvertedType.TIMESTAMP_MILLIS, ConvertedType.TIMESTAMP_MICROS);
            // UUID, FLOAT16, GEOMETRY, GEOGRAPHY, VARIANT, UNKNOWN, and the local-time / NANOS time kinds postdate
            // the converted-type enum and have no legacy equivalent.
            default -> Optional.empty();
        };
    }

    private static Optional<ConvertedType> intConvertedType(byte bitWidth, boolean isSigned) {
        return switch (bitWidth) {
            case 8 -> Optional.of(isSigned ? ConvertedType.INT_8 : ConvertedType.UINT_8);
            case 16 -> Optional.of(isSigned ? ConvertedType.INT_16 : ConvertedType.UINT_16);
            case 32 -> Optional.of(isSigned ? ConvertedType.INT_32 : ConvertedType.UINT_32);
            case 64 -> Optional.of(isSigned ? ConvertedType.INT_64 : ConvertedType.UINT_64);
            default -> Optional.empty();
        };
    }

    /**
     * The legacy TIME/TIMESTAMP converted types only ever meant the UTC-adjusted milli- and microsecond encodings;
     * local-time and nanosecond columns have no equivalent and keep the modern annotation alone.
     */
    private static Optional<ConvertedType> utcAdjustedConvertedType(
            boolean isAdjustedToUtc, LogicalType.TimeUnit unit, ConvertedType millis, ConvertedType micros) {
        if (!isAdjustedToUtc) {
            return Optional.empty();
        }
        return switch (unit) {
            case MILLIS -> Optional.of(millis);
            case MICROS -> Optional.of(micros);
            case NANOS -> Optional.empty();
        };
    }

    private static OptionalInt decimalScale(Optional<LogicalType> logicalType) {
        if (logicalType.orElse(null) instanceof LogicalType.Decimal decimal) {
            return OptionalInt.of(decimal.scale());
        }
        return OptionalInt.empty();
    }

    private static OptionalInt decimalPrecision(Optional<LogicalType> logicalType) {
        if (logicalType.orElse(null) instanceof LogicalType.Decimal decimal) {
            return OptionalInt.of(decimal.precision());
        }
        return OptionalInt.empty();
    }

    private static FieldRepetitionType mapRepetition(SchemaNode field) {
        return switch (field.repetition()) {
            case REQUIRED -> FieldRepetitionType.REQUIRED;
            case OPTIONAL -> FieldRepetitionType.OPTIONAL;
            case REPEATED -> FieldRepetitionType.REPEATED;
        };
    }

    private static PhysicalType toPhysicalType(PrimitiveKind kind) {
        return switch (kind) {
            case BOOLEAN -> PhysicalType.BOOLEAN;
            case INT32 -> PhysicalType.INT32;
            case INT64 -> PhysicalType.INT64;
            case INT96 -> PhysicalType.INT96;
            case FLOAT -> PhysicalType.FLOAT;
            case DOUBLE -> PhysicalType.DOUBLE;
            case BYTE_ARRAY -> PhysicalType.BYTE_ARRAY;
            case FIXED_LEN_BYTE_ARRAY -> PhysicalType.FIXED_LEN_BYTE_ARRAY;
        };
    }
}
