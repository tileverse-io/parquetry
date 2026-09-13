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
package io.tileverse.parquetry.iceberg;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.UuidConverter;
import io.tileverse.parquetry.schema.geo.ParquetCrs;

/**
 * The Iceberg table's current schema, presented as a Parquet schema for reads.
 *
 * <p>Iceberg keeps the authoritative column set, types, ids, and nullability in its table metadata, evolving them
 * independently of any single data file's footer. This holds that current schema and projects it into a
 * {@link ParquetSchema} of one top-level leaf per field, ordered as Iceberg declares them and keyed by Iceberg field
 * id. Reads present this schema instead of the first data file's footer, the prerequisite for matching each data file's
 * columns back to the table by field id.
 *
 * <p>Only top-level primitive fields are modeled; nested struct/list/map fields are out of scope for this cut and are
 * already dropped upstream by {@link IcebergTableMetadata}.
 */
final class IcebergSchema {

    private static final String ROOT_GROUP_NAME = "table";
    private static final int ROOT_FIELD_ID = -1;

    private final List<IcebergField> fields;
    private final Map<Integer, IcebergField> fieldsById;
    private final ParquetSchema parquetSchema;

    private IcebergSchema(List<IcebergField> fields) {
        this.fields = List.copyOf(fields);
        this.fieldsById = indexById(this.fields);
        this.parquetSchema = presentAsParquetSchema(this.fields);
    }

    /** Builds the schema from an Iceberg table's current primitive fields. */
    public static IcebergSchema of(List<IcebergField> fields) {
        Objects.requireNonNull(fields, "fields");
        return new IcebergSchema(fields);
    }

    /** The Iceberg fields in declaration order. */
    public List<IcebergField> fields() {
        return fields;
    }

    /** The current schema presented as a Parquet schema, one top-level leaf per field, keyed by field id. */
    public ParquetSchema parquetSchema() {
        return parquetSchema;
    }

    /** The field with the given Iceberg field id, or {@link Optional#empty()} when none matches. */
    public Optional<IcebergField> fieldById(int fieldId) {
        return Optional.ofNullable(fieldsById.get(fieldId));
    }

    /** The Parquet primitive kind a column of {@code field} is presented as, from this schema's single type mapping. */
    static PrimitiveKind kindOf(IcebergField field) {
        return mappingFor(field.type()).kind();
    }

    private static Map<Integer, IcebergField> indexById(List<IcebergField> fields) {
        Map<Integer, IcebergField> byId = new LinkedHashMap<>();
        for (IcebergField field : fields) {
            byId.put(field.fieldId(), field);
        }
        return byId;
    }

    private static ParquetSchema presentAsParquetSchema(List<IcebergField> fields) {
        List<SchemaNode> leaves = new ArrayList<>(fields.size());
        for (IcebergField field : fields) {
            leaves.add(leafOf(field));
        }
        SchemaNode.Group root =
                new SchemaNode.Group(ROOT_GROUP_NAME, Repetition.REQUIRED, leaves, Optional.empty(), ROOT_FIELD_ID);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive leafOf(IcebergField field) {
        PrimitiveMapping mapping = mappingFor(field.type());
        Repetition repetition = field.required() ? Repetition.REQUIRED : Repetition.OPTIONAL;
        return new SchemaNode.Primitive(
                field.name(), repetition, mapping.kind(), mapping.typeLength(), mapping.logicalType(), field.fieldId());
    }

    private static PrimitiveMapping mappingFor(IcebergType type) {
        return switch (type) {
            case IcebergType.BoolType _ -> plain(PrimitiveKind.BOOLEAN);
            case IcebergType.IntType _ -> plain(PrimitiveKind.INT32);
            case IcebergType.LongType _ -> plain(PrimitiveKind.INT64);
            case IcebergType.FloatType _ -> plain(PrimitiveKind.FLOAT);
            case IcebergType.DoubleType _ -> plain(PrimitiveKind.DOUBLE);
            case IcebergType.DateType _ -> annotated(PrimitiveKind.INT32, new LogicalType.DateType());
            case IcebergType.StringType _ -> annotated(PrimitiveKind.BYTE_ARRAY, new LogicalType.StringType());
            case IcebergType.BinaryType _ -> plain(PrimitiveKind.BYTE_ARRAY);
            case IcebergType.UuidType _ ->
                new PrimitiveMapping(
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                        OptionalInt.of(UuidConverter.BYTES),
                        Optional.of(new LogicalType.UuidType()));
            case IcebergType.TimestampType(boolean adjustToUtc, LogicalType.TimeUnit unit) ->
                annotated(PrimitiveKind.INT64, new LogicalType.Timestamp(adjustToUtc, unit));
            case IcebergType.TimeType _ ->
                annotated(PrimitiveKind.INT64, new LogicalType.Time(false, LogicalType.TimeUnit.MICROS));
            case IcebergType.DecimalType(int precision, int scale) -> decimalMapping(precision, scale);
            case IcebergType.FixedType(int length) ->
                new PrimitiveMapping(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, OptionalInt.of(length), Optional.empty());
            case IcebergType.GeometryType(Optional<ParquetCrs> crs) ->
                annotated(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geometry(crs));
            case IcebergType.GeographyType(Optional<ParquetCrs> crs, Optional<EdgeInterpolationAlgorithm> algorithm) ->
                annotated(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geography(crs, algorithm));
            case IcebergType.UnknownType _ -> annotated(PrimitiveKind.INT32, new LogicalType.UnknownType());
        };
    }

    private static PrimitiveMapping plain(PrimitiveKind kind) {
        return new PrimitiveMapping(kind, OptionalInt.empty(), Optional.empty());
    }

    private static PrimitiveMapping annotated(PrimitiveKind kind, LogicalType logicalType) {
        return new PrimitiveMapping(kind, OptionalInt.empty(), Optional.of(logicalType));
    }

    /** Iceberg's writer stores a decimal in the narrowest Parquet type that holds its precision. */
    private static PrimitiveMapping decimalMapping(int precision, int scale) {
        LogicalType.Decimal logicalType = new LogicalType.Decimal(scale, precision);
        if (precision <= 9) {
            return annotated(PrimitiveKind.INT32, logicalType);
        }
        if (precision <= 18) {
            return annotated(PrimitiveKind.INT64, logicalType);
        }
        return new PrimitiveMapping(
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(decimalByteLength(precision)),
                Optional.of(logicalType));
    }

    /**
     * The smallest byte count whose signed two's-complement range holds every unscaled value of {@code precision}
     * digits: Iceberg's canonical FIXED_LEN_BYTE_ARRAY length for a decimal column.
     */
    static int decimalByteLength(int precision) {
        BigInteger largest = BigInteger.TEN.pow(precision).subtract(BigInteger.ONE);
        int length = 1;
        while (BigInteger.ONE.shiftLeft(8 * length - 1).subtract(BigInteger.ONE).compareTo(largest) < 0) {
            length++;
        }
        return length;
    }

    private record PrimitiveMapping(PrimitiveKind kind, OptionalInt typeLength, Optional<LogicalType> logicalType) {}
}
