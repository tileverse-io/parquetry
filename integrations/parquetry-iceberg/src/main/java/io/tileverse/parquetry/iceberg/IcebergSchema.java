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
package io.tileverse.parquetry.iceberg;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

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
        Optional<LogicalType> logicalType = geoLogicalType(field).or(mapping::logicalType);
        return new SchemaNode.Primitive(
                field.name(), repetition, mapping.kind(), mapping.typeLength(), logicalType, field.fieldId());
    }

    private static Optional<LogicalType> geoLogicalType(IcebergField field) {
        return switch (field.type()) {
            case "geometry" -> Optional.of(new LogicalType.Geometry(field.crs()));
            case "geography" -> Optional.of(new LogicalType.Geography(field.crs(), field.geographyAlgorithm()));
            default -> Optional.empty();
        };
    }

    private static PrimitiveMapping mappingFor(String icebergType) {
        return switch (icebergType) {
            case "int" -> new PrimitiveMapping(PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty());
            case "long" -> new PrimitiveMapping(PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty());
            case "float" -> new PrimitiveMapping(PrimitiveKind.FLOAT, OptionalInt.empty(), Optional.empty());
            case "double" -> new PrimitiveMapping(PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty());
            case "boolean" -> new PrimitiveMapping(PrimitiveKind.BOOLEAN, OptionalInt.empty(), Optional.empty());
            case "date" ->
                new PrimitiveMapping(PrimitiveKind.INT32, OptionalInt.empty(), Optional.of(new LogicalType.DateType()));
            case "string" ->
                new PrimitiveMapping(
                        PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.of(new LogicalType.StringType()));
            case "binary" -> new PrimitiveMapping(PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty());
            case "geometry", "geography" ->
                new PrimitiveMapping(PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty());
            case "uuid" ->
                new PrimitiveMapping(
                        PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                        OptionalInt.of(16),
                        Optional.of(new LogicalType.UuidType()));
            case "timestamp" ->
                new PrimitiveMapping(
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.of(new LogicalType.Timestamp(false, LogicalType.TimeUnit.MICROS)));
            case "timestamptz" ->
                new PrimitiveMapping(
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS)));
            case "timestamp_ns" ->
                new PrimitiveMapping(
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.of(new LogicalType.Timestamp(false, LogicalType.TimeUnit.NANOS)));
            case "timestamptz_ns" ->
                new PrimitiveMapping(
                        PrimitiveKind.INT64,
                        OptionalInt.empty(),
                        Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.NANOS)));
            case "unknown" ->
                new PrimitiveMapping(
                        PrimitiveKind.INT32, OptionalInt.empty(), Optional.of(new LogicalType.UnknownType()));
            default -> throw new IcebergFormatException("unsupported Iceberg field type: " + icebergType);
        };
    }

    private record PrimitiveMapping(PrimitiveKind kind, OptionalInt typeLength, Optional<LogicalType> logicalType) {}
}
