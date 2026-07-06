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
package io.tileverse.parquetry.cli.support;

import java.io.OutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/** Writes tiny, deterministic Parquet fixtures using parquetry's own writer. */
public final class Fixtures {

    public static final ColumnPath ID = ColumnPath.of("id");
    public static final ColumnPath NAME = ColumnPath.of("name");
    public static final ColumnPath POP = ColumnPath.of("pop");
    public static final ColumnPath CAPITAL = ColumnPath.of("capital");
    public static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    private Fixtures() {}

    public static ParquetSchema citiesSchema() {
        SchemaNode.Primitive id = primitive("id", Repetition.REQUIRED, PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive name = primitive(
                "name", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, Optional.of(new LogicalType.StringType()));
        SchemaNode.Primitive pop = primitive("pop", Repetition.OPTIONAL, PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Primitive capital =
                primitive("capital", Repetition.OPTIONAL, PrimitiveKind.BOOLEAN, Optional.empty());
        SchemaNode.Group root = new SchemaNode.Group(
                "schema", Repetition.REQUIRED, List.of(id, name, pop, capital), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * Schema-only fixture exposing one OPTIONAL leaf of each numeric primitive kind ({@code i32}, {@code i64},
     * {@code f32}, {@code f64}), used to exercise the per-kind literal coercion branches of the SQL filter translator.
     */
    public static ParquetSchema numericKindsSchema() {
        SchemaNode.Primitive i32 = primitive("i32", Repetition.OPTIONAL, PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive i64 = primitive("i64", Repetition.OPTIONAL, PrimitiveKind.INT64, Optional.empty());
        SchemaNode.Primitive f32 = primitive("f32", Repetition.OPTIONAL, PrimitiveKind.FLOAT, Optional.empty());
        SchemaNode.Primitive f64 = primitive("f64", Repetition.OPTIONAL, PrimitiveKind.DOUBLE, Optional.empty());
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(i32, i64, f32, f64), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    /**
     * Schema-only fixture exposing a single {@code id} column declared as {@code FIXED_LEN_BYTE_ARRAY(16)} annotated
     * with the {@link LogicalType.UuidType}, used to exercise the UUID literal coercion branches of the SQL filter
     * translator.
     */
    public static ParquetSchema uuidColumnSchema() {
        SchemaNode.Primitive id = new SchemaNode.Primitive(
                "id",
                Repetition.REQUIRED,
                PrimitiveKind.FIXED_LEN_BYTE_ARRAY,
                OptionalInt.of(16),
                Optional.of(new LogicalType.UuidType()),
                -1);
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    public static void writeCities(Path file) throws Exception {
        writeCities(file, Map.of());
    }

    /** Writes the cities fixture with caller-supplied file-level key-value metadata in the footer. */
    public static void writeCities(Path file, Map<String, String> keyValueMetadata) throws Exception {
        ParquetSchema schema = citiesSchema();
        WriteOptions.Builder builder =
                WriteOptions.builder().tempDir(file.toAbsolutePath().getParent());
        if (!keyValueMetadata.isEmpty()) {
            builder.keyValueMetadata(keyValueMetadata);
        }
        WriteOptions options = builder.build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            city(appender, 1, "Rosario", 1_300_000L, false);
            city(appender, 2, "Cordoba", 1_400_000L, false);
            city(appender, 3, "Buenos Aires", 3_100_000L, true);
            city(appender, 4, null, 500L, false);
            appender.flush();
        }
    }

    /**
     * Writes a tiny GeoParquet file with an {@code id} INT32 column and a {@code geometry} BYTE_ARRAY column carrying
     * WKB points. The writer emits GeoParquet {@code "geo"} metadata in the file footer, which exercises the
     * {@code GeoParquetMetadata.parse} path via Jackson record deserialization when reading the file back.
     */
    public static ParquetSchema geoCitiesSchema() {
        SchemaNode.Primitive id = primitive("id", Repetition.REQUIRED, PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive geometry =
                primitive("geometry", Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, Optional.empty());
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(id, geometry), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    public static MemorySegment wkbPointSegment(double x, double y) {
        return MemorySegment.ofArray(wkbPoint(x, y));
    }

    public static void writeGeoCities(Path file) throws Exception {
        ParquetSchema schema = geoCitiesSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(file.toAbsolutePath().getParent())
                .crsEpsg("geometry", 4326)
                .build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            geoCity(appender, 1, -60.65, -32.94);
            geoCity(appender, 2, -64.18, -31.42);
            appender.flush();
        }
    }

    private static void geoCity(ParquetRecordBatchBuilder appender, int id, double lon, double lat) {
        appender.setInt(ID, id);
        appender.setBinary(GEOMETRY, MemorySegment.ofArray(wkbPoint(lon, lat)));
        appender.endRow();
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer bb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(1);
        bb.putDouble(x);
        bb.putDouble(y);
        return bb.array();
    }

    private static void city(ParquetRecordBatchBuilder appender, int id, String name, long pop, boolean capital) {
        appender.setInt(ID, id);
        if (name == null) {
            appender.setNull(NAME);
        } else {
            appender.setBinary(NAME, MemorySegment.ofArray(name.getBytes(StandardCharsets.UTF_8)));
        }
        appender.setLong(POP, pop);
        appender.setBoolean(CAPITAL, capital);
        appender.endRow();
    }

    private static SchemaNode.Primitive primitive(
            String name, Repetition repetition, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        return new SchemaNode.Primitive(name, repetition, kind, OptionalInt.empty(), logicalType, -1);
    }
}
