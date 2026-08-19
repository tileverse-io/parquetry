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

    /**
     * A GeoParquet schema with an {@code id} INT32 column and an OPTIONAL {@code geometry} BYTE_ARRAY column, used by
     * the cases that need a row with no geometry at all.
     */
    public static ParquetSchema extentCasesSchema() {
        SchemaNode.Primitive id = primitive("id", Repetition.REQUIRED, PrimitiveKind.INT32, Optional.empty());
        SchemaNode.Primitive geometry =
                primitive("geometry", Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, Optional.empty());
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

    /**
     * The vertical line's longitude, and the one coordinate in the fixture that float32 cannot hold exactly. A FLOAT
     * bbox covering rounds it outward, covering a strip of longitude the geometry does not reach, which is what tells
     * an answer read off the covering columns apart from one that tests the geometry.
     */
    public static final double EXTENT_LINE_X = 2.3816;

    /**
     * Writes the rows that tell a bounding-box relation apart from an exact one, against the query rectangle {@code (0,
     * 0, 10, 10)}:
     *
     * <ul>
     *   <li>1: a point well inside the query box
     *   <li>2: a point exactly on the query box edge, where Within and CoveredBy disagree
     *   <li>3: a triangle whose bounding box overlaps the query box while the shape itself stays clear of it
     *   <li>4: a vertical line at {@link #EXTENT_LINE_X}, whose bounding box has zero width and no exact FLOAT covering
     *   <li>5: a point well outside the query box
     *   <li>6: no geometry at all
     * </ul>
     */
    public static void writeExtentCases(Path file) throws Exception {
        ParquetSchema schema = extentCasesSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(file.toAbsolutePath().getParent())
                .crsEpsg("geometry", 4326)
                .build();
        try (OutputStream out = Files.newOutputStream(file);
                ParquetFileWriter writer = ParquetFileWriter.create(out, schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender();
            extentCase(appender, 1, wkbPoint(5.0, 5.0));
            extentCase(appender, 2, wkbPoint(0.0, 5.0));
            extentCase(appender, 3, wkbTriangle(9.0, 20.0, 20.0, 9.0, 20.0, 20.0));
            extentCase(appender, 4, wkbLine(EXTENT_LINE_X, 2.0, EXTENT_LINE_X, 8.0));
            extentCase(appender, 5, wkbPoint(50.0, 50.0));
            extentCase(appender, 6, null);
            appender.flush();
        }
    }

    private static void extentCase(ParquetRecordBatchBuilder appender, int id, byte[] wkb) {
        appender.setInt(ID, id);
        if (wkb == null) {
            appender.setNull(GEOMETRY);
        } else {
            appender.setBinary(GEOMETRY, MemorySegment.ofArray(wkb));
        }
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

    private static byte[] wkbLine(double x1, double y1, double x2, double y2) {
        ByteBuffer bb = ByteBuffer.allocate(9 + 2 * 16).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(2);
        bb.putInt(2);
        bb.putDouble(x1);
        bb.putDouble(y1);
        bb.putDouble(x2);
        bb.putDouble(y2);
        return bb.array();
    }

    private static byte[] wkbTriangle(double x1, double y1, double x2, double y2, double x3, double y3) {
        ByteBuffer bb = ByteBuffer.allocate(13 + 4 * 16).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(3);
        bb.putInt(1);
        bb.putInt(4);
        bb.putDouble(x1);
        bb.putDouble(y1);
        bb.putDouble(x2);
        bb.putDouble(y2);
        bb.putDouble(x3);
        bb.putDouble(y3);
        bb.putDouble(x1);
        bb.putDouble(y1);
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
