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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.data.WriteRow;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;

/**
 * Exercises the GeoParquet metadata that lands in the footer after a real end-to-end write through
 * {@link ParquetWriter}. Complements {@link GeoMetadataWriterTest} (unit-level rendering) by validating the JSON-as-KV
 * placement and the v2 logical-type annotations the writer sets on the schema elements as observed by the read side.
 */
class GeoMetadataWriteTest {

    @TempDir
    Path tempDir;

    @Test
    void dualModeEmitsGeoKvAndLogicalTypeOnSchemaElement() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        WriteOptions options = options()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("dual.parquet");
        byte[] wkb = wkbPoint(1.0, 2.0);
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.write(
                    WriteRow.of(Map.of(ColumnPath.of("geometry"), MemorySegment.ofArray(wkb), ColumnPath.of("id"), 1)));
        }

        FileMetaData footer = readFooter(file);
        String geoJson = geoJsonOrThrow(footer);
        assertThat(geoJson)
                .contains("\"version\":\"1.1.0\"", "\"primary_column\":\"geometry\"", "\"encoding\":\"WKB\"");

        GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJson);
        assertThat(parsed).isInstanceOf(GeoParquetMetadata.V1_1.class);
        GeoColumn geometryColumn = parsed.columns().get("geometry");
        assertThat(geometryColumn.crs()).isPresent();
        assertThat(geometryColumn.crs().orElseThrow().id().orElseThrow().epsgCode())
                .hasValue(4326);

        SchemaElement geometryElement = footerElementByName(footer, "geometry");
        LogicalType geometryLogical = geometryElement.logicalType().orElseThrow();
        assertThat(geometryLogical).isInstanceOf(LogicalType.Geometry.class);
    }

    @Test
    void v1OnlyModeEmitsGeoKvAndOmitsLogicalType() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        WriteOptions options = options()
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("v1.parquet");
        writePointFile(file, schema, options, 0.0, 0.0);

        FileMetaData footer = readFooter(file);
        assertThat(geoJsonOrThrow(footer)).contains("\"version\":\"1.1.0\"");

        SchemaElement geometryElement = footerElementByName(footer, "geometry");
        assertThat(geometryElement.logicalType())
                .as("V1_1_ONLY must not annotate the schema element with a v2 logical type")
                .isEmpty();
    }

    @Test
    void v2OnlyModeOmitsGeoKvAndEmitsLogicalType() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        WriteOptions options = options()
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("v2.parquet");
        writePointFile(file, schema, options, 0.0, 0.0);

        FileMetaData footer = readFooter(file);
        Optional<String> geo = lookupKv(footer, "geo");
        assertThat(geo).as("V2_0_ONLY must not write the v1.1 geo KV blob").isEmpty();

        SchemaElement geometryElement = footerElementByName(footer, "geometry");
        assertThat(geometryElement.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);
    }

    @Test
    void multipleGeometryColumnsCarryLogicalTypesAndAppearInJson() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredBinary("centroid"));
        WriteOptions options = options()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg("geometry", 4326)
                .crsEpsg("centroid", 3857)
                .build();
        Path file = tempDir.resolve("multi-geo.parquet");
        byte[] geometryWkb = wkbPoint(10.0, 20.0);
        byte[] centroidWkb = wkbPoint(15.0, 25.0);
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            Map<ColumnPath, Object> values = new HashMap<>();
            values.put(ColumnPath.of("geometry"), MemorySegment.ofArray(geometryWkb));
            values.put(ColumnPath.of("centroid"), MemorySegment.ofArray(centroidWkb));
            writer.write(values::get);
        }

        FileMetaData footer = readFooter(file);
        SchemaElement geometryElement = footerElementByName(footer, "geometry");
        SchemaElement centroidElement = footerElementByName(footer, "centroid");
        assertThat(geometryElement.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);
        assertThat(centroidElement.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);

        GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJsonOrThrow(footer));
        assertThat(parsed.columns().keySet()).containsExactlyInAnyOrder("geometry", "centroid");
        assertThat(parsed.primaryColumn())
                .as("primary_column follows schema order")
                .isEqualTo("geometry");
    }

    @Test
    void bboxInJsonUnionsEveryRowGroupBbox() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        WriteOptions options = options()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(2))
                .build();
        Path file = tempDir.resolve("bbox-union.parquet");
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.write(geometryRow(0.0, 0.0));
            writer.write(geometryRow(1.0, 1.0));
            writer.write(geometryRow(-5.0, -3.0));
            writer.write(geometryRow(4.0, 6.0));
        }

        FileMetaData footer = readFooter(file);
        assertThat(footer.rowGroups()).hasSizeGreaterThanOrEqualTo(2);

        GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJsonOrThrow(footer));
        GeoColumn geometryColumn = parsed.columns().get("geometry");
        assertThat(geometryColumn.bbox()).isPresent();
        assertThat(geometryColumn.bbox().orElseThrow().xmin()).isEqualTo(-5.0);
        assertThat(geometryColumn.bbox().orElseThrow().xmax()).isEqualTo(4.0);
        assertThat(geometryColumn.bbox().orElseThrow().ymin()).isEqualTo(-3.0);
        assertThat(geometryColumn.bbox().orElseThrow().ymax()).isEqualTo(6.0);
    }

    @Test
    void crsRoundTripsThroughTheReader() throws Exception {
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        WriteOptions options = options()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("crs-roundtrip.parquet");
        writePointFile(file, schema, options, 0.0, 0.0);

        FileMetaData footer = readFooter(file);
        GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJsonOrThrow(footer));
        CoordinateReferenceSystem crs = parsed.columns().get("geometry").crs().orElseThrow();
        CoordinateReferenceSystem expected =
                CoordinateReferenceSystems.forEpsg(4326).orElseThrow();
        assertThat(crs.id().orElseThrow().epsgCode()).hasValue(4326);
        assertThat(crs.getClass()).isEqualTo(expected.getClass());
    }

    // --- helpers ---

    private WriteOptions.Builder options() {
        return WriteOptions.builder().tempDir(tempDir);
    }

    private static void writePointFile(Path file, ParquetSchema schema, WriteOptions options, double x, double y)
            throws Exception {
        byte[] wkb = wkbPoint(x, y);
        try (ParquetWriter writer = ParquetWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.write(WriteRow.of(Map.of(ColumnPath.of("geometry"), MemorySegment.ofArray(wkb))));
        }
    }

    private static WriteRow geometryRow(double x, double y) {
        byte[] wkb = wkbPoint(x, y);
        return WriteRow.of(Map.of(ColumnPath.of("geometry"), MemorySegment.ofArray(wkb)));
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    private static String geoJsonOrThrow(FileMetaData footer) {
        return lookupKv(footer, "geo").orElseThrow(() -> new AssertionError("Missing 'geo' key in footer KV"));
    }

    private static Optional<String> lookupKv(FileMetaData footer, String key) {
        for (KeyValue entry : footer.keyValueMetadata()) {
            if (key.equals(entry.key())) {
                return entry.value();
            }
        }
        return Optional.empty();
    }

    private static SchemaElement footerElementByName(FileMetaData footer, String name) {
        for (SchemaElement element : footer.schema()) {
            if (name.equals(element.name())) {
                return element;
            }
        }
        throw new AssertionError("Missing schema element: " + name);
    }

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = Stream.of(leaves).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer bb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(1);
        bb.putDouble(x);
        bb.putDouble(y);
        return bb.array();
    }
}
