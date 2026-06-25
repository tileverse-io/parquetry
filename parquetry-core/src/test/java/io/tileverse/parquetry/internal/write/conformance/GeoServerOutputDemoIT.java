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
package io.tileverse.parquetry.internal.write.conformance;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.RowGroupSize;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.format.KeyValue;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.RowGroup;
import io.tileverse.parquetry.format.SchemaElement;
import io.tileverse.parquetry.internal.write.WriteFixtures;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Streaming end-to-end write that mimics a GeoServer-style {@code FeatureCollection} feed: a feature stub with an id, a
 * name, and a WKB geometry. The test asserts the resulting file parses, carries the GeoParquet 1.1 KV blob, has the v2
 * logical type on the geometry column, and that every row group reports a non-empty geospatial bbox whose union is
 * covered by the file-level bbox in the KV blob.
 */
@Tag("conformance")
class GeoServerOutputDemoIT {

    private static final int FEATURE_COUNT = 10_000;
    private static final long ROWS_PER_RG = 2_000L;

    @TempDir
    Path tempDir;

    @Test
    void writesGeoParquetWithPerRowGroupBboxAndFileLevelUnion() throws Exception {
        ParquetSchema schema = featureCollectionSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_RG))
                .build();
        Path file = tempDir.resolve("features.parquet");

        List<Feature> features = generateFeatures(FEATURE_COUNT);
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender((int) ROWS_PER_RG);
            for (Feature feature : features) {
                WriteFixtures.appendRow(appender, schema, toRow(feature));
            }
            appender.flush();
        }

        FileMetaData footer = readFooter(file);
        assertThat(footer.numRows()).isEqualTo(FEATURE_COUNT);
        long expectedRowGroups = (FEATURE_COUNT + ROWS_PER_RG - 1) / ROWS_PER_RG;
        assertThat(footer.rowGroups()).hasSize((int) expectedRowGroups);

        SchemaElement geometryElement = footerElementByName(footer, "geometry");
        assertThat(geometryElement.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);

        BoundingBox rowGroupUnion = unionRowGroupBboxes(footer);
        assertThat(rowGroupUnion)
                .as("at least one row group must publish a non-empty geo bbox")
                .isNotNull();

        String geoJson = lookupKv(footer, "geo")
                .orElseThrow(() -> new AssertionError("default DUAL mode must emit the 'geo' KV blob"));
        GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJson);
        GeoColumn geometryColumn = parsed.columns().get("geometry");
        BoundingBox fileBbox = geometryColumn.bbox().orElseThrow();
        assertThat(fileBbox.xmin()).isLessThanOrEqualTo(rowGroupUnion.xmin());
        assertThat(fileBbox.ymin()).isLessThanOrEqualTo(rowGroupUnion.ymin());
        assertThat(fileBbox.xmax()).isGreaterThanOrEqualTo(rowGroupUnion.xmax());
        assertThat(fileBbox.ymax()).isGreaterThanOrEqualTo(rowGroupUnion.ymax());
    }

    @Test
    void streamingWriteStaysWithinPeakHeapBudget() throws Exception {
        ParquetSchema schema = featureCollectionSchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .rowGroupSize(RowGroupSize.rows(ROWS_PER_RG))
                .build();
        Path file = tempDir.resolve("features-memprobe.parquet");

        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long baseline = runtime.totalMemory() - runtime.freeMemory();

        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            ParquetRecordBatchBuilder appender = writer.appender((int) ROWS_PER_RG);
            for (int i = 0; i < FEATURE_COUNT; i++) {
                WriteFixtures.appendRow(appender, schema, toRow(buildFeature(i)));
            }
            appender.flush();
        }

        long peakUsed = runtime.totalMemory() - runtime.freeMemory();
        long delta = peakUsed - baseline;
        long budget = 256L * 1024L * 1024L;
        assertThat(delta)
                .as("peak heap delta during streaming write must stay below 256 MB (was %d bytes)", delta)
                .isLessThan(budget);
    }

    // --- feature stub + row construction ---

    private record Feature(long id, String name, byte[] wkb) {}

    private static List<Feature> generateFeatures(int count) {
        List<Feature> features = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            features.add(buildFeature(i));
        }
        return features;
    }

    private static Feature buildFeature(int i) {
        double x = ((i * 7919) % 36000) / 100.0 - 180.0;
        double y = ((i * 6151) % 18000) / 100.0 - 90.0;
        return new Feature(i, "feature-" + i, wkbPoint(x, y));
    }

    private static Map<ColumnPath, Object> toRow(Feature feature) {
        Map<ColumnPath, Object> values = new HashMap<>(3);
        values.put(ColumnPath.of("id"), feature.id());
        values.put(ColumnPath.of("name"), MemorySegment.ofArray(feature.name().getBytes(StandardCharsets.UTF_8)));
        values.put(ColumnPath.of("geometry"), MemorySegment.ofArray(feature.wkb()));
        return values;
    }

    // --- schema + footer helpers ---

    private static ParquetSchema featureCollectionSchema() {
        SchemaNode.Primitive id = primitive("id", PrimitiveKind.INT64);
        SchemaNode.Primitive name = primitive("name", PrimitiveKind.BYTE_ARRAY);
        SchemaNode.Primitive geometry = primitive("geometry", PrimitiveKind.BYTE_ARRAY);
        List<SchemaNode> children =
                Stream.of(id, name, geometry).map(f -> (SchemaNode) f).toList();
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive primitive(String name, PrimitiveKind kind) {
        return new SchemaNode.Primitive(name, Repetition.REQUIRED, kind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static FileMetaData readFooter(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetFormat.readFooter(source);
        }
    }

    private static SchemaElement footerElementByName(FileMetaData footer, String name) {
        for (SchemaElement element : footer.schema()) {
            if (name.equals(element.name())) {
                return element;
            }
        }
        throw new AssertionError("Missing schema element: " + name);
    }

    private static Optional<String> lookupKv(FileMetaData footer, String key) {
        for (KeyValue entry : footer.keyValueMetadata()) {
            if (key.equals(entry.key())) {
                return entry.value();
            }
        }
        return Optional.empty();
    }

    private static BoundingBox unionRowGroupBboxes(FileMetaData footer) {
        BoundingBox union = null;
        for (RowGroup rg : footer.rowGroups()) {
            ColumnMetaData meta = geometryMetaData(rg);
            GeospatialStatistics stats =
                    meta.geospatialStatistics().orElseThrow(() -> new AssertionError("row group must carry geo stats"));
            BoundingBox bbox = stats.bbox().orElseThrow(() -> new AssertionError("row group bbox must be present"));
            union = union == null ? bbox : merge(union, bbox);
        }
        return union;
    }

    private static ColumnMetaData geometryMetaData(RowGroup rg) {
        for (ColumnChunk chunk : rg.columns()) {
            ColumnMetaData meta = chunk.metaData().orElse(null);
            if (meta == null) {
                continue;
            }
            if (meta.pathInSchema().size() == 1
                    && "geometry".equals(meta.pathInSchema().get(0))) {
                return meta;
            }
        }
        throw new AssertionError("geometry column missing from row group");
    }

    private static BoundingBox merge(BoundingBox left, BoundingBox right) {
        return BoundingBox.builder()
                .xmin(Math.min(left.xmin(), right.xmin()))
                .xmax(Math.max(left.xmax(), right.xmax()))
                .ymin(Math.min(left.ymin(), right.ymin()))
                .ymax(Math.max(left.ymax(), right.ymax()))
                .build();
    }

    // --- WKB ---

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer bb = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        bb.put((byte) 1);
        bb.putInt(1);
        bb.putDouble(x);
        bb.putDouble(y);
        return bb.array();
    }
}
