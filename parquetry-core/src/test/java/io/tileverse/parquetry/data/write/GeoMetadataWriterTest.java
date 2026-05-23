/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.data.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystems;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;

class GeoMetadataWriterTest {

    @Test
    void dualModeEmitsBothLogicalTypesAndJson() {
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg("geometry", 4326)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredInt32("id"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        ParquetSchema enriched = writer.applyV2LogicalTypes(schema);
        SchemaNode.Primitive geomLeaf =
                (SchemaNode.Primitive) enriched.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(geomLeaf.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);

        Optional<String> json = writer.v1JsonPayload(enriched, summaries("geometry", polygonSummary()));
        assertThat(json).isPresent();
        GeoParquetMetadata parsed = GeoParquetMetadata.parse(json.orElseThrow());
        assertThat(parsed).isInstanceOf(GeoParquetMetadata.V1_1.class);
        assertThat(parsed.primaryColumn()).isEqualTo("geometry");
        GeoColumn col = parsed.columns().get("geometry");
        assertThat(col.encoding()).contains("WKB");
        assertThat(col.geometryTypes()).containsExactly("Polygon");
        assertThat(col.crs().orElseThrow()).isInstanceOf(GeographicCRS.class);
    }

    @Test
    void v1OnlyModeOmitsLogicalTypesButEmitsJson() {
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crsEpsg("geometry", 4326)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        ParquetSchema enriched = writer.applyV2LogicalTypes(schema);
        SchemaNode.Primitive geomLeaf =
                (SchemaNode.Primitive) enriched.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(geomLeaf.logicalType())
                .as("V1_1_ONLY must not annotate the schema with v2 logical types")
                .isEmpty();

        Optional<String> json = writer.v1JsonPayload(enriched, summaries("geometry", polygonSummary()));
        assertThat(json).isPresent();
        assertThat(json.orElseThrow()).contains("\"version\":\"1.1.0\"");
    }

    @Test
    void v2OnlyModeEmitsLogicalTypesAndSkipsJson() {
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.V2_0_ONLY)
                .crsEpsg("geometry", 4326)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        ParquetSchema enriched = writer.applyV2LogicalTypes(schema);
        SchemaNode.Primitive geomLeaf =
                (SchemaNode.Primitive) enriched.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(geomLeaf.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);

        Optional<String> json = writer.v1JsonPayload(enriched, summaries("geometry", polygonSummary()));
        assertThat(json).as("V2_0_ONLY must skip the v1.1 KV blob").isEmpty();
    }

    @Test
    void multipleGeometryColumnsAreAllAnnotatedAndListed() {
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg("geometry", 4326)
                .crsEpsg("centroid", 3857)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("geometry"), requiredBinary("centroid"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        ParquetSchema enriched = writer.applyV2LogicalTypes(schema);
        for (String name : List.of("geometry", "centroid")) {
            SchemaNode.Primitive leaf =
                    (SchemaNode.Primitive) enriched.find(ColumnPath.of(name)).orElseThrow();
            assertThat(leaf.logicalType().orElseThrow()).isInstanceOf(LogicalType.Geometry.class);
        }

        Optional<String> json = writer.v1JsonPayload(
                enriched,
                Map.of(ColumnPath.of("geometry"), polygonSummary(), ColumnPath.of("centroid"), pointSummary()));
        GeoParquetMetadata parsed = GeoParquetMetadata.parse(json.orElseThrow());
        assertThat(parsed.columns().keySet()).containsExactlyInAnyOrder("geometry", "centroid");
        assertThat(parsed.primaryColumn())
                .as("primary_column follows schema order")
                .isEqualTo("geometry");
    }

    @Test
    void bboxAggregatesAcrossRowGroups() {
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crsEpsg("geometry", 4326)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        BoundingBox union = bbox(-10.0, -5.0, 0.0, 5.0).build();
        GeoColumnSummary summary = GeoColumnSummary.wkb(Optional.of(union), List.of(3));
        Optional<String> json = writer.v1JsonPayload(schema, Map.of(ColumnPath.of("geometry"), summary));
        GeoParquetMetadata parsed = GeoParquetMetadata.parse(json.orElseThrow());
        BoundingBox parsedBbox = parsed.columns().get("geometry").bbox().orElseThrow();
        assertThat(parsedBbox.xmin()).isEqualTo(-10.0);
        assertThat(parsedBbox.xmax()).isEqualTo(0.0);
        assertThat(parsedBbox.ymin()).isEqualTo(-5.0);
        assertThat(parsedBbox.ymax()).isEqualTo(5.0);
    }

    @Test
    void emptyCrsConfigYieldsEmptyJsonAndSchemaUnchanged() {
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("data"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        assertThat(writer.applyV2LogicalTypes(schema)).isEqualTo(schema);
        assertThat(writer.v1JsonPayload(schema, Map.of())).isEmpty();
    }

    @Test
    void rawProjjsonCrsRoundTripsThroughTheReader() {
        CoordinateReferenceSystem wgs84 =
                CoordinateReferenceSystems.forEpsg(4326).orElseThrow();
        WriteOptions options = WriteOptions.builder()
                .geoParquetMetadata(GeoParquetMetadataMode.V1_1_ONLY)
                .crs("geometry", wgs84)
                .build();
        ParquetSchema schema = flatSchema(requiredBinary("geometry"));
        GeoMetadataWriter writer = new GeoMetadataWriter(options);

        String json = writer.v1JsonPayload(schema, summaries("geometry", polygonSummary()))
                .orElseThrow();
        GeoParquetMetadata parsed = GeoParquetMetadata.parse(json);
        CoordinateReferenceSystem reparsed =
                parsed.columns().get("geometry").crs().orElseThrow();
        assertThat(reparsed).isInstanceOf(GeographicCRS.class);
        assertThat(reparsed.id().orElseThrow().epsgCode()).hasValue(4326);
    }

    // --- helpers ---

    private static ParquetSchema flatSchema(SchemaNode.Primitive... leaves) {
        List<SchemaNode> children = List.of(leaves);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, children, Optional.empty(), /* fieldId */ -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive requiredInt32(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static Map<ColumnPath, GeoColumnSummary> summaries(String name, GeoColumnSummary summary) {
        return Map.of(ColumnPath.of(name), summary);
    }

    private static GeoColumnSummary polygonSummary() {
        BoundingBox box = bbox(-1.0, -1.0, 1.0, 1.0).build();
        return GeoColumnSummary.wkb(Optional.of(box), List.of(3));
    }

    private static GeoColumnSummary pointSummary() {
        BoundingBox box = bbox(0.0, 0.0, 0.5, 0.5).build();
        return GeoColumnSummary.wkb(Optional.of(box), List.of(1));
    }

    private static BoundingBox.BoundingBoxBuilder bbox(double xmin, double ymin, double xmax, double ymax) {
        return BoundingBox.builder()
                .xmin(xmin)
                .xmax(xmax)
                .ymin(ymin)
                .ymax(ymax)
                .zmin(OptionalDouble.empty())
                .zmax(OptionalDouble.empty())
                .mmin(OptionalDouble.empty())
                .mmax(OptionalDouble.empty());
    }
}
