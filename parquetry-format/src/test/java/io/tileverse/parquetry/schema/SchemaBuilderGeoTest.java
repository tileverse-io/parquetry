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
package io.tileverse.parquetry.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;

/**
 * Covers {@link SchemaBuilder#synthesizeGeoLogicalTypes} - the GeoParquet 1.x "geo" key-value metadata fold-in path.
 * Cases mirror the contract: planar/default edges become Geometry, spherical edges become Geography, missing or
 * malformed metadata leaves the schema untouched, and a native annotation on the column always wins over the JSON.
 */
class SchemaBuilderGeoTest {

    private static final ParquetSchema GEO_SCHEMA = new ParquetSchema(new SchemaNode.Group(
            "root",
            Repetition.REQUIRED,
            List.of(
                    new SchemaNode.Primitive(
                            "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1),
                    new SchemaNode.Primitive(
                            "geometry",
                            Repetition.OPTIONAL,
                            PrimitiveKind.BYTE_ARRAY,
                            OptionalInt.empty(),
                            Optional.empty(),
                            -1)),
            Optional.empty(),
            -1));

    @Test
    void missingGeoMetadataLeavesSchemaUnchanged() {
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(GEO_SCHEMA, Map.of());
        assertThat(out)
                .as("no 'geo' KV entry should leave the schema reference equal")
                .isSameAs(GEO_SCHEMA);
    }

    @Test
    void malformedGeoJsonLogsAndReturnsSchemaUnchanged() {
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(GEO_SCHEMA, Map.of("geo", "{not json"));
        assertThat(out)
                .as("malformed 'geo' JSON must degrade gracefully rather than throw")
                .isSameAs(GEO_SCHEMA);
    }

    @Test
    void planarGeoParquet1xColumnBecomesGeometry() {
        String geo = """
                {
                  "version": "1.1.0",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Polygon"]
                    }
                  }
                }
                """;
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(GEO_SCHEMA, Map.of("geo", geo));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType())
                .as("default / planar 'geo' column should synthesize Geometry")
                .containsInstanceOf(LogicalType.Geometry.class);
        LogicalType.Geometry geom = (LogicalType.Geometry) leaf.logicalType().orElseThrow();
        assertThat(geom.crs())
                .as("absent 'crs' surfaces as Optional.empty (consumer falls back to spec default OGC:CRS84)")
                .isEmpty();
    }

    @Test
    void sphericalEdgesYieldGeography() {
        String geo = """
                {
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "edges": "spherical",
                      "crs": {"type":"GeographicCRS","name":"WGS 84"}
                    }
                  }
                }
                """;
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(GEO_SCHEMA, Map.of("geo", geo));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType())
                .as("'edges: spherical' should synthesize Geography")
                .containsInstanceOf(LogicalType.Geography.class);
        LogicalType.Geography geog = (LogicalType.Geography) leaf.logicalType().orElseThrow();
        assertThat(geog.crs())
                .as("inline PROJJSON should be parsed eagerly into the typed CRS ADT")
                .isPresent();
        CoordinateReferenceSystem crs = geog.crs().orElseThrow();
        assertThat(crs)
                .as("WGS 84 PROJJSON should land as a GeographicCRS record")
                .isInstanceOf(GeographicCRS.class);
        assertThat(crs.name())
                .as("CRS name should round-trip through the typed parse")
                .contains("WGS 84");
    }

    @Test
    void unknownColumnInGeoMetadataIsSilentlySkipped() {
        String geo = """
                {
                  "columns": {
                    "not_a_column": {"encoding": "WKB"}
                  }
                }
                """;
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(GEO_SCHEMA, Map.of("geo", geo));
        SchemaNode.Primitive geometry =
                (SchemaNode.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(geometry.logicalType())
                .as("an unknown column in 'geo' should not annotate any actual leaf")
                .isEmpty();
    }

    @Test
    void nativeGeometryAnnotationWinsOverGeoMetadata() {
        LogicalType.Geometry nativeType = new LogicalType.Geometry(Optional.<CoordinateReferenceSystem>of(
                new GeographicCRS(Optional.of("EPSG:3857"), Optional.empty(), Optional.empty(), Optional.empty())));
        ParquetSchema schemaWithNative = schemaWithAnnotation(nativeType);

        String geo = """
                {
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "crs": {"type":"GeographicCRS","name":"WGS 84"}
                    }
                  }
                }
                """;
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(schemaWithNative, Map.of("geo", geo));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType())
                .as("native Geometry annotation must be preserved even when 'geo' KV would synthesize a different CRS")
                .contains(nativeType);
    }

    @Test
    void nativeGeographyAnnotationWinsOverGeoMetadata() {
        LogicalType.Geography nativeType = new LogicalType.Geography(Optional.empty(), Optional.empty());
        ParquetSchema schemaWithNative = schemaWithAnnotation(nativeType);

        // "geo" KV would synthesize Geometry (planar) - disagrees with native Geography. Native still wins.
        String geo = """
                {
                  "columns": {
                    "geometry": {"encoding": "WKB", "edges": "planar"}
                  }
                }
                """;
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(schemaWithNative, Map.of("geo", geo));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType())
                .as("native Geography wins even when 'geo' KV would have synthesized Geometry")
                .contains(nativeType);
    }

    @Test
    void nativeAnnotationAgreesWithGeoMetadataAndIsKeptUnchanged() {
        LogicalType.Geometry nativeType = new LogicalType.Geometry(Optional.empty());
        ParquetSchema schemaWithNative = schemaWithAnnotation(nativeType);

        // "geo" KV would also produce Geometry(Optional.empty()): silent agreement path.
        String geo = """
                {
                  "columns": {
                    "geometry": {"encoding": "WKB"}
                  }
                }
                """;
        ParquetSchema out = SchemaBuilder.synthesizeGeoLogicalTypes(schemaWithNative, Map.of("geo", geo));
        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType())
                .as("agreeing native + 'geo' KV should leave the native annotation unchanged")
                .contains(nativeType);
    }

    private static ParquetSchema schemaWithAnnotation(LogicalType annotation) {
        return new ParquetSchema(new SchemaNode.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        new SchemaNode.Primitive(
                                "id",
                                Repetition.REQUIRED,
                                PrimitiveKind.INT64,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new SchemaNode.Primitive(
                                "geometry",
                                Repetition.OPTIONAL,
                                PrimitiveKind.BYTE_ARRAY,
                                OptionalInt.empty(),
                                Optional.of(annotation),
                                -1)),
                Optional.empty(),
                -1));
    }
}
