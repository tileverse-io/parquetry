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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;

class GeoMetadataBridgeTest {

    private static final ParquetSchema GEO_SCHEMA = new ParquetSchema(new Field.Group(
            "root",
            Repetition.REQUIRED,
            List.of(
                    new Field.Primitive(
                            "id", Repetition.REQUIRED, PrimitiveKind.INT64, OptionalInt.empty(), Optional.empty(), -1),
                    new Field.Primitive(
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
        ParquetSchema out = GeoMetadataBridge.apply(GEO_SCHEMA, Map.of());
        assertThat(out).isSameAs(GEO_SCHEMA);
    }

    @Test
    void malformedGeoJsonLogsAndReturnsSchemaUnchanged() {
        ParquetSchema out = GeoMetadataBridge.apply(GEO_SCHEMA, Map.of("geo", "{not json"));
        assertThat(out).isSameAs(GEO_SCHEMA);
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
        ParquetSchema out = GeoMetadataBridge.apply(GEO_SCHEMA, Map.of("geo", geo));
        Field.Primitive leaf =
                (Field.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType()).containsInstanceOf(LogicalType.Geometry.class);
        LogicalType.Geometry geom = (LogicalType.Geometry) leaf.logicalType().orElseThrow();
        assertThat(geom.crs()).isEmpty(); // absent crs surfaces as Optional.empty (spec default)
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
        ParquetSchema out = GeoMetadataBridge.apply(GEO_SCHEMA, Map.of("geo", geo));
        Field.Primitive leaf =
                (Field.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType()).containsInstanceOf(LogicalType.Geography.class);
        LogicalType.Geography geog = (LogicalType.Geography) leaf.logicalType().orElseThrow();
        assertThat(geog.crs()).isPresent();
        assertThat(geog.crs().orElseThrow()).contains("WGS 84");
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
        ParquetSchema out = GeoMetadataBridge.apply(GEO_SCHEMA, Map.of("geo", geo));
        Field.Primitive geometry =
                (Field.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(geometry.logicalType()).isEmpty();
    }

    @Test
    void nativeGeometryAnnotationWinsOverGeoMetadata() {
        // 2.0 file: schema arrives with the native Geometry logical type already set on the geometry column.
        LogicalType.Geometry native_ = new LogicalType.Geometry(Optional.of("{\"name\":\"EPSG:3857\"}"));
        ParquetSchema schemaWithNative = nativeGeometrySchema(native_);

        String geo = """
                {
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "crs": {"name":"WGS 84"}
                    }
                  }
                }
                """;
        ParquetSchema out = GeoMetadataBridge.apply(schemaWithNative, Map.of("geo", geo));
        Field.Primitive leaf =
                (Field.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        // Native CRS preserved verbatim; the divergent "geo" KV did not overwrite it.
        assertThat(leaf.logicalType()).contains(native_);
    }

    @Test
    void nativeGeographyAnnotationWinsOverGeoMetadata() {
        LogicalType.Geography native_ = new LogicalType.Geography(Optional.empty(), Optional.empty());
        ParquetSchema schemaWithNative = nativeGeographySchema(native_);

        // "geo" KV says Geometry (planar) - disagrees with native Geography. Native still wins.
        String geo = """
                {
                  "columns": {
                    "geometry": {"encoding": "WKB", "edges": "planar"}
                  }
                }
                """;
        ParquetSchema out = GeoMetadataBridge.apply(schemaWithNative, Map.of("geo", geo));
        Field.Primitive leaf =
                (Field.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType()).contains(native_);
    }

    @Test
    void nativeAnnotationAgreesWithGeoMetadataAndIsKeptUnchanged() {
        LogicalType.Geometry native_ = new LogicalType.Geometry(Optional.empty());
        ParquetSchema schemaWithNative = nativeGeometrySchema(native_);

        // "geo" KV would have produced the same Geometry(Optional.empty()) - no warning expected at runtime, native
        // path is taken silently. We only assert the resulting schema; warning behavior is observed via log level.
        String geo = """
                {
                  "columns": {
                    "geometry": {"encoding": "WKB"}
                  }
                }
                """;
        ParquetSchema out = GeoMetadataBridge.apply(schemaWithNative, Map.of("geo", geo));
        Field.Primitive leaf =
                (Field.Primitive) out.find(ColumnPath.of("geometry")).orElseThrow();
        assertThat(leaf.logicalType()).contains(native_);
    }

    // --- helpers ---

    private static ParquetSchema nativeGeometrySchema(LogicalType.Geometry annotation) {
        return schemaWithAnnotation(annotation);
    }

    private static ParquetSchema nativeGeographySchema(LogicalType.Geography annotation) {
        return schemaWithAnnotation(annotation);
    }

    private static ParquetSchema schemaWithAnnotation(LogicalType annotation) {
        return new ParquetSchema(new Field.Group(
                "root",
                Repetition.REQUIRED,
                List.of(
                        new Field.Primitive(
                                "id",
                                Repetition.REQUIRED,
                                PrimitiveKind.INT64,
                                OptionalInt.empty(),
                                Optional.empty(),
                                -1),
                        new Field.Primitive(
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
