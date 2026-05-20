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
package io.tileverse.parquetry.schema.geo.geoparquet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.projjson.GeographicCRS;

import tools.jackson.databind.json.JsonMapper;

/**
 * Coverage for the typed GeoParquet metadata ADT. Exercises each version's record permit, the typed PROJJSON CRS in the
 * {@code crs} field, the 1.1+ {@code covering} structure, forward-default of unknown versions to
 * {@link GeoParquetMetadata.V2}, and the no-SPI-auto-registration contract for {@link GeoParquetModule}.
 */
class GeoParquetMetadataTest {

    @Test
    void parsesV1_0() {
        String json = """
                {
                  "version": "1.0.0",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Polygon", "MultiPolygon"],
                      "bbox": [-180.0, -90.0, 180.0, 90.0]
                    }
                  }
                }
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta)
                .as("1.0.0 version string dispatches to the V1_0 record permit")
                .isInstanceOf(GeoParquetMetadata.V1_0.class);
        assertThat(meta.primaryColumn()).isEqualTo("geometry");
        GeoColumn col = meta.columns().get("geometry");
        assertThat(col.geometryTypes()).containsExactly("Polygon", "MultiPolygon");
        assertThat(col.encoding()).contains("WKB");
        BoundingBox bbox = col.bbox().orElseThrow();
        assertThat(bbox.xmin())
                .as("bbox JSON [xmin, ymin, xmax, ymax] should permute into BoundingBox flat-double order")
                .isEqualTo(-180.0);
        assertThat(bbox.ymin()).isEqualTo(-90.0);
        assertThat(bbox.xmax()).isEqualTo(180.0);
        assertThat(bbox.ymax()).isEqualTo(90.0);
        assertThat(col.covering()).as("GeoParquet 1.0 has no covering").isEmpty();
    }

    @Test
    void parsesV1_1WithTypedCrsAndCovering() {
        String json = """
                {
                  "version": "1.1.0",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Polygon"],
                      "crs": {
                        "type": "GeographicCRS",
                        "name": "WGS 84",
                        "id": {"authority": "EPSG", "code": 4326}
                      },
                      "edges": "spherical",
                      "orientation": "counterclockwise",
                      "epoch": 2020.5,
                      "covering": {
                        "bbox": {
                          "xmin": ["bbox", "xmin"],
                          "xmax": ["bbox", "xmax"],
                          "ymin": ["bbox", "ymin"],
                          "ymax": ["bbox", "ymax"]
                        }
                      }
                    }
                  }
                }
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta).as("1.1.0 dispatches to V1_1").isInstanceOf(GeoParquetMetadata.V1_1.class);
        GeoColumn col = meta.columns().get("geometry");
        assertThat(col.crs())
                .as("inline PROJJSON CRS parses through ProjJsonModule into typed GeographicCRS")
                .hasValueSatisfying(crs -> {
                    assertThat(crs).isInstanceOf(GeographicCRS.class);
                    assertThat(crs.id().orElseThrow().epsgCode()).hasValue(4326);
                });
        assertThat(col.edges()).contains("spherical");
        assertThat(col.orientation()).contains("counterclockwise");
        assertThat(col.epoch()).hasValue(2020.5);
        BboxCovering bbox = col.covering().orElseThrow().bbox();
        assertThat(bbox.xmin()).isEqualTo(ColumnPath.of("bbox", "xmin"));
        assertThat(bbox.ymax()).isEqualTo(ColumnPath.of("bbox", "ymax"));
    }

    @Test
    void parsesV1_1ErrataVariant() {
        String json = """
                {"version": "1.1.0+p1", "primary_column": "geom", "columns": {"geom": {"geometry_types": []}}}
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta).as("1.1.0+p1 (errata) shares the V1_1 wire shape").isInstanceOf(GeoParquetMetadata.V1_1.class);
    }

    @Test
    void parsesV2() {
        String json = """
                {
                  "version": "2.0.0-dev",
                  "primary_column": "geometry",
                  "columns": {
                    "geometry": {
                      "encoding": "WKB",
                      "geometry_types": ["Point"]
                    }
                  }
                }
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta).as("2.0.0-dev dispatches to V2").isInstanceOf(GeoParquetMetadata.V2.class);
    }

    @Test
    void parsesV2Final() {
        String json = """
                {"version": "2.0.0", "primary_column": "geom", "columns": {"geom": {"geometry_types": []}}}
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta).as("2.0.0 final dispatches to the same V2 record").isInstanceOf(GeoParquetMetadata.V2.class);
    }

    @Test
    void unknownVersionFallsBackToV2() {
        String json = """
                {"version": "3.7.0-future", "primary_column": "g", "columns": {"g": {"geometry_types": []}}}
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta)
                .as("unknown future versions degrade to V2 (newest known shape)")
                .isInstanceOf(GeoParquetMetadata.V2.class);
        assertThat(meta.version()).isEqualTo("3.7.0-future");
    }

    @Test
    void missingVersionFallsBackToV2() {
        String json = """
                {"primary_column": "g", "columns": {"g": {"geometry_types": []}}}
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        assertThat(meta).isInstanceOf(GeoParquetMetadata.V2.class);
    }

    @Test
    void malformedJsonThrows() {
        assertThatThrownBy(() -> GeoParquetMetadata.parse("not json"))
                .as("malformed input must not silently produce an empty metadata record")
                .isInstanceOf(Exception.class);
    }

    @Test
    void coveringWithZAxesParses() {
        String json = """
                {
                  "version": "1.1.0", "primary_column": "g",
                  "columns": {"g": {"geometry_types": [], "covering": {"bbox": {
                    "xmin": ["a","x"], "xmax": ["a","X"],
                    "ymin": ["a","y"], "ymax": ["a","Y"],
                    "zmin": ["a","z"], "zmax": ["a","Z"]
                  }}}}
                }
                """;
        GeoParquetMetadata meta = GeoParquetMetadata.parse(json);
        BboxCovering bbox = meta.columns().get("g").covering().orElseThrow().bbox();
        assertThat(bbox.zmin()).contains(ColumnPath.of("a", "z"));
        assertThat(bbox.zmax()).contains(ColumnPath.of("a", "Z"));
    }

    @Test
    void moduleIsNotAutoRegistered() {
        JsonMapper plain = JsonMapper.builder().build();
        String json = """
                {"version": "1.0.0", "primary_column": "g", "columns": {}}
                """;
        assertThatThrownBy(() -> plain.readValue(json, GeoParquetMetadata.class))
                .as("a fresh mapper without GeoParquetModule must NOT pick up the deserializer via SPI")
                .isInstanceOf(Exception.class);
    }
}
