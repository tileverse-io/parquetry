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
package io.tileverse.parquetry.arrow;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;

import tools.jackson.databind.json.JsonMapper;

/**
 * Resolves which projected leaf columns are geometry and produces their GeoArrow field metadata (the
 * {@code geoarrow.wkb} extension name plus a small {@code crs}/{@code edges} JSON document). Geometry is detected from
 * the GeoParquet 2.0 logical type or, for GeoParquet 1.x, from the {@code geo} key-value metadata column map.
 */
final class GeoArrowFields {

    private static final String EXTENSION_NAME_KEY = "ARROW:extension:name";
    private static final String EXTENSION_METADATA_KEY = "ARROW:extension:metadata";
    private static final String GEOARROW_WKB = "geoarrow.wkb";
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private final Map<ColumnPath, Map<String, String>> byColumn;

    private GeoArrowFields(Map<ColumnPath, Map<String, String>> byColumn) {
        this.byColumn = byColumn;
    }

    boolean isGeometry(ColumnPath path) {
        return byColumn.containsKey(path);
    }

    Map<String, String> metadataFor(ColumnPath path) {
        return byColumn.getOrDefault(path, Map.of());
    }

    static GeoArrowFields resolve(ParquetSchema schema, Optional<GeoParquetMetadata> geo) {
        Map<ColumnPath, Map<String, String>> result = new HashMap<>();
        for (ColumnPath path : schema.leafColumns()) {
            Optional<GeometryInfo> info = geometryInfo(schema, path, geo);
            if (info.isPresent()) {
                result.put(path, fieldMetadata(info.get()));
            }
        }
        return new GeoArrowFields(result);
    }

    private static Optional<GeometryInfo> geometryInfo(
            ParquetSchema schema, ColumnPath path, Optional<GeoParquetMetadata> geo) {
        Optional<SchemaNode> node = schema.find(path);
        if (node.isPresent() && node.get() instanceof SchemaNode.Primitive primitive) {
            Optional<LogicalType> logical = primitive.logicalType();
            if (logical.isPresent()
                    && logical.get() instanceof LogicalType.Geometry(Optional<CoordinateReferenceSystem> geomCrs)) {
                return Optional.of(new GeometryInfo(geomCrs, "planar"));
            }
            if (logical.isPresent()
                    && logical.get()
                            instanceof
                            LogicalType.Geography(
                                    Optional<CoordinateReferenceSystem> geogCrs,
                                    Optional<EdgeInterpolationAlgorithm> _)) {
                return Optional.of(new GeometryInfo(geogCrs, "spherical"));
            }
        }
        if (geo.isPresent()) {
            GeoColumn column = geo.get().columns().get(path.dot());
            if (column != null) {
                return Optional.of(new GeometryInfo(column.crs(), column.edges().orElse("planar")));
            }
        }
        return Optional.empty();
    }

    private static Map<String, String> fieldMetadata(GeometryInfo info) {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(EXTENSION_NAME_KEY, GEOARROW_WKB);
        metadata.put(EXTENSION_METADATA_KEY, extensionMetadataJson(info));
        return Map.copyOf(metadata);
    }

    private static String extensionMetadataJson(GeometryInfo info) {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("edges", info.edges());
        info.crs().ifPresent(crs -> document.put("crs", crs));
        return MAPPER.writeValueAsString(document);
    }

    private record GeometryInfo(Optional<CoordinateReferenceSystem> crs, String edges) {}
}
