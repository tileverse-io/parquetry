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
package io.tileverse.parquetry.arrow.ipc;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.ParquetCrs;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

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
            if (logical.isPresent() && logical.get() instanceof LogicalType.Geometry(Optional<ParquetCrs> geomCrs)) {
                return Optional.of(new GeometryInfo(geomCrs.map(GeoArrowFields::crsJsonValue), "planar"));
            }
            if (logical.isPresent()
                    && logical.get()
                            instanceof
                            LogicalType.Geography(
                                    Optional<ParquetCrs> geogCrs,
                                    Optional<EdgeInterpolationAlgorithm> _)) {
                return Optional.of(new GeometryInfo(geogCrs.map(GeoArrowFields::crsJsonValue), "spherical"));
            }
        }
        if (geo.isPresent()) {
            GeoColumn column = geo.get().columns().get(path.dot());
            if (column != null) {
                Optional<Object> crs = column.crs().map(Object.class::cast);
                return Optional.of(new GeometryInfo(crs, column.edges().orElse("planar")));
            }
        }
        return Optional.empty();
    }

    /**
     * Renders a native {@link ParquetCrs} as the GeoArrow {@code crs} metadata value: the inline PROJJSON document for
     * {@link ParquetCrs.Inline}, otherwise the {@code authority:code} / {@code srid:} / {@code projjson:} reference
     * string.
     */
    private static Object crsJsonValue(ParquetCrs crs) {
        return switch (crs) {
            case ParquetCrs.Inline(var projjson) -> projjson;
            case ParquetCrs.AuthorityCode(String authority, String code) -> authority + ":" + code;
            case ParquetCrs.Srid(long id) -> ParquetCrs.SRID_PREFIX + id;
            case ParquetCrs.ProjjsonKey(String key) -> ParquetCrs.PROJJSON_PREFIX + key;
        };
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

    private record GeometryInfo(Optional<Object> crs, String edges) {}
}
