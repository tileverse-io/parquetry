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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tileverse.parquetry.format.EdgeInterpolationAlgorithm;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;
import io.tileverse.parquetry.schema.ParquetSchema;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Bridges GeoParquet 1.x {@code "geo"} key-value metadata to the GeoParquet 2.0 native logical types
 * ({@link LogicalType.Geometry} / {@link LogicalType.Geography}).
 *
 * <p>At {@code ParquetDataset.open()} the schema is built from the Thrift {@code SchemaElement}s, which for GeoParquet
 * 1.x carry no logical-type annotation on the geometry column - it's just a {@code BYTE_ARRAY}. The bridge parses the
 * file-level {@code "geo"} JSON (when present) and rewrites the matching schema leaves so downstream code sees one
 * uniform shape regardless of file version.
 *
 * <h2>Precedence</h2>
 *
 * <p>Native (Parquet 2.12+ {@code GEOMETRY} / {@code GEOGRAPHY} logical types) always wins over the {@code "geo"} KV
 * metadata. The bridge only synthesizes an annotation when the leaf has no native one already. When both are present
 * and they disagree (different geometry vs geography kind, different CRS, different algorithm) the bridge logs a
 * warning so an operator can spot a stale {@code "geo"} entry. Same shape on both sides is silent.
 *
 * <h2>Tolerance</h2>
 *
 * <p>Missing or malformed metadata logs at WARN and returns the schema unchanged. Parquet files without geometry
 * columns must not be rejected.
 */
final class GeoMetadataBridge {

    private static final Logger LOG = LoggerFactory.getLogger(GeoMetadataBridge.class);
    private static final String GEO_KEY = "geo";
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private GeoMetadataBridge() {}

    /**
     * If {@code keyValueMetadata} contains a GeoParquet {@code "geo"} entry, returns {@code schema} with each geometry
     * / geography column's logical type rewritten to the native form. Otherwise returns {@code schema} unchanged. See
     * the class-level note on precedence: native annotations are honored even when {@code "geo"} disagrees.
     */
    static ParquetSchema apply(ParquetSchema schema, Map<String, String> keyValueMetadata) {
        String geo = keyValueMetadata.get(GEO_KEY);
        if (geo == null || geo.isEmpty()) {
            return schema;
        }
        try {
            return applyGeoJson(schema, geo);
        } catch (RuntimeException e) {
            // JacksonException is unchecked, so this catches both parse failures and our own ill-formed-JSON guards.
            LOG.warn("Malformed GeoParquet 'geo' metadata; leaving schema unchanged: {}", e.getMessage());
            return schema;
        }
    }

    private static ParquetSchema applyGeoJson(ParquetSchema schema, String geoJson) {
        JsonNode root = MAPPER.readTree(geoJson);
        JsonNode columns = root.get("columns");
        if (columns == null || !columns.isObject()) {
            LOG.debug("'geo' metadata has no 'columns' object; leaving schema unchanged");
            return schema;
        }
        Map<ColumnPath, LogicalType> overrides = new HashMap<>();
        columns.propertyStream()
                .forEach(entry -> processColumnEntry(schema, entry.getKey(), entry.getValue(), overrides));
        return schema.withLogicalTypes(overrides);
    }

    /**
     * Inspects one entry under {@code columns}. Skips the override when the leaf already carries a native Geometry /
     * Geography annotation; if the {@code "geo"} side would produce a different annotation, logs a warning so the stale
     * {@code "geo"} doesn't silently lie about the column.
     */
    private static void processColumnEntry(
            ParquetSchema schema, String columnName, JsonNode columnEntry, Map<ColumnPath, LogicalType> overrides) {
        ColumnPath path = ColumnPath.of(columnName);
        Optional<Field> field = schema.find(path);
        if (field.isEmpty()) {
            LOG.debug("'geo' references unknown column '{}'; skipping", columnName);
            return;
        }
        Optional<LogicalType> derived = buildLogicalType(columnEntry);
        Optional<LogicalType> existing = nativeGeoAnnotation(field.get());
        if (existing.isPresent()) {
            // Native annotation wins. Only warn when the two would disagree - matching values are silent so the common
            // case of a 2.0 file with a 1.x-compat "geo" key isn't noisy.
            derived.ifPresent(d -> {
                if (!d.equals(existing.get())) {
                    LOG.warn(
                            "Column '{}' has native annotation {} but 'geo' KV metadata says {}; native wins.",
                            columnName,
                            existing.get(),
                            d);
                }
            });
            return;
        }
        derived.ifPresent(lt -> overrides.put(path, lt));
    }

    /**
     * Returns the leaf's native Geometry / Geography logical type when present. Non-primitive fields and primitives
     * with any other (or no) logical-type annotation surface as {@link Optional#empty()}.
     */
    private static Optional<LogicalType> nativeGeoAnnotation(Field field) {
        if (!(field instanceof Field.Primitive primitive)) {
            return Optional.empty();
        }
        return primitive.logicalType().filter(GeoMetadataBridge::isGeoAnnotation);
    }

    private static boolean isGeoAnnotation(LogicalType lt) {
        return lt instanceof LogicalType.Geometry || lt instanceof LogicalType.Geography;
    }

    /**
     * Translates one {@code "columns"} entry into a {@link LogicalType.Geometry} or {@link LogicalType.Geography}.
     * GeoParquet 1.x's {@code "edges"} string ({@code planar} / {@code spherical}) maps to Geometry / Geography
     * respectively, mirroring the GeoParquet 2.0 logical-type split.
     */
    private static Optional<LogicalType> buildLogicalType(JsonNode columnEntry) {
        if (!columnEntry.isObject()) {
            return Optional.empty();
        }
        Optional<String> crs = extractCrs(columnEntry);
        String edges = textOrNull(columnEntry.get("edges"));
        if ("spherical".equalsIgnoreCase(edges)) {
            return Optional.of(new LogicalType.Geography(crs, geographyAlgorithm()));
        }
        // Default and "planar" both map to Geometry.
        return Optional.of(new LogicalType.Geometry(crs));
    }

    /**
     * Extracts {@code "crs"} as a raw JSON string for consumers (parquetry does not parse PROJJSON itself). When absent
     * the GeoParquet spec says "use the spec default" (typically OGC:CRS84); we surface that as
     * {@link Optional#empty()}.
     */
    private static Optional<String> extractCrs(JsonNode columnEntry) {
        JsonNode crs = columnEntry.get("crs");
        if (crs == null || crs.isNull()) {
            return Optional.empty();
        }
        return Optional.of(crs.toString());
    }

    /**
     * GeoParquet 1.x does not carry a Geography {@code algorithm} value (it only has {@code "edges": "spherical"} or
     * {@code "planar"}). 2.0 native files do, but the bridge only runs against 1.x; the 2.0 path is the native
     * {@link LogicalType.Geography} annotation. We always report empty here so the consumer falls back to the spec
     * default {@code SPHERICAL}.
     */
    private static Optional<EdgeInterpolationAlgorithm> geographyAlgorithm() {
        return Optional.empty();
    }

    private static String textOrNull(JsonNode node) {
        return (node == null || node.isNull()) ? null : node.asString();
    }
}
