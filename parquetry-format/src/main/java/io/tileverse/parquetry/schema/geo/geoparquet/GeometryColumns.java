/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Resolves the geometry columns of a Parquet file from two independent signals.
 *
 * <p>A column is a geometry by its native Parquet logical type ({@link LogicalType.Geometry} /
 * {@link LogicalType.Geography}, GeoParquet 2.0), by being listed in the GeoParquet {@code "geo"} file metadata
 * (GeoParquet 1.x, where WKB columns have no native annotation), or by both. No single signal covers every producer:
 * 1.x files declare geometries only in the {@code "geo"} JSON, while 2.0 files annotate the logical type and may omit
 * or duplicate the metadata entry. {@link #resolve} returns the union; {@link #primary} picks the GeoParquet
 * {@code primary_column} when it is one of them.
 */
public final class GeometryColumns {

    private static final String GEO_METADATA_KEY = "geo";

    private GeometryColumns() {}

    /** The geometry columns named by the schema's logical types or by the parsed {@code "geo"} metadata, or both. */
    public static Set<ColumnPath> resolve(ParquetSchema schema, Optional<GeoParquetMetadata> geoMetadata) {
        Set<ColumnPath> geometryColumns = new LinkedHashSet<>();
        addLogicalTypeGeometries(schema, geometryColumns);
        geoMetadata.ifPresent(metadata -> addGeoMetadataColumns(metadata, geometryColumns));
        return geometryColumns;
    }

    /** The geometry columns, reading the {@code "geo"} metadata straight from a file's raw key-value metadata. */
    public static Set<ColumnPath> resolve(ParquetSchema schema, Map<String, String> keyValueMetadata) {
        return resolve(schema, parseGeoMetadata(keyValueMetadata));
    }

    /**
     * The file's primary geometry column: the GeoParquet {@code primary_column} when the metadata names one that is a
     * resolved geometry, otherwise the first resolved geometry column in schema order. Empty when the file has no
     * geometry column.
     */
    public static Optional<ColumnPath> primary(ParquetSchema schema, Optional<GeoParquetMetadata> geoMetadata) {
        Set<ColumnPath> geometryColumns = resolve(schema, geoMetadata);
        if (geometryColumns.isEmpty()) {
            return Optional.empty();
        }
        Optional<ColumnPath> declaredPrimary = geoMetadata
                .map(GeoParquetMetadata::primaryColumn)
                .filter(name -> !name.isBlank())
                .map(name -> ColumnPath.of(name.split("\\.")))
                .filter(geometryColumns::contains);
        return declaredPrimary.or(() -> geometryColumns.stream().findFirst());
    }

    private static Optional<GeoParquetMetadata> parseGeoMetadata(Map<String, String> keyValueMetadata) {
        String geoJson = keyValueMetadata.get(GEO_METADATA_KEY);
        if (geoJson == null) {
            return Optional.empty();
        }
        return Optional.of(GeoParquetMetadata.parse(geoJson));
    }

    private static void addLogicalTypeGeometries(ParquetSchema schema, Set<ColumnPath> geometryColumns) {
        for (ColumnPath leaf : schema.leafColumns()) {
            Optional<SchemaNode> node = schema.find(leaf);
            if (node.isPresent() && node.get() instanceof SchemaNode.Primitive primitive && isGeometry(primitive)) {
                geometryColumns.add(leaf);
            }
        }
    }

    private static void addGeoMetadataColumns(GeoParquetMetadata metadata, Set<ColumnPath> geometryColumns) {
        for (String columnName : metadata.columns().keySet()) {
            geometryColumns.add(ColumnPath.of(columnName.split("\\.")));
        }
    }

    private static boolean isGeometry(SchemaNode.Primitive primitive) {
        Optional<LogicalType> logicalType = primitive.logicalType();
        if (logicalType.isEmpty()) {
            return false;
        }
        LogicalType type = logicalType.get();
        return type instanceof LogicalType.Geometry || type instanceof LogicalType.Geography;
    }
}
