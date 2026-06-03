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
package io.tileverse.parquetry.cli.expr;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Resolves the set of geometry columns in a Parquet file from two independent signals.
 *
 * <p>A column may be a geometry by its native Parquet logical type ({@link LogicalType.Geometry} /
 * {@link LogicalType.Geography}, GeoParquet 2.0), by being listed in the GeoParquet {@code "geo"} file metadata
 * (GeoParquet 1.x, where WKB columns have no native annotation), or by both. Both signals matter because no single one
 * covers every producer: 1.x files declare geometries only in the {@code "geo"} JSON, while 2.0 files annotate the
 * logical type and may omit or duplicate the metadata entry. The resolved set is the union of both.
 */
public final class GeometryColumns {

    private static final String GEO_METADATA_KEY = "geo";

    private GeometryColumns() {}

    public static Set<ColumnPath> resolve(ParquetSchema schema, Map<String, String> keyValueMetadata) {
        Set<ColumnPath> geometryColumns = new LinkedHashSet<>();
        addLogicalTypeGeometries(schema, geometryColumns);
        addGeoMetadataColumns(keyValueMetadata, geometryColumns);
        return geometryColumns;
    }

    private static void addLogicalTypeGeometries(ParquetSchema schema, Set<ColumnPath> geometryColumns) {
        for (ColumnPath leaf : schema.leafColumns()) {
            Optional<SchemaNode> node = schema.find(leaf);
            if (node.isPresent() && node.get() instanceof SchemaNode.Primitive primitive && isGeometry(primitive)) {
                geometryColumns.add(leaf);
            }
        }
    }

    private static void addGeoMetadataColumns(Map<String, String> keyValueMetadata, Set<ColumnPath> geometryColumns) {
        String geoJson = keyValueMetadata.get(GEO_METADATA_KEY);
        if (geoJson == null) {
            return;
        }
        GeoParquetMetadata metadata = GeoParquetMetadata.parse(geoJson);
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
