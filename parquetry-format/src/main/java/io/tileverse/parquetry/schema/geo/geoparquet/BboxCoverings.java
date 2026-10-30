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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.Optional;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Finds the bbox covering of a geometry column: four numeric columns holding a box around the geometry of each row.
 * Spatial pruning, file bounds and the attributes hidden from a GeoTools feature type find the covering here, and agree
 * on it.
 */
public final class BboxCoverings {

    private BboxCoverings() {}

    /**
     * The bbox covering of {@code geometryColumn}, or empty when the file has none for it. A covering declared by the
     * GeoParquet metadata of the column counts when its four leaves exist as primitive columns. Without a declaration,
     * the conventional {@code bbox} struct counts, written without one by GeoParquet 1.0 producers such as Overture and
     * GDAL.
     *
     * <p>The struct describes one geometry column. A file with several of them does not tell which one, and the struct
     * counts as the covering of the primary geometry column alone.
     */
    public static Optional<BboxCovering> coveringOf(
            ColumnPath geometryColumn, ParquetSchema schema, GeoParquetMetadata geo) {
        GeoColumn geoColumn = geo.columns().get(geometryColumn.dot());
        if (geoColumn == null) {
            return Optional.empty();
        }
        Optional<BboxCovering> declared = geoColumn.covering().map(Covering::bbox);
        if (declared.isPresent()) {
            return declared.filter(covering -> leavesArePrimitive(schema, covering));
        }
        return conventionalCovering(geometryColumn, schema, geo);
    }

    /** The conventional {@code bbox} struct, when the schema has it and {@code geometryColumn} is the primary one. */
    private static Optional<BboxCovering> conventionalCovering(
            ColumnPath geometryColumn, ParquetSchema schema, GeoParquetMetadata geo) {
        BboxCovering conventional = BboxCovering.conventional();
        if (!leavesArePrimitive(schema, conventional) || !isPrimary(geometryColumn, schema, geo)) {
            return Optional.empty();
        }
        return Optional.of(conventional);
    }

    private static boolean isPrimary(ColumnPath geometryColumn, ParquetSchema schema, GeoParquetMetadata geo) {
        Optional<ColumnPath> primary = GeometryColumns.primary(schema, Optional.of(geo));
        return primary.filter(geometryColumn::equals).isPresent();
    }

    private static boolean leavesArePrimitive(ParquetSchema schema, BboxCovering covering) {
        return isPrimitive(schema, covering.xmin())
                && isPrimitive(schema, covering.xmax())
                && isPrimitive(schema, covering.ymin())
                && isPrimitive(schema, covering.ymax());
    }

    private static boolean isPrimitive(ParquetSchema schema, ColumnPath path) {
        return schema.find(path)
                .filter(node -> node instanceof SchemaNode.Primitive)
                .isPresent();
    }
}
