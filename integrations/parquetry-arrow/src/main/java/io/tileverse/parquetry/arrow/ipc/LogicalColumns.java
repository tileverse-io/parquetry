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
package io.tileverse.parquetry.arrow.ipc;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Resolves a projected {@link ParquetSchema} into its top-level (logical) columns: one entry per direct child of the
 * schema root, each pairing the column's top-level {@link ColumnPath} with its recursive {@link ArrowField}. A
 * composite group (list, struct, map, or Parquet Variant) is a single logical column keyed at its group path, exactly
 * as the read path keys it. Both Arrow emitters iterate these columns instead of the physical leaf columns.
 */
public final class LogicalColumns {

    private LogicalColumns() {
        // utility
    }

    /** One top-level column: its path under the schema root and the Arrow field tree resolved for it. */
    public record LogicalColumn(ColumnPath path, ArrowField field) {}

    /**
     * Resolves the logical columns of {@code schema}, attaching GeoArrow extension metadata to geometry leaves found
     * either by the GeoParquet 2.0 logical type or by the optional GeoParquet 1.x {@code geo} metadata.
     */
    public static List<LogicalColumn> of(ParquetSchema schema, Optional<GeoParquetMetadata> geo) {
        return of(schema, GeoArrowFields.resolve(schema, geo));
    }

    /**
     * Resolves the logical columns of {@code schema}, attaching GeoArrow extension metadata to geometry leaves. Each
     * resolved field is checked for an exportable shape, which rejects a Parquet Variant nested under a list or map for
     * every Arrow emitter that resolves through here.
     */
    static List<LogicalColumn> of(ParquetSchema schema, GeoArrowFields geometry) {
        List<LogicalColumn> columns = new ArrayList<>();
        for (SchemaNode node : schema.root().children()) {
            ColumnPath path = ColumnPath.of(node.name());
            ArrowField field = ArrowField.of(node, geometry);
            field.validateExportable();
            columns.add(new LogicalColumn(path, field));
        }
        return List.copyOf(columns);
    }
}
