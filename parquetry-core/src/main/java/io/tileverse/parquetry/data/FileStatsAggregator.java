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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.internal.filter.FilterPipeline;
import io.tileverse.parquetry.internal.filter.StatsEvaluator;
import io.tileverse.parquetry.internal.filter.StatsEvaluator.ColumnSummary;
import io.tileverse.parquetry.internal.filter.prune.FooterStatsAggregator;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.read.RowGroupChunks;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Builds the footer-aggregated prunable {@link FileStats} for one file from its row-group chunk views: per-column
 * min/max/null-count combined across the file's row groups, the geometry bounding box for each geometry column, and the
 * record count. Reads only the cached footer metadata, no page data.
 */
final class FileStatsAggregator {

    private final FileMetaData footer;
    private final ParquetSchema fileSchema;
    private final Optional<GeoParquetMetadata> geoMetadata;

    FileStatsAggregator(FileMetaData footer, ParquetSchema fileSchema, Optional<GeoParquetMetadata> geoMetadata) {
        this.footer = footer;
        this.fileSchema = fileSchema;
        this.geoMetadata = geoMetadata;
    }

    FileStats aggregate(List<RowGroupChunks> groups) {
        FileStats.Builder builder = FileStats.builder().recordCount(recordCount(groups));
        Set<ColumnPath> geometryColumns = geometryColumnPaths();
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            if (geometryColumns.contains(leaf)) {
                continue;
            }
            FooterStatsAggregator.aggregate(columnSummaries(groups, leaf))
                    .ifPresent(stats -> builder.column(leaf, stats));
        }
        addGeometryBounds(builder, geometryColumns);
        return builder.build();
    }

    private static long recordCount(List<RowGroupChunks> groups) {
        long total = 0L;
        for (RowGroupChunks group : groups) {
            total += group.numRows();
        }
        return total;
    }

    private static List<Optional<ColumnSummary>> columnSummaries(List<RowGroupChunks> groups, ColumnPath leaf) {
        List<Optional<ColumnSummary>> summaries = new ArrayList<>(groups.size());
        for (RowGroupChunks group : groups) {
            summaries.add(safeSummary(group, leaf));
        }
        return summaries;
    }

    /**
     * Decodes one row group's statistics for {@code leaf}, best-effort: a truncated or malformed footer value degrades
     * that row group's column to no statistics rather than failing the read. The aggregate then leaves the column out
     * of pruning. This mirrors the Iceberg manifest path, which never lets an unreadable bound fail a read.
     */
    private static Optional<ColumnSummary> safeSummary(RowGroupChunks group, ColumnPath leaf) {
        Optional<FilterPipeline.ColumnStats> stats = group.stats(leaf);
        if (stats.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(StatsEvaluator.summarize(stats.orElseThrow()));
        } catch (RuntimeException _) {
            // A malformed or truncated footer value disables pruning for this column; it never fails the read.
            return Optional.empty();
        }
    }

    /**
     * The geometry leaf columns, kept out of the numeric statistics. A GeoParquet 1.x file names its (top-level)
     * geometry columns only in the {@code geo} metadata; a GeoParquet 2.0 file annotates the leaf with a
     * GEOMETRY/GEOGRAPHY logical type. Honoring both keeps a WKB column out of {@code columns()} regardless of which
     * encoding wrote it, even when the two disagree.
     */
    private Set<ColumnPath> geometryColumnPaths() {
        Set<ColumnPath> paths = new LinkedHashSet<>();
        geoMetadata.ifPresent(geo -> geo.columns().keySet().forEach(name -> paths.add(ColumnPath.of(name))));
        for (ColumnPath leaf : fileSchema.leafColumns()) {
            if (isGeometryLeaf(leaf)) {
                paths.add(leaf);
            }
        }
        return paths;
    }

    private boolean isGeometryLeaf(ColumnPath leaf) {
        return fileSchema.find(leaf).orElse(null) instanceof SchemaNode.Primitive primitive
                && primitive
                        .logicalType()
                        .map(FileStatsAggregator::isGeometryLike)
                        .orElse(false);
    }

    private static boolean isGeometryLike(LogicalType logicalType) {
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    private void addGeometryBounds(FileStats.Builder builder, Set<ColumnPath> geometryColumns) {
        if (geometryColumns.isEmpty()) {
            return;
        }
        SpatialBoundsSource bounds = SpatialBoundsSource.of(footer, fileSchema, geoMetadata);
        for (ColumnPath geometry : geometryColumns) {
            bounds.fileBounds(geometry).ifPresent(box -> builder.geometryBounds(geometry, box));
        }
    }
}
