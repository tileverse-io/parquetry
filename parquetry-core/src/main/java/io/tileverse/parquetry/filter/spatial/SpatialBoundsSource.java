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
package io.tileverse.parquetry.filter.spatial;

import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Single accessor for "what bounding box does column X cover (in the whole file, or within a specific row group)?".
 *
 * <p>Decouples consumers (a GeoTools datastore, an in-pipeline spatial evaluator, etc.) from the three different places
 * a Parquet file can carry that information.
 *
 * <p>Three real backings plus an empty fallback:
 *
 * <ul>
 *   <li>{@link NativeStatsSource} - GeoParquet 2.0 native {@code GeospatialStatistics.bbox} on each row group's column
 *       metadata. Most precise; per-row-group AND file-level bounds available.
 *   <li>{@link CoveringColumnSource} - GeoParquet 1.1 {@code covering.bbox} sidecar columns. Bounds are recovered from
 *       the numeric {@code Statistics} (min/max) on the referenced bbox columns. Per-row-group AND file-level.
 *   <li>{@link GeoJsonFileBoundsSource} - GeoParquet 1.x {@code GeoColumn.bbox} (the 4- or 6-double bbox in the
 *       {@code "geo"} JSON). File-level only; row-group queries always return {@link Optional#empty()}.
 *   <li>{@link EmptyBoundsSource} - no geo / no usable bounds in any tier; every query returns
 *       {@link Optional#empty()}.
 * </ul>
 *
 * <p>{@link #of(FileMetaData, ParquetSchema, Optional)} picks the most precise applicable backing for the file as a
 * whole. Native wins unconditionally. Then covering. Then geo JSON bbox. Else empty.
 *
 * <p>{@link BoundingBox#wrapsAntimeridian()} surfaces at this layer too: aggregators that take the union across files
 * must split such a bbox into two non-wrapping pieces before joining. {@code SpatialBoundsSource} returns the bbox
 * exactly as it was written; it does not normalize.
 */
public sealed interface SpatialBoundsSource
        permits NativeStatsSource, CoveringColumnSource, GeoJsonFileBoundsSource, EmptyBoundsSource {

    /** Bounds for {@code geometryColumn} across the whole file, or {@link Optional#empty()} when not known. */
    Optional<BoundingBox> fileBounds(ColumnPath geometryColumn);

    /**
     * Bounds for {@code geometryColumn} within the given row group, or {@link Optional#empty()} when not known
     * (including the case where the backing only carries file-level bounds, as with {@link GeoJsonFileBoundsSource}).
     */
    Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex);

    /**
     * Picks the most precise applicable backing for {@code footer}.
     *
     * <p>Dispatch is intentionally file-wide rather than per-column: a file's writer is overwhelmingly likely to be
     * consistent across geometry columns, and a single chosen tier keeps the dispatch table small. A column that the
     * chosen backing doesn't know about surfaces as {@link Optional#empty()} from both accessors - the caller can
     * decide what to do with that.
     */
    static SpatialBoundsSource of(FileMetaData footer, ParquetSchema schema, Optional<GeoParquetMetadata> geo) {
        Optional<SpatialBoundsSource> nativeSource = NativeStatsSource.tryBuild(footer);
        if (nativeSource.isPresent()) {
            return nativeSource.orElseThrow();
        }
        if (geo.isPresent()) {
            Optional<SpatialBoundsSource> covering = CoveringColumnSource.tryBuild(footer, schema, geo.orElseThrow());
            if (covering.isPresent()) {
                return covering.orElseThrow();
            }
            Optional<SpatialBoundsSource> geoJson = GeoJsonFileBoundsSource.tryBuild(geo.orElseThrow());
            if (geoJson.isPresent()) {
                return geoJson.orElseThrow();
            }
        }
        return EmptyBoundsSource.INSTANCE;
    }
}
