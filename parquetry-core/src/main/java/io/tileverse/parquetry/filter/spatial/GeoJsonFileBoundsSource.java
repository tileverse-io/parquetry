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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * {@link SpatialBoundsSource} backed by the {@code "geo"} JSON {@link GeoColumn#bbox()} entry (4 doubles {@code [xmin,
 * ymin, xmax, ymax]} for XY only, or 6 doubles {@code [xmin, ymin, zmin, xmax, ymax, zmax]} when Z is present). The
 * order matches the GeoParquet spec, which writes XY in the corner-pair order, not the {@link BoundingBox} flat-double
 * order.
 *
 * <p>Row-group queries always return {@link Optional#empty()}: the {@code "geo"} bbox is a file-level summary, not a
 * per-row-group quantity.
 */
final class GeoJsonFileBoundsSource implements SpatialBoundsSource {

    private final Map<ColumnPath, BoundingBox> fileLevel;

    private GeoJsonFileBoundsSource(Map<ColumnPath, BoundingBox> fileLevel) {
        this.fileLevel = fileLevel;
    }

    /**
     * Returns a {@code GeoJsonFileBoundsSource} when at least one column in {@code geo} carries a parseable
     * {@code bbox} (4 or 6 numeric entries). Empty / null bboxes are skipped silently; a file whose only candidate bbox
     * is malformed falls through to {@link EmptyBoundsSource}.
     */
    static Optional<SpatialBoundsSource> tryBuild(GeoParquetMetadata geo) {
        Map<ColumnPath, BoundingBox> byPath = new LinkedHashMap<>();
        geo.columns().forEach((name, geoColumn) -> tryAddBbox(name, geoColumn, byPath));
        if (byPath.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new GeoJsonFileBoundsSource(Map.copyOf(byPath)));
    }

    @Override
    public Optional<BoundingBox> fileBounds(ColumnPath geometryColumn) {
        return Optional.ofNullable(fileLevel.get(geometryColumn));
    }

    @Override
    public Optional<BoundingBox> rowGroupBounds(ColumnPath geometryColumn, int rowGroupIndex) {
        return Optional.empty();
    }

    private static void tryAddBbox(String columnName, GeoColumn geoColumn, Map<ColumnPath, BoundingBox> sink) {
        geoColumn
                .bbox()
                .flatMap(GeoJsonFileBoundsSource::parseBbox)
                .ifPresent(bbox -> sink.put(ColumnPath.of(columnName), bbox));
    }

    /**
     * Parses a GeoParquet bbox list into {@link BoundingBox}. Accepts either 4 entries (XY) or 6 entries (XYZ). The
     * spec writes the corner-pair order {@code [xmin, ymin, zmin, xmax, ymax, zmax]}; we permute that into the
     * flat-double order {@link BoundingBox} uses. Any other length is treated as malformed and surfaces as empty.
     */
    private static Optional<BoundingBox> parseBbox(List<Double> bbox) {
        return switch (bbox.size()) {
            case 4 ->
                Optional.of(new BoundingBox(
                        bbox.get(0),
                        bbox.get(2),
                        bbox.get(1),
                        bbox.get(3),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty(),
                        OptionalDouble.empty()));
            case 6 ->
                Optional.of(new BoundingBox(
                        bbox.get(0),
                        bbox.get(3),
                        bbox.get(1),
                        bbox.get(4),
                        OptionalDouble.of(bbox.get(2)),
                        OptionalDouble.of(bbox.get(5)),
                        OptionalDouble.empty(),
                        OptionalDouble.empty()));
            default -> Optional.empty();
        };
    }
}
