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
package io.tileverse.parquetry.filter.spatial;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * {@link SpatialBoundsSource} backed by the {@code "geo"} JSON {@link GeoColumn#bbox()} entry, already parsed into the
 * typed {@link BoundingBox} record by {@code BboxDeserializer} (which permutes the GeoParquet corner-pair order into
 * {@link BoundingBox}'s flat-double order at parse time).
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
     * Returns a {@code GeoJsonFileBoundsSource} when at least one column in {@code geo} carries a bbox. Columns with no
     * bbox are skipped; a file with no geo columns or no bboxes falls through to {@link EmptyBoundsSource}.
     */
    static Optional<SpatialBoundsSource> tryBuild(GeoParquetMetadata geo) {
        Map<ColumnPath, BoundingBox> byPath = new LinkedHashMap<>();
        geo.columns()
                .forEach(
                        (name, geoColumn) -> geoColumn.bbox().ifPresent(bbox -> byPath.put(ColumnPath.of(name), bbox)));
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
}
