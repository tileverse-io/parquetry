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
package io.tileverse.parquetry.data.write;

import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.BoundingBox;

import lombok.NonNull;

/**
 * Per-geometry-column summary fed to {@link GeoMetadataWriter#v1JsonPayload}.
 *
 * <p>Aggregated by the dataset-level writer across every row group's
 * {@link io.tileverse.parquetry.format.GeospatialStatistics}: the file-level bbox is the union of every chunk's bbox,
 * and the file-level WKB type-code set is the union of every chunk's type-code set.
 *
 * @param bbox column-wide bounding box; {@link Optional#empty()} when no chunk recorded coordinates
 * @param geometryTypeCodes union of WKB type codes observed across every chunk (1 = Point, 2 = LineString, ...; +1000
 *     for Z, +2000 for M, +3000 for ZM); empty when no chunk observed any geometry
 * @param encoding wire encoding; {@code "WKB"} for every column the writer emits today
 */
public record GeoColumnSummary(
        Optional<BoundingBox> bbox,
        @NonNull List<Integer> geometryTypeCodes,
        @NonNull String encoding) {

    public GeoColumnSummary {
        geometryTypeCodes = List.copyOf(geometryTypeCodes);
        if (encoding.isBlank()) {
            throw new IllegalArgumentException("encoding must not be blank");
        }
    }

    /** Convenience constructor for the common WKB case. */
    public static GeoColumnSummary wkb(Optional<BoundingBox> bbox, List<Integer> geometryTypeCodes) {
        return new GeoColumnSummary(bbox, geometryTypeCodes, "WKB");
    }
}
