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
package io.tileverse.parquetry.format;

import java.util.List;
import java.util.Optional;

import lombok.Builder;

/**
 * Geospatial column statistics; mirror of {@code GeospatialStatistics} in {@code parquet.thrift}.
 *
 * <p>Carried by {@link ColumnMetaData#geospatialStatistics()} for GEOMETRY and GEOGRAPHY logical types. Both fields are
 * independently optional: a writer may emit a bounding box without the type set, the type set without a bounding box,
 * both, or neither.
 *
 * @param bbox optional {@link BoundingBox} covering every non-null value in the column chunk
 * @param geospatialTypes optional list of WKB type codes for the geometry kinds observed in the column chunk (1 =
 *     Point, 2 = LineString, 3 = Polygon, 4 = MultiPoint, 5 = MultiLineString, 6 = MultiPolygon, 7 =
 *     GeometryCollection; +1000 for Z, +2000 for M, +3000 for ZM). The set is kept as raw integers so future WKB
 *     extensions read through without source changes; semantic uniqueness is the writer's responsibility
 */
// S2789: constructor null-tolerates Optional to support the @Builder pattern.
@SuppressWarnings("java:S2789")
@Builder
public record GeospatialStatistics(Optional<BoundingBox> bbox, Optional<List<Integer>> geospatialTypes) {

    public GeospatialStatistics {
        bbox = bbox == null ? Optional.empty() : bbox;
        geospatialTypes = geospatialTypes == null ? Optional.empty() : geospatialTypes.map(List::copyOf);
    }
}
