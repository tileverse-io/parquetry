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
package io.tileverse.parquetry.data;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.stream.Stream;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.internal.write.GeoColumnSummary;

/**
 * Folds a row group's geospatial statistics into the running file-level {@link GeoColumnSummary} for a geometry column:
 * the union of the per-row-group bounding boxes and the distinct geometry type codes seen so far.
 */
final class GeoSummaryMerger {

    private GeoSummaryMerger() {}

    static GeoColumnSummary mergeGeoSummary(GeoColumnSummary previous, GeospatialStatistics chunk) {
        Optional<BoundingBox> mergedBbox = chunk.bbox();
        List<Integer> left = List.of();
        if (previous != null) {
            mergedBbox = unionBbox(previous.bbox(), chunk.bbox());
            left = previous.geometryTypeCodes();
        }
        List<Integer> right = chunk.geospatialTypes().orElse(List.of());
        List<Integer> mergedTypes = unionTypeCodes(left, right);
        return GeoColumnSummary.wkb(mergedBbox, mergedTypes);
    }

    private static Optional<BoundingBox> unionBbox(Optional<BoundingBox> left, Optional<BoundingBox> right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        BoundingBox a = left.get();
        BoundingBox b = right.get();
        return Optional.of(BoundingBox.builder()
                .xmin(Math.min(a.xmin(), b.xmin()))
                .xmax(Math.max(a.xmax(), b.xmax()))
                .ymin(Math.min(a.ymin(), b.ymin()))
                .ymax(Math.max(a.ymax(), b.ymax()))
                .zmin(unionMin(a.zmin(), b.zmin()))
                .zmax(unionMax(a.zmax(), b.zmax()))
                .mmin(unionMin(a.mmin(), b.mmin()))
                .mmax(unionMax(a.mmax(), b.mmax()))
                .build());
    }

    private static OptionalDouble unionMin(OptionalDouble left, OptionalDouble right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return OptionalDouble.of(Math.min(left.getAsDouble(), right.getAsDouble()));
    }

    private static OptionalDouble unionMax(OptionalDouble left, OptionalDouble right) {
        if (left.isEmpty()) {
            return right;
        }
        if (right.isEmpty()) {
            return left;
        }
        return OptionalDouble.of(Math.max(left.getAsDouble(), right.getAsDouble()));
    }

    private static List<Integer> unionTypeCodes(List<Integer> left, List<Integer> right) {
        return Stream.concat(left.stream(), right.stream())
                .distinct()
                .sorted(Comparator.naturalOrder())
                .toList();
    }
}
