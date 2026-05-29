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
package io.tileverse.parquetry.filter;

import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.format.BoundingBox;

/**
 * Eliminates a row group when the geometry column's native per-row-group bounding box proves no row can satisfy a
 * {@link Predicate.Spatial} relation. This is the row-group-level counterpart of the statistics tier for spatial
 * predicates.
 *
 * <p>It only ever eliminates whole row groups; it never narrows the surviving rows within one. Native GeoParquet 2.0
 * geometry statistics are per row group, not per page: there is no finer granularity to exploit. Files that have
 * GeoParquet 1.1 covering columns are handled earlier by lowering spatial predicates onto those numeric columns; this
 * evaluator handles the case where a spatial leaf survives because the only available bounds are native row-group
 * statistics (every file parquetry itself writes).
 *
 * <p>The check is conservative: a row group is eliminated only when its union bounding box rules out every relation a
 * matching row could have. A negated spatial leaf or an absent bounding box leaves the row group in place.
 */
public final class SpatialBoundsEvaluator {

    private SpatialBoundsEvaluator() {}

    /**
     * Returns {@link PruningDecision.Eliminated} when {@code predicate} is provably unsatisfiable for the row group at
     * {@code rowGroupIndex} given {@code bounds}; otherwise {@link PruningDecision.NotApplied}.
     */
    public static PruningDecision evaluate(Predicate predicate, SpatialBoundsSource bounds, int rowGroupIndex) {
        if (isUnsatisfiable(predicate, bounds, rowGroupIndex)) {
            return new PruningDecision.Eliminated(Tier.SPATIAL, "geometry row-group bbox excludes the query");
        }
        return new PruningDecision.NotApplied(Tier.SPATIAL, "geometry row-group bbox does not exclude the query");
    }

    @SuppressWarnings("java:S6878")
    private static boolean isUnsatisfiable(Predicate predicate, SpatialBoundsSource bounds, int rowGroupIndex) {
        return switch (predicate) {
            case Predicate.And and -> anyUnsatisfiable(and.children(), bounds, rowGroupIndex);
            case Predicate.Or or -> allUnsatisfiable(or.children(), bounds, rowGroupIndex);
            case Predicate.Not _ -> false;
            case Predicate.Spatial spatial -> spatialUnsatisfiable(spatial, bounds, rowGroupIndex);
            case Predicate.GeometryFilterPredicate(GeometryFilter<?> f) ->
                f.pruningPredicate()
                        .map(spatial -> spatialUnsatisfiable(spatial, bounds, rowGroupIndex))
                        .orElse(false);
            default -> false;
        };
    }

    private static boolean anyUnsatisfiable(List<Predicate> children, SpatialBoundsSource bounds, int rowGroupIndex) {
        for (Predicate child : children) {
            if (isUnsatisfiable(child, bounds, rowGroupIndex)) {
                return true;
            }
        }
        return false;
    }

    private static boolean allUnsatisfiable(List<Predicate> children, SpatialBoundsSource bounds, int rowGroupIndex) {
        if (children.isEmpty()) {
            return false;
        }
        for (Predicate child : children) {
            if (!isUnsatisfiable(child, bounds, rowGroupIndex)) {
                return false;
            }
        }
        return true;
    }

    private static boolean spatialUnsatisfiable(
            Predicate.Spatial spatial, SpatialBoundsSource bounds, int rowGroupIndex) {
        Optional<BoundingBox> rowGroupBox = bounds.rowGroupBounds(spatial.col(), rowGroupIndex);
        if (rowGroupBox.isEmpty()) {
            return false;
        }
        BoundingBox union = rowGroupBox.orElseThrow();
        Bbox query = spatial.bbox();
        return switch (spatial) {
            case Predicate.Spatial.BboxIntersects _ -> disjoint(union, query);
            case Predicate.Spatial.BboxCoveredBy _ -> disjoint(union, query);
            case Predicate.Spatial.BboxEquals _ -> disjoint(union, query);
            case Predicate.Spatial.BboxContains _ -> !unionCanEnclose(union, query);
        };
    }

    private static boolean disjoint(BoundingBox union, Bbox query) {
        return union.xmax() < query.minX()
                || union.xmin() > query.maxX()
                || union.ymax() < query.minY()
                || union.ymin() > query.maxY();
    }

    private static boolean unionCanEnclose(BoundingBox union, Bbox query) {
        return union.xmin() <= query.minX()
                && union.xmax() >= query.maxX()
                && union.ymin() <= query.minY()
                && union.ymax() >= query.maxY();
    }
}
