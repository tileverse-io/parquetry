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

import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.Covering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Lowers {@link Predicate.Spatial} bbox relations into equivalent numeric comparisons on a geometry column's GeoParquet
 * 1.1 {@code covering.bbox} sidecar columns.
 *
 * <p>A covering's four leaves ({@code xmin}, {@code xmax}, {@code ymin}, {@code ymax}) hold each row's exact 2D
 * bounding box per the GeoParquet specification. Each bbox relation is therefore logically equal to a conjunction of
 * comparisons against those leaves:
 *
 * <ul>
 *   <li>intersects: {@code xmin <= q.maxX AND xmax >= q.minX AND ymin <= q.maxY AND ymax >= q.minY}
 *   <li>contains: {@code xmin <= q.minX AND xmax >= q.maxX AND ymin <= q.minY AND ymax >= q.maxY}
 *   <li>coveredBy: {@code xmin >= q.minX AND xmax <= q.maxX AND ymin >= q.minY AND ymax <= q.maxY}
 *   <li>equals: {@code xmin = q.minX AND xmax = q.maxX AND ymin = q.minY AND ymax = q.maxY}
 * </ul>
 *
 * <p>Because the covering leaves are regular numeric columns, the lowered comparisons let the STATS (row-group) and
 * COLUMN_INDEX (page) tiers prune for free, and the record-level evaluation stays exact, all without ever decoding the
 * geometry WKB for filtering. A geometry column with no resolvable covering keeps its spatial leaf untouched: the
 * record-level WKB evaluation still returns correct results, and a later mechanism handles files that have native
 * geospatial statistics instead.
 *
 * <p>This rewrite trusts the covering values exactly as the rest of the reader trusts any column's min/max statistics.
 */
public final class SpatialCoveringRewrite {

    private SpatialCoveringRewrite() {}

    /**
     * Returns {@code predicate} with every {@link Predicate.Spatial} leaf whose geometry column has a resolvable
     * covering replaced by the equivalent numeric conjunction; all other nodes are preserved. When {@code geo} is empty
     * the predicate is returned unchanged.
     */
    public static Predicate expand(Predicate predicate, ParquetSchema schema, Optional<GeoParquetMetadata> geo) {
        if (geo.isEmpty()) {
            return predicate;
        }
        return rewrite(predicate, schema, geo.orElseThrow());
    }

    private static Predicate rewrite(Predicate predicate, ParquetSchema schema, GeoParquetMetadata geo) {
        return switch (predicate) {
            case Predicate.And(List<Predicate> children) -> new Predicate.And(rewriteAll(children, schema, geo));
            case Predicate.Or(List<Predicate> children) -> new Predicate.Or(rewriteAll(children, schema, geo));
            case Predicate.Not(Predicate child) -> new Predicate.Not(rewrite(child, schema, geo));
            case Predicate.Spatial spatial -> rewriteSpatial(spatial, schema, geo);
            case Predicate.GeometryFilterPredicate gfp -> rewriteGeometryFilter(gfp, schema, geo);
            default -> predicate;
        };
    }

    private static List<Predicate> rewriteAll(List<Predicate> children, ParquetSchema schema, GeoParquetMetadata geo) {
        return children.stream().map(child -> rewrite(child, schema, geo)).toList();
    }

    private static Predicate rewriteSpatial(Predicate.Spatial spatial, ParquetSchema schema, GeoParquetMetadata geo) {
        Optional<BboxCovering> covering = coveringFor(spatial.col(), schema, geo);
        if (covering.isEmpty()) {
            return spatial;
        }
        return lower(spatial, covering.orElseThrow());
    }

    /**
     * Rewrites a {@link Predicate.GeometryFilterPredicate} when its {@link GeometryFilter#pruningPredicate()} provides
     * a bbox lowering and the geometry column has a resolvable covering. Because the lowering is only a necessary
     * condition of the exact gate (not equivalent to it), the gate leaf is preserved and the covering comparison is
     * added as a coarse pre-filter: the result is {@code And(coveringComparisons, gfp)}. When the lowering is absent or
     * no covering is resolvable, the predicate is returned unchanged.
     */
    private static Predicate rewriteGeometryFilter(
            Predicate.GeometryFilterPredicate gfp, ParquetSchema schema, GeoParquetMetadata geo) {
        Optional<Predicate.Spatial> lowering = gfp.filter().pruningPredicate();
        if (lowering.isEmpty()) {
            return gfp;
        }
        Predicate.Spatial spatial = lowering.orElseThrow();
        Optional<BboxCovering> covering = coveringFor(spatial.col(), schema, geo);
        if (covering.isEmpty()) {
            return gfp;
        }
        Predicate loweredCovering = lower(spatial, covering.orElseThrow());
        return new Predicate.And(List.of(loweredCovering, gfp));
    }

    private static Predicate lower(Predicate.Spatial spatial, BboxCovering c) {
        Bbox q = spatial.bbox();
        return switch (spatial) {
            case Predicate.Spatial.BboxIntersects _ ->
                new Predicate.And(List.of(
                        Pred.col(c.xmin()).ltEq(q.maxX()),
                        Pred.col(c.xmax()).gtEq(q.minX()),
                        Pred.col(c.ymin()).ltEq(q.maxY()),
                        Pred.col(c.ymax()).gtEq(q.minY())));
            case Predicate.Spatial.BboxContains _ ->
                new Predicate.And(List.of(
                        Pred.col(c.xmin()).ltEq(q.minX()),
                        Pred.col(c.xmax()).gtEq(q.maxX()),
                        Pred.col(c.ymin()).ltEq(q.minY()),
                        Pred.col(c.ymax()).gtEq(q.maxY())));
            case Predicate.Spatial.BboxCoveredBy _ ->
                new Predicate.And(List.of(
                        Pred.col(c.xmin()).gtEq(q.minX()),
                        Pred.col(c.xmax()).ltEq(q.maxX()),
                        Pred.col(c.ymin()).gtEq(q.minY()),
                        Pred.col(c.ymax()).ltEq(q.maxY())));
            case Predicate.Spatial.BboxEquals _ ->
                new Predicate.And(List.of(
                        Pred.col(c.xmin()).eq(q.minX()),
                        Pred.col(c.xmax()).eq(q.maxX()),
                        Pred.col(c.ymin()).eq(q.minY()),
                        Pred.col(c.ymax()).eq(q.maxY())));
        };
    }

    /**
     * Resolves the bbox covering for {@code geometryColumn}, but only when all four covering leaves exist as primitive
     * columns in {@code schema}. The geo metadata map is keyed by the geometry column's name, matching how the rest of
     * the reader looks it up.
     */
    private static Optional<BboxCovering> coveringFor(
            ColumnPath geometryColumn, ParquetSchema schema, GeoParquetMetadata geo) {
        GeoColumn geoColumn = geo.columns().get(geometryColumn.dot());
        if (geoColumn == null) {
            return Optional.empty();
        }
        Optional<BboxCovering> bbox = geoColumn.covering().map(Covering::bbox);
        if (bbox.isEmpty()) {
            return Optional.empty();
        }
        BboxCovering c = bbox.orElseThrow();
        if (allPrimitive(schema, c.xmin(), c.xmax(), c.ymin(), c.ymax())) {
            return bbox;
        }
        return Optional.empty();
    }

    private static boolean allPrimitive(ParquetSchema schema, ColumnPath... paths) {
        for (ColumnPath path : paths) {
            if (!isPrimitive(schema, path)) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPrimitive(ParquetSchema schema, ColumnPath path) {
        return schema.find(path)
                .filter(node -> node instanceof SchemaNode.Primitive)
                .isPresent();
    }
}
