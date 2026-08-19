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
package io.tileverse.parquetry.internal.filter.spatial;

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
 * Adds numeric comparisons on a geometry column's GeoParquet 1.1 {@code covering.bbox} sidecar columns to a spatial
 * predicate, alongside the relation they were derived from.
 *
 * <p>A covering's four leaves ({@code xmin}, {@code xmax}, {@code ymin}, {@code ymax}) hold a box that encloses each
 * row's geometry. GeoParquet allows that box to be wider than the geometry's own bounding box, and parquetry's own
 * FLOAT covering is wider by design: it rounds every bound outward. The lowered comparisons are therefore a necessary
 * condition of the relation rather than an equivalent of it, and only the relations that stay true when the box grows
 * keep their own shape:
 *
 * <ul>
 *   <li>intersects, coveredBy: the covering overlaps the query box, {@code xmin <= q.maxX AND xmax >= q.minX AND ymin
 *       <= q.maxY AND ymax >= q.minY}
 *   <li>contains, equals: the covering encloses the query box, {@code xmin <= q.minX AND xmax >= q.maxX AND ymin <=
 *       q.minY AND ymax >= q.maxY}
 * </ul>
 *
 * <p>Because the covering leaves are regular numeric columns, those comparisons let the STATS (row-group) and
 * COLUMN_INDEX (page) tiers prune before any geometry WKB is decoded. The relation itself stays in the predicate and
 * decides each surviving row from its geometry, which is what keeps the answer exact. A geometry column with no
 * resolvable covering keeps its spatial leaf alone: record-level WKB evaluation still returns correct results, and
 * native row-group geospatial statistics prune the files that provide them.
 *
 * <p>This rewrite trusts the covering values exactly as the rest of the reader trusts any column's min/max statistics:
 * as bounds on what the row holds.
 */
public final class SpatialCoveringRewrite {

    private SpatialCoveringRewrite() {}

    /**
     * Returns {@code predicate} with every {@link Predicate.Spatial} leaf whose geometry column has a resolvable
     * covering conjoined with the numeric comparisons that leaf implies on the covering columns; all other nodes are
     * preserved. When {@code geo} is empty the predicate is returned unchanged.
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

    /**
     * Rewrites a {@link Predicate.Spatial} leaf when its geometry column has a resolvable covering. The covering
     * comparisons come first as a coarse pre-filter and the relation follows to decide each surviving row from its
     * geometry: the result is {@code And(coveringComparisons, spatial)}. With no resolvable covering the leaf is
     * returned unchanged.
     */
    private static Predicate rewriteSpatial(Predicate.Spatial spatial, ParquetSchema schema, GeoParquetMetadata geo) {
        Optional<BboxCovering> covering = coveringFor(spatial.col(), schema, geo);
        if (covering.isEmpty()) {
            return spatial;
        }
        Predicate loweredCovering = lower(spatial, covering.orElseThrow());
        return new Predicate.And(List.of(loweredCovering, spatial));
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

    /**
     * The strongest comparison on the covering columns that every row satisfying {@code spatial} also satisfies. A row
     * whose geometry is enclosed by the query box, or whose bounding box equals it, may still have a covering that
     * reaches outside; those two relations lower to the weaker one they imply, which the wider box cannot falsify.
     */
    private static Predicate lower(Predicate.Spatial spatial, BboxCovering c) {
        Bbox q = spatial.bbox();
        return switch (spatial) {
            case Predicate.Spatial.BboxIntersects _ -> coveringOverlaps(c, q);
            case Predicate.Spatial.BboxCoveredBy _ -> coveringOverlaps(c, q);
            case Predicate.Spatial.BboxContains _ -> coveringEncloses(c, q);
            case Predicate.Spatial.BboxEquals _ -> coveringEncloses(c, q);
        };
    }

    /** The covering box shares at least one point with the query box, edges inclusive. */
    private static Predicate coveringOverlaps(BboxCovering c, Bbox q) {
        return new Predicate.And(List.of(
                Pred.col(c.xmin()).ltEq(q.maxX()),
                Pred.col(c.xmax()).gtEq(q.minX()),
                Pred.col(c.ymin()).ltEq(q.maxY()),
                Pred.col(c.ymax()).gtEq(q.minY())));
    }

    /** The covering box holds every point of the query box, edges inclusive. */
    private static Predicate coveringEncloses(BboxCovering c, Bbox q) {
        return new Predicate.And(List.of(
                Pred.col(c.xmin()).ltEq(q.minX()),
                Pred.col(c.xmax()).gtEq(q.maxX()),
                Pred.col(c.ymin()).ltEq(q.minY()),
                Pred.col(c.ymax()).gtEq(q.maxY())));
    }

    /**
     * Resolves the bbox covering for {@code geometryColumn}: the GeoParquet {@code covering.bbox} declaration when the
     * geo metadata provides one and all four of its leaves exist as primitive columns, otherwise the conventional
     * {@code bbox} struct that GeoParquet 1.0 writers (Overture, GDAL) emit without declaring it. The geo metadata map
     * is keyed by the geometry column's name, matching how the rest of the reader looks it up.
     */
    private static Optional<BboxCovering> coveringFor(
            ColumnPath geometryColumn, ParquetSchema schema, GeoParquetMetadata geo) {
        GeoColumn geoColumn = geo.columns().get(geometryColumn.dot());
        if (geoColumn == null) {
            return Optional.empty();
        }
        Optional<BboxCovering> declared = geoColumn.covering().map(Covering::bbox);
        if (declared.isPresent()) {
            BboxCovering c = declared.orElseThrow();
            return allPrimitive(schema, c.xmin(), c.xmax(), c.ymin(), c.ymax()) ? declared : Optional.empty();
        }
        return conventionalBboxCovering(schema);
    }

    /**
     * The de-facto bbox covering a file exposes without declaring it: a {@code bbox} struct whose four leaves
     * {@code xmin}/{@code xmax}/{@code ymin}/{@code ymax} enclose each row's geometry. Resolves only when all four
     * exist as primitive columns; the lowered comparisons are then trusted exactly like a declared covering.
     */
    private static Optional<BboxCovering> conventionalBboxCovering(ParquetSchema schema) {
        ColumnPath xmin = ColumnPath.of("bbox", "xmin");
        ColumnPath xmax = ColumnPath.of("bbox", "xmax");
        ColumnPath ymin = ColumnPath.of("bbox", "ymin");
        ColumnPath ymax = ColumnPath.of("bbox", "ymax");
        if (allPrimitive(schema, xmin, xmax, ymin, ymax)) {
            return Optional.of(new BboxCovering(xmin, xmax, ymin, ymax, Optional.empty(), Optional.empty()));
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
