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
package io.tileverse.parquetry.geo;

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate.Spatial;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * JTS-backed exact geometry filter. Each factory method creates a filter for a specific geometric relation between
 * candidate geometries (read from the file) and a fixed query geometry. All geometries are assumed to be in the file's
 * native CRS; no reprojection is performed.
 *
 * <p>Pruning contract: every factory's {@link #pruningPredicate()} is a SOUND necessary condition of its
 * {@link #matches} test - when {@code matches(g)} is {@code true}, the bbox relation from {@code pruningPredicate()}
 * holds for {@code g}'s envelope. The reader prunes row groups and pages using that relation before decoding any WKB.
 * {@link #disjoint} has no sound bbox lowering and returns {@link Optional#empty()}, causing the reader to scan every
 * row without bbox pruning.
 *
 * <p>Thread safety: instances are thread-safe. {@link PreparedGeometry} is documented thread-safe for concurrent use
 * after construction, and {@link #decode} delegates to a shared thread-safe reader. The factories that do not use
 * {@link PreparedGeometry} ({@link #equalsExact} and {@link #dwithin}) capture the query as an unmodified reference;
 * the JTS read operations they call are safe to share across threads.
 */
public final class JtsGeometryFilter implements GeometryFilter<Geometry> {

    private final ColumnPath column;
    private final Predicate<Geometry> matcher;
    private final Optional<Spatial> lowering;

    private JtsGeometryFilter(ColumnPath column, Predicate<Geometry> matcher, Optional<Spatial> lowering) {
        this.column = column;
        this.matcher = matcher;
        this.lowering = lowering;
    }

    // --- factories ---

    /**
     * Retains candidates that intersect the query (shares at least one point). The pruning predicate is the query's
     * bounding box intersection.
     */
    public static JtsGeometryFilter intersects(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        return new JtsGeometryFilter(column, pg::intersects, bboxIntersects(column, query));
    }

    /**
     * Retains candidates that touch the query (share a boundary point but no interior). The pruning predicate is the
     * query's bounding box intersection.
     */
    public static JtsGeometryFilter touches(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        return new JtsGeometryFilter(column, pg::touches, bboxIntersects(column, query));
    }

    /**
     * Retains candidates that cross the query (share some but not all interior points, with different dimension). The
     * pruning predicate is the query's bounding box intersection.
     */
    public static JtsGeometryFilter crosses(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        return new JtsGeometryFilter(column, pg::crosses, bboxIntersects(column, query));
    }

    /**
     * Retains candidates that overlap the query (share interior points, same dimension, neither contains the other).
     * The pruning predicate is the query's bounding box intersection.
     */
    public static JtsGeometryFilter overlaps(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        return new JtsGeometryFilter(column, pg::overlaps, bboxIntersects(column, query));
    }

    /**
     * Retains candidates that contain the query (the candidate's interior contains all points of the query). The
     * pruning predicate is the query's bounding box containment: a candidate whose bbox does not contain the query bbox
     * cannot contain the query.
     *
     * <p>Note: candidate contains query is equivalent to pg(query).within(candidate), hence the matcher uses
     * {@code pg::within}.
     */
    public static JtsGeometryFilter contains(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        // candidate contains query <=> pg(query).within(candidate)
        return new JtsGeometryFilter(column, pg::within, bboxLowering(column, query, Spatial.BboxContains::new));
    }

    /**
     * Retains candidates that cover the query (every point of the query lies inside or on the boundary of the
     * candidate). The pruning predicate is the query's bounding box containment.
     *
     * <p>Note: candidate covers query is equivalent to pg(query).coveredBy(candidate), hence the matcher uses
     * {@code pg::coveredBy}.
     */
    public static JtsGeometryFilter covers(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        // candidate covers query <=> pg(query).coveredBy(candidate)
        return new JtsGeometryFilter(column, pg::coveredBy, bboxLowering(column, query, Spatial.BboxContains::new));
    }

    /**
     * Retains candidates that are within the query (all points of the candidate lie inside or on the boundary of the
     * query). The pruning predicate is the candidate bbox being covered by the query bbox.
     *
     * <p>Note: candidate within query is equivalent to pg(query).contains(candidate), hence the matcher uses
     * {@code pg::contains}.
     */
    public static JtsGeometryFilter within(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        // candidate within query <=> pg(query).contains(candidate)
        return new JtsGeometryFilter(column, pg::contains, bboxLowering(column, query, Spatial.BboxCoveredBy::new));
    }

    /**
     * Retains candidates that are covered by the query (every point of the candidate lies inside or on the boundary of
     * the query). The pruning predicate is the candidate bbox being covered by the query bbox.
     *
     * <p>Note: candidate coveredBy query is equivalent to pg(query).covers(candidate), hence the matcher uses
     * {@code pg::covers}.
     */
    public static JtsGeometryFilter coveredBy(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        // candidate coveredBy query <=> pg(query).covers(candidate)
        return new JtsGeometryFilter(column, pg::covers, bboxLowering(column, query, Spatial.BboxCoveredBy::new));
    }

    /**
     * Retains candidates that are disjoint from the query (share no points at all). There is no sound bbox lowering for
     * disjoint; {@link #pruningPredicate()} returns empty and every row is scanned.
     */
    public static JtsGeometryFilter disjoint(ColumnPath column, Geometry query) {
        PreparedGeometry pg = PreparedGeometryFactory.prepare(query);
        return new JtsGeometryFilter(column, pg::disjoint, Optional.empty());
    }

    /**
     * Retains candidates whose geometry is exactly equal to the query (same coordinate sequences, vertex-for-vertex).
     * The pruning predicate is the query's bounding box equality.
     */
    public static JtsGeometryFilter equalsExact(ColumnPath column, Geometry query) {
        return new JtsGeometryFilter(
                column,
                candidate -> candidate.equalsExact(query),
                bboxLowering(column, query, Spatial.BboxEquals::new));
    }

    /**
     * Retains candidates within {@code distance} of the query. The pruning predicate is the query's bounding box
     * expanded by {@code distance} in all directions.
     */
    public static JtsGeometryFilter dwithin(ColumnPath column, Geometry query, double distance) {
        return new JtsGeometryFilter(
                column,
                candidate -> candidate.isWithinDistance(query, distance),
                bboxIntersectsExpanded(column, query, distance));
    }

    // --- GeometryFilter implementation ---

    @Override
    public ColumnPath column() {
        return column;
    }

    @Override
    public Optional<Spatial> pruningPredicate() {
        return lowering;
    }

    @Override
    public Geometry decode(MemorySegment wkb) {
        return JtsWkb.read(wkb);
    }

    @Override
    public boolean matches(Geometry geometry) {
        return matcher.test(geometry);
    }

    // --- private helpers ---

    /**
     * Builds a {@link Spatial.BboxIntersects} lowering from the query's envelope. Used for symmetric relations
     * (intersects, touches, crosses, overlaps).
     */
    private static Optional<Spatial> bboxIntersects(ColumnPath column, Geometry query) {
        return bboxLowering(column, query, Spatial.BboxIntersects::new);
    }

    /**
     * Builds a {@link Spatial.BboxIntersects} lowering from the query's envelope expanded by {@code distance} in all
     * directions. Used for dwithin.
     */
    private static Optional<Spatial> bboxIntersectsExpanded(ColumnPath column, Geometry query, double distance) {
        Envelope env = new Envelope(query.getEnvelopeInternal());
        env.expandBy(distance);
        Bbox bbox = Bbox.of2d(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
        return Optional.of(new Spatial.BboxIntersects(column, bbox));
    }

    /**
     * Builds a lowering from the query's envelope using the supplied {@link Spatial} constructor reference. The
     * {@code constructor} receives the column path and the 2D bbox derived from the query's JTS envelope.
     */
    private static Optional<Spatial> bboxLowering(
            ColumnPath column, Geometry query, BiFunction<ColumnPath, Bbox, Spatial> constructor) {
        Envelope env = query.getEnvelopeInternal();
        Bbox bbox = Bbox.of2d(env.getMinX(), env.getMinY(), env.getMaxX(), env.getMaxY());
        return Optional.of(constructor.apply(column, bbox));
    }
}
