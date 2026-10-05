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
package io.tileverse.parquetry.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.explain.PruningDecision;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.internal.filter.spatial.SpatialBoundsSource;
import io.tileverse.parquetry.internal.filter.spatial.SuppliedBoundsSource;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Row-group elimination by the SPATIAL tier for the four bbox relations. A row-group box wrapping the antimeridian
 * bounds the y values of its rows but no planar x range: a row from longitude 179 to -179 has a planar envelope
 * spanning x [-179, 179]. Such a box may eliminate on y alone.
 */
class SpatialBoundsEvaluatorTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    /** Longitudes [170, 180] and [-180, -170], latitudes [10, 20]. */
    private static final BoundingBox WRAPPING = box(170, -170, 10, 20);

    /** Longitudes [10, 20], latitudes [10, 20]. */
    private static final BoundingBox REGULAR = box(10, 20, 10, 20);

    static Stream<Relation> relations() {
        return Stream.of(Relation.values());
    }

    @ParameterizedTest
    @MethodSource("relations")
    void aWrappingBoxKeepsAQueryInTheGapBetweenItsLongitudes(Relation relation) {
        Bbox inTheGap = Bbox.of2d(0, 12, 5, 15);

        assertThat(evaluate(relation, WRAPPING, inTheGap)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @ParameterizedTest
    @MethodSource("relations")
    void aWrappingBoxKeepsAQueryOnEitherSideOfTheAntimeridian(Relation relation) {
        Bbox west = Bbox.of2d(-180, 12, -175, 15);
        Bbox east = Bbox.of2d(175, 12, 180, 15);

        assertThat(evaluate(relation, WRAPPING, west)).isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(evaluate(relation, WRAPPING, east)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @ParameterizedTest
    @MethodSource("relations")
    void aWrappingBoxEliminatesAQueryOutsideItsLatitudes(Relation relation) {
        Bbox north = Bbox.of2d(175, 30, 180, 35);

        assertThat(evaluate(relation, WRAPPING, north)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @ParameterizedTest
    @MethodSource("relations")
    void aRegularBoxEliminatesAQueryOutsideItsLongitudes(Relation relation) {
        Bbox east = Bbox.of2d(30, 12, 35, 15);

        assertThat(evaluate(relation, REGULAR, east)).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @ParameterizedTest
    @MethodSource("relations")
    void aRegularBoxKeepsAQueryInsideIt(Relation relation) {
        Bbox inside = Bbox.of2d(12, 12, 15, 15);

        assertThat(evaluate(relation, REGULAR, inside)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @ParameterizedTest
    @MethodSource("relations")
    void aWrappingBoxKeepsAQueryAcrossTheWholeLongitudeRange(Relation relation) {
        Bbox worldBand = Bbox.of2d(-180, 12, 180, 15);

        assertThat(evaluate(relation, WRAPPING, worldBand)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    @Test
    void containmentNeedsTheLatitudesOfTheQueryInsideAWrappingBox() {
        Bbox reachingSouth = Bbox.of2d(0, 5, 5, 15);

        assertThat(evaluate(Relation.CONTAINS, WRAPPING, reachingSouth)).isInstanceOf(PruningDecision.Eliminated.class);
        assertThat(evaluate(Relation.INTERSECTS, WRAPPING, reachingSouth))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    /** A box with a NaN bound proves nothing about its rows, whichever relation the query asks for. */
    @ParameterizedTest
    @MethodSource("relations")
    void aBoxWithANaNBoundEliminatesNothing(Relation relation) {
        BoundingBox nanMaximum = box(10, Double.NaN, 10, 20);
        Bbox insideTheRealBounds = Bbox.of2d(12, 12, 15, 15);
        Bbox westOfTheRealBounds = Bbox.of2d(0, 12, 5, 15);

        assertThat(evaluate(relation, nanMaximum, insideTheRealBounds)).isInstanceOf(PruningDecision.NotApplied.class);
        assertThat(evaluate(relation, nanMaximum, westOfTheRealBounds)).isInstanceOf(PruningDecision.NotApplied.class);
    }

    /** The box of a row group holding no geometry with an extent still eliminates the row group. */
    @ParameterizedTest
    @MethodSource("relations")
    void anEmptyBoxEliminatesEachQuery(Relation relation) {
        BoundingBox empty = box(
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);

        assertThat(evaluate(relation, empty, Bbox.of2d(12, 12, 15, 15))).isInstanceOf(PruningDecision.Eliminated.class);
    }

    @Test
    void anExactContainmentFilterEliminatesNothingOnABoxWithANaNBound() {
        Geometry query = new GeometryFactory().toGeometry(new Envelope(12, 15, 12, 15));
        Predicate containment = new Predicate.GeometryFilterPredicate(JtsGeometryFilter.contains(GEOMETRY, query));
        SpatialBoundsSource bounds = new SuppliedBoundsSource(Map.of(GEOMETRY, box(10, Double.NaN, 10, 20)));

        assertThat(SpatialBoundsEvaluator.evaluate(containment, bounds, 0))
                .isInstanceOf(PruningDecision.NotApplied.class);
    }

    private static PruningDecision evaluate(Relation relation, BoundingBox rowGroupBox, Bbox query) {
        SpatialBoundsSource bounds = new SuppliedBoundsSource(Map.of(GEOMETRY, rowGroupBox));
        Predicate predicate = relation.predicate(query);
        return SpatialBoundsEvaluator.evaluate(predicate, bounds, 0);
    }

    private static BoundingBox box(double xmin, double xmax, double ymin, double ymax) {
        return BoundingBox.builder().xmin(xmin).xmax(xmax).ymin(ymin).ymax(ymax).build();
    }

    enum Relation {
        INTERSECTS(Predicate.Spatial.BboxIntersects::new),
        CONTAINS(Predicate.Spatial.BboxContains::new),
        COVERED_BY(Predicate.Spatial.BboxCoveredBy::new),
        EQUALS(Predicate.Spatial.BboxEquals::new);

        private final BiFunction<ColumnPath, Bbox, Predicate> factory;

        Relation(BiFunction<ColumnPath, Bbox, Predicate> factory) {
            this.factory = factory;
        }

        Predicate predicate(Bbox query) {
            return factory.apply(GEOMETRY, query);
        }
    }
}
