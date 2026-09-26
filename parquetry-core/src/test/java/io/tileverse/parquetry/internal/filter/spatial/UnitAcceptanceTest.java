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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.geo.JtsGeometryFilter;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Pins which predicate shapes let a unit be accepted from its box alone. Acceptance means every row of the unit
 * satisfies the predicate; the function answers false whenever that is not provable from the box.
 */
class UnitAcceptanceTest {

    private static final ColumnPath GEOM = ColumnPath.of("geometry");
    private static final ColumnPath OTHER = ColumnPath.of("other_geometry");
    private static final Bbox QUERY = Bbox.of2d(0, 0, 10, 10);
    private static final Bbox INSIDE = Bbox.of2d(2, 2, 4, 4);
    private static final Bbox STRADDLING = Bbox.of2d(8, 8, 12, 12);

    @Test
    void alwaysTrueAccepts() {
        assertThat(UnitAcceptance.accepts(Predicate.ALWAYS_TRUE, GEOM, INSIDE)).isTrue();
        assertThat(UnitAcceptance.accepts(Predicate.ALWAYS_FALSE, GEOM, INSIDE)).isFalse();
    }

    @Test
    void bboxIntersectsAcceptsAUnitWithinTheQueryBox() {
        Predicate intersects = new Predicate.Spatial.BboxIntersects(GEOM, QUERY);
        assertThat(UnitAcceptance.accepts(intersects, GEOM, INSIDE)).isTrue();
        assertThat(UnitAcceptance.accepts(intersects, GEOM, Bbox.of2d(0, 0, 10, 10)))
                .as("edges are inclusive")
                .isTrue();
        assertThat(UnitAcceptance.accepts(intersects, GEOM, STRADDLING)).isFalse();
    }

    @Test
    void bboxCoveredByAcceptsAUnitWithinTheQueryBox() {
        Predicate coveredBy = new Predicate.Spatial.BboxCoveredBy(GEOM, QUERY);
        assertThat(UnitAcceptance.accepts(coveredBy, GEOM, INSIDE)).isTrue();
        assertThat(UnitAcceptance.accepts(coveredBy, GEOM, STRADDLING)).isFalse();
    }

    @Test
    void containsAndEqualsNeverAccept() {
        assertThat(UnitAcceptance.accepts(new Predicate.Spatial.BboxContains(GEOM, INSIDE), GEOM, INSIDE))
                .isFalse();
        assertThat(UnitAcceptance.accepts(new Predicate.Spatial.BboxEquals(GEOM, INSIDE), GEOM, INSIDE))
                .isFalse();
    }

    @Test
    void aSpatialLeafOnAnotherGeometryColumnNeverAccepts() {
        Predicate intersects = new Predicate.Spatial.BboxIntersects(OTHER, QUERY);
        assertThat(UnitAcceptance.accepts(intersects, GEOM, INSIDE)).isFalse();
    }

    @Test
    void andAcceptsOnlyWhenEveryChildAccepts() {
        Predicate spatial = new Predicate.Spatial.BboxIntersects(GEOM, QUERY);
        Predicate attribute = Pred.col("height").gt(3);
        assertThat(UnitAcceptance.accepts(new Predicate.And(List.of(spatial, spatial)), GEOM, INSIDE))
                .isTrue();
        assertThat(UnitAcceptance.accepts(new Predicate.And(List.of(spatial, attribute)), GEOM, INSIDE))
                .isFalse();
    }

    @Test
    void orAcceptsWhenAnyChildAccepts() {
        Predicate spatial = new Predicate.Spatial.BboxIntersects(GEOM, QUERY);
        Predicate attribute = Pred.col("height").gt(3);
        assertThat(UnitAcceptance.accepts(new Predicate.Or(List.of(attribute, spatial)), GEOM, INSIDE))
                .isTrue();
        assertThat(UnitAcceptance.accepts(new Predicate.Or(List.of(attribute, attribute)), GEOM, INSIDE))
                .isFalse();
    }

    @Test
    void geometryFilterAcceptsWhatItsRegionAnswerCovers() {
        Geometry square = square(0, 0, 10, 10);
        Predicate exact = Predicate.geometryFilter(JtsGeometryFilter.intersects(GEOM, square));
        assertThat(UnitAcceptance.accepts(exact, GEOM, INSIDE)).isTrue();
        assertThat(UnitAcceptance.accepts(exact, GEOM, STRADDLING)).isFalse();
        Predicate onOtherColumn = Predicate.geometryFilter(JtsGeometryFilter.intersects(OTHER, square));
        assertThat(UnitAcceptance.accepts(onOtherColumn, GEOM, INSIDE)).isFalse();
    }

    @Test
    void aBoxWrappingTheAntimeridianNeverAccepts() {
        Bbox wrapping = Bbox.of2d(170, 0, -170, 10);
        Predicate intersects = new Predicate.Spatial.BboxIntersects(GEOM, Bbox.of2d(-175, 0, 175, 10));
        assertThat(UnitAcceptance.accepts(intersects, GEOM, wrapping))
                .as("a row at longitude 178 misses the query box")
                .isFalse();
        assertThat(UnitAcceptance.accepts(Predicate.ALWAYS_TRUE, GEOM, wrapping))
                .as("no shape accepts a box without a real extent")
                .isFalse();
    }

    @Test
    void aNaNBoxNeverAccepts() {
        Bbox notANumber = Bbox.of2d(Double.NaN, 2, 4, 4);
        Predicate intersects = new Predicate.Spatial.BboxIntersects(GEOM, QUERY);
        assertThat(UnitAcceptance.accepts(intersects, GEOM, notANumber)).isFalse();
        Predicate exact = Predicate.geometryFilter(JtsGeometryFilter.intersects(GEOM, square(0, 0, 10, 10)));
        assertThat(UnitAcceptance.accepts(exact, GEOM, notANumber)).isFalse();
    }

    @ParameterizedTest
    @MethodSource("shapesThatNeverAccept")
    void otherShapesNeverAccept(Predicate predicate) {
        assertThat(UnitAcceptance.accepts(predicate, GEOM, INSIDE)).isFalse();
    }

    static Stream<Predicate> shapesThatNeverAccept() {
        Predicate spatial = new Predicate.Spatial.BboxIntersects(GEOM, QUERY);
        return Stream.of(
                Pred.col("height").eq(3),
                Pred.col("height").isNull(),
                new Predicate.In(ColumnPath.of("height"), List.of(new Value.IntVal(1))),
                new Predicate.Not(spatial));
    }

    private static Geometry square(double minX, double minY, double maxX, double maxY) {
        return new GeometryFactory().toGeometry(new Envelope(minX, maxX, minY, maxY));
    }
}
