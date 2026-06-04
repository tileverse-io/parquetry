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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.filter.Pred.col;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.Wkb;

class RecordLevelEvaluatorTest {

    @Test
    void eqMatchingValueIsTrue() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020), row)).isTrue();
    }

    @Test
    void eqMismatchingValueIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2019), row)).isFalse();
    }

    @Test
    void ltAndGtChain() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(
                        col("year").gt(2019).and(col("year").lt(2025)), row))
                .isTrue();
    }

    @Test
    void orShortCircuits() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020).or(col("year").eq(2021)), row))
                .isTrue();
    }

    @Test
    void inMatchesIfAnyValuePresent() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2021));
        assertThat(RecordLevelEvaluator.test(col("year").inInts(2020, 2021, 2022), row))
                .isTrue();
    }

    @Test
    void inFailsIfNoValueMatches() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2025));
        assertThat(RecordLevelEvaluator.test(col("year").inInts(2020, 2021, 2022), row))
                .isFalse();
    }

    @Test
    void isNullOnAbsentColumnIsTrue() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of()); // empty
        assertThat(RecordLevelEvaluator.test(col("year").isNull(), row)).isTrue();
    }

    @Test
    void isNotNullOnPresentColumnIsTrue() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").isNotNull(), row)).isTrue();
    }

    @Test
    void valueComparisonAgainstNullIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of()); // year missing
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020), row)).isFalse();
        assertThat(RecordLevelEvaluator.test(col("year").gt(2019), row)).isFalse();
    }

    @Test
    void notEqAgainstNullIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of());
        assertThat(RecordLevelEvaluator.test(col("year").notEq(2020), row)).isFalse();
    }

    @Test
    void stringEqMatches() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("country", "AR"));
        assertThat(RecordLevelEvaluator.test(col("country").eq("AR"), row)).isTrue();
    }

    @Test
    void doubleLtMatches() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("price", 1.5));
        assertThat(RecordLevelEvaluator.test(col("price").lt(2.0), row)).isTrue();
    }

    @Test
    void alwaysTrueIsTrue() {
        assertThat(RecordLevelEvaluator.test(Predicate.ALWAYS_TRUE, row(Map.of())))
                .isTrue();
    }

    @Test
    void alwaysFalseIsFalse() {
        assertThat(RecordLevelEvaluator.test(Predicate.ALWAYS_FALSE, row(Map.of())))
                .isFalse();
    }

    @Test
    void andWithFailingChildIsFalse() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(
                        col("year").eq(2020).and(col("year").eq(2021)), row))
                .isFalse();
    }

    @Test
    void notInverts() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("year", 2020));
        assertThat(RecordLevelEvaluator.test(col("year").eq(2020).negate(), row))
                .isFalse();
        assertThat(RecordLevelEvaluator.test(col("year").eq(2019).negate(), row))
                .isTrue();
    }

    @Test
    void bboxIntersectsEvaluatesWkbGeometryPerRow() {
        // A point at (5, 5) intersects the query box [0,0 - 10,10].
        MemorySegment hitGeom = wkbPoint(5.0, 5.0);
        RecordLevelEvaluator.RecordAccessor hit = row(Map.of("geom", hitGeom));
        assertThat(RecordLevelEvaluator.test(col("geom").intersects(Bbox.of2d(0, 0, 10, 10)), hit))
                .as("point inside query box should intersect")
                .isTrue();

        // A point at (20, 20) is outside [0,0 - 10,10].
        MemorySegment missGeom = wkbPoint(20.0, 20.0);
        RecordLevelEvaluator.RecordAccessor miss = row(Map.of("geom", missGeom));
        assertThat(RecordLevelEvaluator.test(col("geom").intersects(Bbox.of2d(0, 0, 10, 10)), miss))
                .as("point outside query box should not intersect")
                .isFalse();
    }

    @Test
    void nullGeometryFailsAnySpatialRelation() {
        // A row with no geometry value (null) must be excluded by every spatial predicate.
        RecordLevelEvaluator.RecordAccessor rowWithNoGeom = row(Map.of());
        assertThat(RecordLevelEvaluator.test(col("geom").intersects(Bbox.of2d(0, 0, 10, 10)), rowWithNoGeom))
                .as("null geometry must not pass a spatial predicate")
                .isFalse();
    }

    @Test
    void nullGeometryAlsoFailsNegatedSpatialPredicate() {
        // OGC simple-feature semantics: a null geometry yields UNKNOWN, which a NOT cannot turn into a match.
        // The row is excluded from both a spatial predicate and its negation.
        RecordLevelEvaluator.RecordAccessor rowWithNoGeom = row(Map.of());
        Predicate notIntersects = new Predicate.Not(col("geom").intersects(Bbox.of2d(0, 0, 10, 10)));
        assertThat(RecordLevelEvaluator.test(notIntersects, rowWithNoGeom))
                .as("null geometry must not pass a negated spatial predicate")
                .isFalse();
    }

    @Test
    void negatedSpatialInvertsForPresentGeometry() {
        Predicate notIntersects = new Predicate.Not(col("geom").intersects(Bbox.of2d(0, 0, 10, 10)));
        RecordLevelEvaluator.RecordAccessor inside = row(Map.of("geom", wkbPoint(5.0, 5.0)));
        assertThat(RecordLevelEvaluator.test(notIntersects, inside))
                .as("a geometry inside the query box intersects; its negation is false")
                .isFalse();
        RecordLevelEvaluator.RecordAccessor outside = row(Map.of("geom", wkbPoint(20.0, 20.0)));
        assertThat(RecordLevelEvaluator.test(notIntersects, outside))
                .as("a geometry outside the query box does not intersect; its negation is true")
                .isTrue();
    }

    @Test
    void emptyGeometryIsDroppedByASpatialPredicate() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("geom", Wkb.fromWkt("POLYGON EMPTY")));
        assertThat(RecordLevelEvaluator.test(col("geom").bboxCoveredBy(Bbox.of2d(0, 0, 10, 10)), row))
                .as("an empty geometry is not covered by anything")
                .isFalse();
        assertThat(RecordLevelEvaluator.test(col("geom").intersects(Bbox.of2d(0, 0, 10, 10)), row))
                .as("an empty geometry intersects nothing")
                .isFalse();
    }

    @Test
    void emptyGeometryIsKeptByANegatedSpatialPredicate() {
        // OGC/JTS: an empty geometry's predicate is a definite false; its negation is therefore true (kept),
        // unlike a null geometry, which is excluded from both.
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("geom", Wkb.fromWkt("POLYGON EMPTY")));
        Predicate notCoveredBy = new Predicate.Not(col("geom").bboxCoveredBy(Bbox.of2d(0, 0, 10, 10)));
        assertThat(RecordLevelEvaluator.test(notCoveredBy, row))
                .as("the negation of a definite-false relation is true")
                .isTrue();
    }

    @Test
    void geometryFilterPassesRowWhosePointIsInsideBox() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("geom", wkbPoint(5.0, 5.0)));
        assertThat(RecordLevelEvaluator.test(Predicate.geometryFilter(pointInBox(0, 0, 10, 10)), row))
                .as("a point inside the query box must pass the geometry filter")
                .isTrue();
    }

    @Test
    void geometryFilterRejectsRowWhosePointIsOutsideBox() {
        RecordLevelEvaluator.RecordAccessor row = row(Map.of("geom", wkbPoint(20.0, 20.0)));
        assertThat(RecordLevelEvaluator.test(Predicate.geometryFilter(pointInBox(0, 0, 10, 10)), row))
                .as("a point outside the query box must fail the geometry filter")
                .isFalse();
    }

    @Test
    void nullGeometryFailsGeometryFilter() {
        RecordLevelEvaluator.RecordAccessor rowWithNoGeom = row(Map.of());
        assertThat(RecordLevelEvaluator.test(Predicate.geometryFilter(pointInBox(0, 0, 10, 10)), rowWithNoGeom))
                .as("a null geometry must not pass a geometry filter (OGC semantics)")
                .isFalse();
    }

    @Test
    void nullGeometryFailsNegatedGeometryFilter() {
        // OGC simple-feature semantics: a null geometry has no spatial truth value; it is excluded
        // from both the filter and its negation - symmetrical with negated spatial predicates.
        RecordLevelEvaluator.RecordAccessor rowWithNoGeom = row(Map.of());
        Predicate notMatch = new Predicate.Not(Predicate.geometryFilter(pointInBox(0, 0, 10, 10)));
        assertThat(RecordLevelEvaluator.test(notMatch, rowWithNoGeom))
                .as("a null geometry must not pass a negated geometry filter either (OGC semantics)")
                .isFalse();
    }

    @Test
    void negatedGeometryFilterIsInverseForPresentGeometry() {
        GeometryFilter<double[]> filter = pointInBox(0, 0, 10, 10);
        Predicate notMatch = new Predicate.Not(Predicate.geometryFilter(filter));
        RecordLevelEvaluator.RecordAccessor inside = row(Map.of("geom", wkbPoint(5.0, 5.0)));
        assertThat(RecordLevelEvaluator.test(notMatch, inside))
                .as("a geometry inside the box passes the filter; its negation is false")
                .isFalse();
        RecordLevelEvaluator.RecordAccessor outside = row(Map.of("geom", wkbPoint(20.0, 20.0)));
        assertThat(RecordLevelEvaluator.test(notMatch, outside))
                .as("a geometry outside the box fails the filter; its negation is true")
                .isTrue();
    }

    /**
     * A geometry filter whose decode reads the WKB point format written by {@link #wkbPoint}: 1 byte byte-order +
     * 4-byte type int + two little-endian doubles. Matches when the point is inside [minX, maxX] x [minY, maxY].
     */
    private static GeometryFilter<double[]> pointInBox(double minX, double minY, double maxX, double maxY) {
        return new GeometryFilter<>() {
            @Override
            public ColumnPath column() {
                return ColumnPath.of("geom");
            }

            @Override
            public Optional<Predicate.Spatial> pruningPredicate() {
                return Optional.empty();
            }

            @Override
            public double[] decode(MemorySegment wkb) {
                // WKB point layout: 1-byte order + 4-byte type + 8-byte x + 8-byte y
                ValueLayout.OfDouble doubleLE = ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
                double x = wkb.get(doubleLE, 5);
                double y = wkb.get(doubleLE, 13);
                return new double[] {x, y};
            }

            @Override
            public boolean matches(double[] point) {
                return point[0] >= minX && point[0] <= maxX && point[1] >= minY && point[1] <= maxY;
            }
        };
    }

    private static MemorySegment wkbPoint(double x, double y) {
        ByteBuffer buf = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buf.put((byte) 0x01);
        buf.putInt(1); // point type
        buf.putDouble(x);
        buf.putDouble(y);
        return MemorySegment.ofArray(buf.array()).asReadOnly();
    }

    private static RecordLevelEvaluator.RecordAccessor row(Map<String, Object> values) {
        Map<ColumnPath, Object> byPath = new HashMap<>();
        values.forEach((k, v) -> byPath.put(ColumnPath.of(k), v));
        return byPath::get;
    }
}
