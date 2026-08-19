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
package io.tileverse.parquetry.cli.expr;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.cli.expr.BboxRelations.BoxCall;
import io.tileverse.parquetry.cli.expr.SpatialFilterTranslator.Relation;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;

class BboxRelationsTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final Bbox BOX = Bbox.of2d(0, 0, 10, 10);

    private BoxCall columnFirst() {
        return new BoxCall(GEOMETRY, BOX, true, 0.0, "ST_Extent(geometry)");
    }

    private BoxCall columnSecond() {
        return new BoxCall(GEOMETRY, BOX, false, 0.0, "ST_Extent(geometry)");
    }

    @Test
    void intersectsBecomesBboxIntersects() {
        Predicate p = BboxRelations.translate(Relation.ST_INTERSECTS, columnFirst());
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxIntersects(GEOMETRY, BOX));
    }

    @Test
    void equalsBecomesBboxEquals() {
        Predicate p = BboxRelations.translate(Relation.ST_EQUALS, columnFirst());
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxEquals(GEOMETRY, BOX));
    }

    @Test
    void disjointNegatesBboxIntersects() {
        Predicate p = BboxRelations.translate(Relation.ST_DISJOINT, columnFirst());
        assertThat(p).isEqualTo(new Predicate.Not(new Predicate.Spatial.BboxIntersects(GEOMETRY, BOX)));
    }

    @Test
    void coversBecomesBboxContains() {
        Predicate p = BboxRelations.translate(Relation.ST_COVERS, columnFirst());
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxContains(GEOMETRY, BOX));
    }

    @Test
    void coveredByBecomesBboxCoveredBy() {
        Predicate p = BboxRelations.translate(Relation.ST_COVERED_BY, columnFirst());
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxCoveredBy(GEOMETRY, BOX));
    }

    @Test
    void columnSecondInvertsTheDirectionalPair() {
        assertThat(BboxRelations.translate(Relation.ST_COVERS, columnSecond()))
                .isEqualTo(new Predicate.Spatial.BboxCoveredBy(GEOMETRY, BOX));
        assertThat(BboxRelations.translate(Relation.ST_COVERED_BY, columnSecond()))
                .isEqualTo(new Predicate.Spatial.BboxContains(GEOMETRY, BOX));
    }

    @Test
    void symmetricRelationsIgnoreArgumentOrder() {
        assertThat(BboxRelations.translate(Relation.ST_INTERSECTS, columnSecond()))
                .isEqualTo(new Predicate.Spatial.BboxIntersects(GEOMETRY, BOX));
        assertThat(BboxRelations.translate(Relation.ST_EQUALS, columnSecond()))
                .isEqualTo(new Predicate.Spatial.BboxEquals(GEOMETRY, BOX));
    }

    @Test
    void withinIsRejectedAndPointsAtCoveredBy() {
        BoxCall call = columnFirst();
        assertThatThrownBy(() -> BboxRelations.translate(Relation.ST_WITHIN, call))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("ST_Within has no bounding-box form")
                .hasMessageContaining("use ST_CoveredBy")
                .hasMessageContaining("remove ST_Extent(geometry)");
    }

    @Test
    void containsIsRejectedAndPointsAtCovers() {
        BoxCall call = columnFirst();
        assertThatThrownBy(() -> BboxRelations.translate(Relation.ST_CONTAINS, call))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("ST_Contains has no bounding-box form")
                .hasMessageContaining("use ST_Covers");
    }

    /** The rectangle relations the bbox predicate family has no record for. */
    static Stream<Relation> relationsOutsideTheFamily() {
        return Stream.of(Relation.ST_TOUCHES, Relation.ST_CROSSES, Relation.ST_OVERLAPS);
    }

    @ParameterizedTest
    @MethodSource("relationsOutsideTheFamily")
    void relationOutsideTheFamilyIsRejected(Relation relation) {
        BoxCall call = columnFirst();
        assertThatThrownBy(() -> BboxRelations.translate(relation, call))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("the bounding-box predicates have no relation for " + relation.sqlName())
                .hasMessageContaining("remove ST_Extent(geometry) to test the exact geometry");
    }

    @Test
    void dwithinIsRejectedAndQuotesTheDistance() {
        BoxCall call = new BoxCall(GEOMETRY, BOX, true, 500.0, "ST_Extent(geometry)");
        assertThatThrownBy(() -> BboxRelations.translate(Relation.ST_DWITHIN, call))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("ST_DWithin has no exact bounding-box form")
                .hasMessageContaining("500.0");
    }
}
