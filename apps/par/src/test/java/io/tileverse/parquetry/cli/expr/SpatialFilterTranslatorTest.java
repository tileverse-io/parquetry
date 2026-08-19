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

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

class SpatialFilterTranslatorTest {

    private static final ColumnPath GEOMETRY_PATH = ColumnPath.of("geometry");

    private final ParquetSchema schema = Fixtures.geoCitiesSchema();
    private final Set<ColumnPath> geometryColumns = Set.of(GEOMETRY_PATH);

    private Predicate parse(String filter) {
        return FilterParser.parse(filter, schema, geometryColumns);
    }

    private GeometryFilter<?> filterOf(Predicate p) {
        return ((Predicate.GeometryFilterPredicate) p).filter();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "ST_Intersects(geometry, ST_GeomFromText('POLYGON((0 0,0 1,1 1,1 0,0 0))'))",
                "ST_Intersects(geometry, ST_MakeEnvelope(0, 0, 10, 10))",
                "ST_DWithin(geometry, ST_GeomFromText('POINT(0 0)'), 5)"
            })
    void stFunctionBecomesGeometryFilter(String filter) {
        Predicate p = parse(filter);
        assertThat(p).isInstanceOf(Predicate.GeometryFilterPredicate.class);
    }

    @Test
    void disjointHasNoPruning() {
        Predicate p = parse("ST_Disjoint(geometry, ST_MakeEnvelope(0, 0, 10, 10))");
        assertThat(filterOf(p).pruningPredicate()).isEmpty();
    }

    @Test
    void columnSecondInvertsContainsToWithin() {
        Predicate p = parse("ST_Contains(ST_MakeEnvelope(0, 0, 10, 10), geometry)");
        GeometryFilter<?> filter = filterOf(p);
        assertThat(filter.pruningPredicate()).isPresent();
        assertThat(filter.pruningPredicate().get()).isInstanceOf(Predicate.Spatial.BboxCoveredBy.class);
    }

    @Test
    void exactTestAcceptsInsidePolygonRejectsInsideBboxOutsidePolygon() {
        Predicate p = parse("ST_Intersects(geometry, ST_GeomFromText('POLYGON((0 0, 4 0, 0 4, 0 0))'))");
        @SuppressWarnings("unchecked")
        GeometryFilter<Geometry> filter = (GeometryFilter<Geometry>) filterOf(p);

        Geometry insidePolygon = filter.decode(Fixtures.wkbPointSegment(1.0, 1.0));
        assertThat(filter.matches(insidePolygon)).isTrue();

        Geometry insideBboxOutsidePolygon = filter.decode(Fixtures.wkbPointSegment(3.5, 3.5));
        assertThat(filter.matches(insideBboxOutsidePolygon)).isFalse();
    }

    @Test
    void unknownStFunctionRejected() {
        assertThatThrownBy(() -> parse("ST_Bogus(geometry, ST_GeomFromText('POINT(0 0)'))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("ST_");
    }

    @Test
    void stOnNonGeometryColumnRejected() {
        assertThatThrownBy(() -> parse("ST_Intersects(id, ST_GeomFromText('POINT(0 0)'))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("geometry column");
    }

    @Test
    void malformedWktRejected() {
        assertThatThrownBy(() -> parse("ST_Intersects(geometry, ST_GeomFromText('NOTWKT'))"))
                .isInstanceOf(FilterParseException.class);
    }

    @Test
    void scalarAndSpatialCombine() {
        Predicate p = parse("id > 0 AND ST_Intersects(geometry, ST_MakeEnvelope(0,0,10,10))");
        assertThat(p).isInstanceOf(Predicate.And.class);
    }

    static List<String> extentFunctions() {
        return SpatialFilterTranslator.extentFunctionNames();
    }

    /** The query box is deliberately wider than it is tall, which pins the envelope-to-bbox axis order. */
    @ParameterizedTest
    @MethodSource("extentFunctions")
    void extentColumnBecomesBboxIntersects(String extentFunction) {
        Predicate p = parse("ST_Intersects(" + extentFunction + "(geometry), ST_MakeEnvelope(0, 0, 10, 5))");
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxIntersects(GEOMETRY_PATH, Bbox.of2d(0, 0, 10, 5)));
    }

    @Test
    void extentIsIdempotent() {
        Predicate p = parse("ST_Intersects(ST_Extent(ST_Extent(geometry)), ST_MakeEnvelope(0, 0, 10, 10))");
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxIntersects(GEOMETRY_PATH, Bbox.of2d(0, 0, 10, 10)));
    }

    @Test
    void extentQueryReducesANonRectangleToItsBox() {
        Predicate p = parse(
                "ST_Intersects(ST_Extent(geometry)," + " ST_Extent(ST_GeomFromText('POLYGON((0 0, 4 0, 0 4, 0 0))')))");
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxIntersects(GEOMETRY_PATH, Bbox.of2d(0, 0, 4, 4)));
    }

    @Test
    void extentColumnSecondInvertsCovers() {
        Predicate p = parse("ST_Covers(ST_MakeEnvelope(0, 0, 10, 10), ST_Extent(geometry))");
        assertThat(p).isEqualTo(new Predicate.Spatial.BboxCoveredBy(GEOMETRY_PATH, Bbox.of2d(0, 0, 10, 10)));
    }

    @Test
    void disjointOnExtentsNegatesBboxIntersects() {
        Predicate p = parse("ST_Disjoint(ST_Extent(geometry), ST_MakeEnvelope(0, 0, 10, 10))");
        assertThat(p).isEqualTo(Pred.not(new Predicate.Spatial.BboxIntersects(GEOMETRY_PATH, Bbox.of2d(0, 0, 10, 10))));
    }

    @Test
    void extentOnTheQuerySideKeepsTheExactFilterAgainstTheQueryRectangle() {
        Predicate p = parse("ST_Intersects(geometry, ST_Extent(ST_GeomFromText('POLYGON((0 0, 4 0, 0 4, 0 0))')))");
        assertThat(p).isInstanceOf(Predicate.GeometryFilterPredicate.class);

        @SuppressWarnings("unchecked")
        GeometryFilter<Geometry> filter = (GeometryFilter<Geometry>) filterOf(p);
        Geometry outsideTriangleInsideItsBox = filter.decode(Fixtures.wkbPointSegment(3.0, 3.0));
        assertThat(filter.matches(outsideTriangleInsideItsBox)).isTrue();
    }

    @Test
    void nonRectangularQueryAgainstAnExtentColumnIsRejected() {
        assertThatThrownBy(() -> parse(
                        "ST_Intersects(ST_Extent(geometry)," + " ST_GeomFromText('POLYGON((0 0, 4 0, 0 4, 0 0))'))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("needs a rectangle on both sides")
                .hasMessageContaining("wrap the query in ST_Extent(...)")
                .hasMessageContaining("remove ST_Extent(geometry)");
    }

    @Test
    void extentOnANonGeometryColumnIsRejected() {
        assertThatThrownBy(() -> parse("ST_Intersects(ST_Extent(id), ST_MakeEnvelope(0, 0, 10, 10))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("ST_Extent takes a geometry column, got: id")
                .hasMessageContaining("the bbox covering columns are used automatically");
    }

    @Test
    void emptyQueryGeometryIsRejectedOnTheBoxPath() {
        assertThatThrownBy(
                        () -> parse("ST_Intersects(ST_Extent(geometry), ST_Extent(ST_GeomFromText('POLYGON EMPTY')))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("the query geometry is empty");
    }

    @Test
    void withinOnAnExtentColumnIsRejected() {
        assertThatThrownBy(() -> parse("ST_Within(ST_Extent(geometry), ST_MakeEnvelope(0, 0, 10, 10))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("ST_Within has no bounding-box form")
                .hasMessageContaining("use ST_CoveredBy");
    }

    @Test
    void aTopLevelExtentCallIsNotAPredicate() {
        assertThatThrownBy(() -> parse("ST_Extent(geometry)"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("unsupported ST_* function: ST_Extent");
    }

    @Test
    void extentWithWrongArityIsRejected() {
        assertThatThrownBy(() -> parse("ST_Intersects(ST_Extent(geometry, 1), ST_MakeEnvelope(0, 0, 10, 10))"))
                .isInstanceOf(FilterParseException.class)
                .hasMessageContaining("expects 1 argument(s), got 2");
    }
}
