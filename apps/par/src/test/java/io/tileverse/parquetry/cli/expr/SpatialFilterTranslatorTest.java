/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.cli.support.Fixtures;
import io.tileverse.parquetry.filter.GeometryFilter;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

class SpatialFilterTranslatorTest {

    private final ParquetSchema schema = Fixtures.geoCitiesSchema();
    private final Set<ColumnPath> geometryColumns = Set.of(ColumnPath.of("geometry"));

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
}
