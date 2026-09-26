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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

/** Pins the geometry family taken by a substitute from a column's declared GeoParquet geometry types. */
class SubstituteShapesTest {

    @Test
    void aSingleDeclaredTypeIsItsOwnFamily() {
        assertThat(SubstituteShapes.familyOf(List.of("Point"))).isEqualTo(Point.class);
        assertThat(SubstituteShapes.familyOf(List.of("MultiPoint"))).isEqualTo(MultiPoint.class);
        assertThat(SubstituteShapes.familyOf(List.of("LineString"))).isEqualTo(LineString.class);
        assertThat(SubstituteShapes.familyOf(List.of("MultiLineString"))).isEqualTo(MultiLineString.class);
        assertThat(SubstituteShapes.familyOf(List.of("Polygon"))).isEqualTo(Polygon.class);
        assertThat(SubstituteShapes.familyOf(List.of("MultiPolygon"))).isEqualTo(MultiPolygon.class);
    }

    @Test
    void aMixOfSingleAndMultiWithinOneFamilyTakesTheSingleType() {
        assertThat(SubstituteShapes.familyOf(List.of("Polygon", "MultiPolygon")))
                .isEqualTo(Polygon.class);
        assertThat(SubstituteShapes.familyOf(List.of("MultiLineString", "LineString")))
                .isEqualTo(LineString.class);
        assertThat(SubstituteShapes.familyOf(List.of("Point", "MultiPoint"))).isEqualTo(Point.class);
    }

    @Test
    void dimensionSuffixesAreIgnored() {
        assertThat(SubstituteShapes.familyOf(List.of("Polygon Z", "MultiPolygon ZM")))
                .isEqualTo(Polygon.class);
        assertThat(SubstituteShapes.familyOf(List.of("Point M"))).isEqualTo(Point.class);
    }

    @Test
    void mixedFamiliesUnknownOrUndeclaredTypesFallBackToAPolygon() {
        assertThat(SubstituteShapes.familyOf(List.of("Point", "Polygon"))).isEqualTo(Polygon.class);
        assertThat(SubstituteShapes.familyOf(List.of("GeometryCollection"))).isEqualTo(Polygon.class);
        assertThat(SubstituteShapes.familyOf(List.of())).isEqualTo(Polygon.class);
    }
}
