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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.internal.filter.spatial.WkbEnvelope;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.Wkb;

class BboxCoveringDeriverTest {

    @Test
    void floatFloorNeverExceedsAndCeilNeverUndercutsTheDouble() {
        double[] samples = {0.0, 179.999_999_9, -179.999_999_9, 1.0 / 3.0, -1.0 / 3.0, 12.3456789};
        for (double d : samples) {
            assertThat((double) BboxCoveringDeriver.floatFloor(d)).isLessThanOrEqualTo(d);
            assertThat((double) BboxCoveringDeriver.floatCeil(d)).isGreaterThanOrEqualTo(d);
        }
    }

    static Stream<Arguments> polygons() {
        return Stream.of(
                Arguments.of(
                        "POLYGON ((1.111111 2.222222, 3.333333 2.222222, 3.333333 4.444444, 1.111111 4.444444, 1.111111 2.222222))"),
                Arguments.of(
                        "POLYGON ((-73.987654 40.123456, -73.900001 40.123456, -73.900001 40.222222, -73.987654 40.222222, -73.987654 40.123456))"));
    }

    @ParameterizedTest
    @MethodSource("polygons")
    void floatCoveringEnclosesTheExactDoubleEnvelope(String wkt) {
        BinaryVector geometry = singleGeometry(wkt);
        StructVector bbox = BboxCoveringDeriver.derive(geometry, 1, true);

        FloatVector xmin = (FloatVector) bbox.children().get(ColumnPath.of("xmin"));
        FloatVector xmax = (FloatVector) bbox.children().get(ColumnPath.of("xmax"));
        FloatVector ymin = (FloatVector) bbox.children().get(ColumnPath.of("ymin"));
        FloatVector ymax = (FloatVector) bbox.children().get(ColumnPath.of("ymax"));

        Bbox exact = WkbEnvelope.compute(geometry.get(0));
        assertThat((double) xmin.getFloat(0)).isLessThanOrEqualTo(exact.minX());
        assertThat((double) ymin.getFloat(0)).isLessThanOrEqualTo(exact.minY());
        assertThat((double) xmax.getFloat(0)).isGreaterThanOrEqualTo(exact.maxX());
        assertThat((double) ymax.getFloat(0)).isGreaterThanOrEqualTo(exact.maxY());
    }

    @Test
    void nullGeometryYieldsNullCovering() {
        BinaryVector geometry = BinaryVector.materialized(new MemorySegment[] {null}, Validity.of(new BitSet(1), 1));
        StructVector bbox = BboxCoveringDeriver.derive(geometry, 1, true);
        assertThat(bbox.children().get(ColumnPath.of("xmin")).isNull(0)).isTrue();
    }

    @Test
    void emptyGeometryYieldsNullCovering() {
        BinaryVector geometry = singleGeometry("POLYGON EMPTY");
        StructVector bbox = BboxCoveringDeriver.derive(geometry, 1, true);
        assertThat(bbox.children().get(ColumnPath.of("xmin")).isNull(0)).isTrue();
    }

    private static BinaryVector singleGeometry(String wkt) {
        return BinaryVector.materialized(new MemorySegment[] {Wkb.fromWkt(wkt)}, Validity.allValid(1));
    }
}
