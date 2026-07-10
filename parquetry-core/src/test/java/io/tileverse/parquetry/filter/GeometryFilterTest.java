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
package io.tileverse.parquetry.filter;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class GeometryFilterTest {

    private static final ValueLayout.OfDouble DOUBLE_LE =
            ValueLayout.JAVA_DOUBLE_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    /** Allocates a 16-byte segment encoding {x, y} as two little-endian doubles. */
    private static MemorySegment point(double x, double y) {
        MemorySegment seg = MemorySegment.ofArray(new byte[16]);
        seg.setAtIndex(DOUBLE_LE, 0, x);
        seg.setAtIndex(DOUBLE_LE, 1, y);
        return seg;
    }

    /** A fake filter that decodes a 2-double LE segment and matches when the point is inside [0,1] x [0,1]. */
    private static GeometryFilter<double[]> insideUnitBox() {
        return new GeometryFilter<>() {
            public ColumnPath column() {
                return ColumnPath.of("geometry");
            }

            public Optional<Predicate.Spatial> pruningPredicate() {
                return Optional.of(
                        new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), Bbox.of2d(0, 0, 1, 1)));
            }

            public double[] decode(MemorySegment wkb) {
                double x = wkb.getAtIndex(DOUBLE_LE, 0);
                double y = wkb.getAtIndex(DOUBLE_LE, 1);
                return new double[] {x, y};
            }

            public boolean matches(double[] p) {
                return p[0] >= 0 && p[0] <= 1 && p[1] >= 0 && p[1] <= 1;
            }
        };
    }

    @Test
    void gateReturnsDecodedGeometryWhenPointIsInsideBox() {
        GeometryFilter<double[]> filter = insideUnitBox();
        MemorySegment wkb = point(0.5, 0.5);

        Optional<Object> result = filter.gate(wkb);

        assertThat(result)
                .as("gate() on an inside point should return a non-empty Optional")
                .isPresent();
        double[] decoded = (double[]) result.get();
        assertThat(decoded[0]).as("decoded x").isEqualTo(0.5);
        assertThat(decoded[1]).as("decoded y").isEqualTo(0.5);
    }

    @Test
    void gateReturnsEmptyWhenPointIsOutsideBox() {
        GeometryFilter<double[]> filter = insideUnitBox();
        MemorySegment wkb = point(2.0, 2.0);

        Optional<Object> result = filter.gate(wkb);

        assertThat(result).as("gate() on an outside point should return empty").isEmpty();
    }

    @Test
    void gateRetainsPointOnBoundaryOfBox() {
        GeometryFilter<double[]> filter = insideUnitBox();
        MemorySegment wkb = point(0.0, 1.0);

        Optional<Object> result = filter.gate(wkb);

        assertThat(result)
                .as("gate() on a boundary point should return a non-empty Optional (edges inclusive)")
                .isPresent();
    }

    @Test
    void predicateFactoryProducesGeometryFilterPredicate() {
        GeometryFilter<double[]> filter = insideUnitBox();

        Predicate predicate = Predicate.geometryFilter(filter);

        assertThat(predicate)
                .as("geometryFilter() should produce a GeometryFilterPredicate")
                .isInstanceOf(Predicate.GeometryFilterPredicate.class);
        Predicate.GeometryFilterPredicate gfp = (Predicate.GeometryFilterPredicate) predicate;
        assertThat(gfp.filter().column())
                .as("GeometryFilterPredicate.filter().column() should be the geometry column")
                .isEqualTo(ColumnPath.of("geometry"));
    }

    @Test
    void predicateColumnsReportsTheGeometryColumn() {
        Predicate predicate = Predicate.geometryFilter(insideUnitBox());

        assertThat(Predicate.columns(predicate))
                .as("the read path must decode the filter's geometry column")
                .containsExactly(ColumnPath.of("geometry"));
    }
}
