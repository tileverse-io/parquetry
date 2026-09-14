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

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateSequenceFilter;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.internal.write.GeospatialStatisticsAccumulator;
import io.tileverse.parquetry.testsupport.WkbCorpus;

/**
 * The read-side envelope and the write-side statistics against the JTS oracle, over the whole WKB corpus: both byte
 * orders, ISO and EWKB, Z/M/ZM, SRIDs, the empties and nested collections. A structural walk that misreads a header, a
 * count or an ordinate in any of those shapes shows up here.
 */
class WkbEnvelopeParityTest {

    static Stream<Arguments> corpus() {
        return WkbCorpus.entries().stream().map(entry -> Arguments.of(entry.label(), entry));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void envelopeEqualsTheJtsEnvelope(String label, WkbCorpus.Entry entry) {
        Envelope expected = entry.geometry().getEnvelopeInternal();

        Bbox actual = WkbEnvelope.compute(entry.wkb());

        if (expected.isNull()) {
            boolean inverted = !(actual.minX() <= actual.maxX() && actual.minY() <= actual.maxY());
            assertThat(inverted)
                    .as("%s: an empty geometry yields an inverted box", label)
                    .isTrue();
            return;
        }
        assertThat(actual.minX()).as("%s minX", label).isEqualTo(expected.getMinX());
        assertThat(actual.minY()).as("%s minY", label).isEqualTo(expected.getMinY());
        assertThat(actual.maxX()).as("%s maxX", label).isEqualTo(expected.getMaxX());
        assertThat(actual.maxY()).as("%s maxY", label).isEqualTo(expected.getMaxY());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void statisticsEqualAJtsCoordinateScan(String label, WkbCorpus.Entry entry) {
        GeospatialStatisticsAccumulator accumulator = new GeospatialStatisticsAccumulator();
        accumulator.update(entry.wkb());
        GeospatialStatistics stats = accumulator.finish();

        Extents expected = Extents.scan(entry.geometry(), entry.hasZ(), entry.hasM());
        if (expected.empty()) {
            assertThat(stats.bbox()).as("%s: no real vertex, no bbox", label).isEmpty();
        } else {
            BoundingBox bbox = stats.bbox().orElseThrow();
            assertThat(bbox.xmin()).as("%s xmin", label).isEqualTo(expected.xmin());
            assertThat(bbox.xmax()).as("%s xmax", label).isEqualTo(expected.xmax());
            assertThat(bbox.ymin()).as("%s ymin", label).isEqualTo(expected.ymin());
            assertThat(bbox.ymax()).as("%s ymax", label).isEqualTo(expected.ymax());
            assertThat(bbox.zmin()).as("%s zmin", label).isEqualTo(expected.zmin());
            assertThat(bbox.zmax()).as("%s zmax", label).isEqualTo(expected.zmax());
            assertThat(bbox.mmin()).as("%s mmin", label).isEqualTo(expected.mmin());
            assertThat(bbox.mmax()).as("%s mmax", label).isEqualTo(expected.mmax());
        }
        assertThat(stats.geospatialTypes())
                .as("%s types", label)
                .hasValueSatisfying(types -> assertThat(types).containsExactlyElementsOf(isoTypeCodes(entry)));
    }

    /** The ISO type code of every geometry node, the top one and every collection member, in ascending order. */
    private static List<Integer> isoTypeCodes(WkbCorpus.Entry entry) {
        Set<Integer> codes = new TreeSet<>();
        collectTypeCodes(entry.geometry(), entry.hasZ(), entry.hasM(), codes);
        return new ArrayList<>(codes);
    }

    private static void collectTypeCodes(Geometry geometry, boolean hasZ, boolean hasM, Set<Integer> out) {
        int base =
                switch (geometry) {
                    case Point _ -> 1;
                    case LineString _ -> 2;
                    case Polygon _ -> 3;
                    case MultiPoint _ -> 4;
                    case MultiLineString _ -> 5;
                    case MultiPolygon _ -> 6;
                    default -> 7;
                };
        int offset = hasZ && hasM ? 3000 : (hasZ ? 1000 : (hasM ? 2000 : 0));
        out.add(base + offset);
        if (geometry instanceof GeometryCollection collection) {
            for (int i = 0; i < collection.getNumGeometries(); i++) {
                collectTypeCodes(collection.getGeometryN(i), hasZ, hasM, out);
            }
        }
    }

    /** The extents of every finite coordinate of a JTS geometry, computed independently of parquetry's walk. */
    private record Extents(
            boolean empty,
            double xmin,
            double ymin,
            double xmax,
            double ymax,
            OptionalDouble zmin,
            OptionalDouble zmax,
            OptionalDouble mmin,
            OptionalDouble mmax) {

        static Extents scan(Geometry geometry, boolean hasZ, boolean hasM) {
            double[] xy = {
                Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY
            };
            double[] z = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
            double[] m = {Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
            boolean[] seen = {false};
            geometry.apply(new CoordinateSequenceFilter() {
                @Override
                public void filter(CoordinateSequence seq, int i) {
                    double x = seq.getX(i);
                    double y = seq.getY(i);
                    if (Double.isNaN(x) || Double.isNaN(y)) {
                        return;
                    }
                    seen[0] = true;
                    xy[0] = Math.min(xy[0], x);
                    xy[1] = Math.min(xy[1], y);
                    xy[2] = Math.max(xy[2], x);
                    xy[3] = Math.max(xy[3], y);
                    if (hasZ && !Double.isNaN(seq.getZ(i))) {
                        z[0] = Math.min(z[0], seq.getZ(i));
                        z[1] = Math.max(z[1], seq.getZ(i));
                    }
                    if (hasM && !Double.isNaN(seq.getM(i))) {
                        m[0] = Math.min(m[0], seq.getM(i));
                        m[1] = Math.max(m[1], seq.getM(i));
                    }
                }

                @Override
                public boolean isDone() {
                    return false;
                }

                @Override
                public boolean isGeometryChanged() {
                    return false;
                }
            });
            return new Extents(
                    !seen[0], xy[0], xy[1], xy[2], xy[3], extent(z[0]), extent(z[1]), extent(m[0]), extent(m[1]));
        }

        private static OptionalDouble extent(double value) {
            return Double.isInfinite(value) ? OptionalDouble.empty() : OptionalDouble.of(value);
        }
    }
}
