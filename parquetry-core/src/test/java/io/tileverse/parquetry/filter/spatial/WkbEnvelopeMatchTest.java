/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.filter.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.Wkb;

class WkbEnvelopeMatchTest {

    @Test
    void intersectsTrueForOverlapFalseForDisjoint() {
        String wkt = "POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))"; // square [0,5]x[0,5]
        MemorySegment poly = Wkb.fromWkt(wkt);

        assertThat(WkbEnvelope.matches(intersects(4, 4, 9, 9), poly)).isTrue();
        assertThat(WkbEnvelope.matches(intersects(6, 6, 9, 9), poly)).isFalse();

        // Cross-check our 2D relation against JTS's reference envelope semantics.
        Envelope reference = Wkb.geometry(wkt).getEnvelopeInternal();
        assertThat(reference.intersects(new Envelope(4, 9, 4, 9))).isTrue();
        assertThat(reference.intersects(new Envelope(6, 9, 6, 9))).isFalse();
    }

    @Test
    void intersectsTrueWhenOnlyTheLastVertexReachesTheQuery() {
        // vertices walk away from q then the final vertex enters it - proves no premature false
        MemorySegment poly = Wkb.fromWkt("POLYGON ((0 0, 1 0, 1 1, 8 8, 0 0))");
        assertThat(WkbEnvelope.matches(intersects(7, 7, 9, 9), poly)).isTrue();
    }

    @Test
    void containsTrueWhenGeometryBboxEnclosesQuery() {
        String wkt = "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))";
        MemorySegment poly = Wkb.fromWkt(wkt);

        assertThat(WkbEnvelope.matches(contains(5, 5, 9, 9), poly)).isTrue();
        assertThat(WkbEnvelope.matches(contains(5, 5, 25, 9), poly)).isFalse();

        // Cross-check: JTS Envelope#covers is the inclusive-edge "contains" semantic we use.
        Envelope reference = Wkb.geometry(wkt).getEnvelopeInternal();
        assertThat(reference.covers(new Envelope(5, 9, 5, 9))).isTrue();
        assertThat(reference.covers(new Envelope(5, 25, 5, 9))).isFalse();
    }

    @Test
    void containsTrueWhenEnclosureCompletesOnlyAtTheLastVertex() {
        // the query box is enclosed only once the final, far-corner vertex widens the running envelope
        MemorySegment poly = Wkb.fromWkt("POLYGON ((0 0, 1 0, 1 1, 30 30, 0 0))");
        assertThat(WkbEnvelope.matches(contains(5, 5, 25, 25), poly)).isTrue();
    }

    @Test
    void coveredByFalseWhenAnyVertexEscapesQuery() {
        MemorySegment big = Wkb.fromWkt("POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))");
        MemorySegment small = Wkb.fromWkt("POLYGON ((1 1, 2 1, 2 2, 1 2, 1 1))");

        assertThat(WkbEnvelope.matches(coveredBy(0, 0, 10, 10), big)).isFalse();
        assertThat(WkbEnvelope.matches(coveredBy(0, 0, 10, 10), small)).isTrue();
    }

    @Test
    void coveredByFalseWhenOnlyTheLastVertexEscapesQuery() {
        // every vertex but the last lies inside q; the final vertex escapes - proves no premature true
        MemorySegment poly = Wkb.fromWkt("POLYGON ((1 1, 2 2, 3 3, 99 99, 1 1))");
        assertThat(WkbEnvelope.matches(coveredBy(0, 0, 10, 10), poly)).isFalse();
    }

    @Test
    void emptyGeometriesFailEveryRelation() {
        ColumnPath g = ColumnPath.of("g");
        Bbox q = Bbox.of2d(0, 0, 10, 10);
        List<String> emptyForms =
                List.of("POINT EMPTY", "LINESTRING EMPTY", "POLYGON EMPTY", "GEOMETRYCOLLECTION EMPTY");
        for (String wkt : emptyForms) {
            MemorySegment wkb = Wkb.fromWkt(wkt);
            assertThat(WkbEnvelope.matches(new Predicate.Spatial.BboxIntersects(g, q), wkb))
                    .as(wkt + " intersects")
                    .isFalse();
            assertThat(WkbEnvelope.matches(new Predicate.Spatial.BboxContains(g, q), wkb))
                    .as(wkt + " contains")
                    .isFalse();
            assertThat(WkbEnvelope.matches(new Predicate.Spatial.BboxCoveredBy(g, q), wkb))
                    .as(wkt + " coveredBy")
                    .isFalse();
            assertThat(WkbEnvelope.matches(new Predicate.Spatial.BboxEquals(g, q), wkb))
                    .as(wkt + " equals")
                    .isFalse();
        }
    }

    @Test
    void equalsTrueOnlyWhenEdgesMatch() {
        MemorySegment poly = Wkb.fromWkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        assertThat(WkbEnvelope.matches(equals(0, 0, 10, 10), poly)).isTrue();
        assertThat(WkbEnvelope.matches(equals(0, 0, 10, 9), poly)).isFalse();
    }

    private static Predicate.Spatial intersects(double minX, double minY, double maxX, double maxY) {
        return new Predicate.Spatial.BboxIntersects(ColumnPath.of("g"), Bbox.of2d(minX, minY, maxX, maxY));
    }

    private static Predicate.Spatial contains(double minX, double minY, double maxX, double maxY) {
        return new Predicate.Spatial.BboxContains(ColumnPath.of("g"), Bbox.of2d(minX, minY, maxX, maxY));
    }

    private static Predicate.Spatial coveredBy(double minX, double minY, double maxX, double maxY) {
        return new Predicate.Spatial.BboxCoveredBy(ColumnPath.of("g"), Bbox.of2d(minX, minY, maxX, maxY));
    }

    private static Predicate.Spatial equals(double minX, double minY, double maxX, double maxY) {
        return new Predicate.Spatial.BboxEquals(ColumnPath.of("g"), Bbox.of2d(minX, minY, maxX, maxY));
    }
}
