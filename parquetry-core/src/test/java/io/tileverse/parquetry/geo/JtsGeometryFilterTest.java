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
package io.tileverse.parquetry.geo;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate.Spatial;
import io.tileverse.parquetry.schema.ColumnPath;

class JtsGeometryFilterTest {

    private static final ColumnPath COL = ColumnPath.of("geometry");

    // --- helpers ---

    private static Geometry wkt(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid WKT: " + wkt, e);
        }
    }

    private static MemorySegment toWkb(Geometry geometry) {
        byte[] bytes = new WKBWriter().write(geometry);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    // --- intersects ---

    @Test
    void intersects_candidateInsideQueryPolygon_gatePresent() {
        Geometry query = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        Geometry inside = wkt("POINT (5 5)");

        JtsGeometryFilter filter = JtsGeometryFilter.intersects(COL, query);

        assertThat(filter.gate(toWkb(inside)))
                .as("a point inside the query polygon should be retained by intersects")
                .isPresent();
    }

    @Test
    void intersects_candidateOutsideQueryPolygon_gateEmpty() {
        Geometry query = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        Geometry outside = wkt("POINT (20 20)");

        JtsGeometryFilter filter = JtsGeometryFilter.intersects(COL, query);

        assertThat(filter.gate(toWkb(outside)))
                .as("a point outside the query polygon should be dropped by intersects")
                .isEmpty();
    }

    @Test
    void intersects_pruningPredicateIsBboxIntersects() {
        Geometry query = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.intersects(COL, query);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("intersects should provide a BboxIntersects pruning predicate")
                .isPresent();
        assertThat(pruning.get())
                .as("pruning predicate should be a BboxIntersects")
                .isInstanceOf(Spatial.BboxIntersects.class);
    }

    // --- contains / within inversion pin ---

    @Test
    void contains_asymmetric_bigCandidateContainsSmallQuery_gatePresent() {
        // query = small box; candidate = big box that contains the query
        Geometry smallQuery = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");
        Geometry bigCandidate = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.contains(COL, smallQuery);

        assertThat(filter.gate(toWkb(bigCandidate)))
                .as("big candidate that contains the small query should pass 'candidate contains query'")
                .isPresent();
    }

    @Test
    void contains_asymmetric_smallCandidateInsideBigQuery_gateEmpty() {
        // query = big box; candidate = small box inside the query - candidate does NOT contain query
        Geometry bigQuery = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        Geometry smallCandidate = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");

        JtsGeometryFilter filter = JtsGeometryFilter.contains(COL, bigQuery);

        assertThat(filter.gate(toWkb(smallCandidate)))
                .as("small candidate inside the big query does not satisfy 'candidate contains query'")
                .isEmpty();
    }

    @Test
    void contains_pruningPredicateIsBboxContains() {
        Geometry query = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");

        JtsGeometryFilter filter = JtsGeometryFilter.contains(COL, query);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("contains should provide a BboxContains pruning predicate")
                .isPresent();
        assertThat(pruning.get())
                .as("pruning predicate type should be BboxContains")
                .isInstanceOf(Spatial.BboxContains.class);
    }

    @Test
    void within_asymmetric_smallCandidateInsideBigQuery_gatePresent() {
        // query = big box; candidate = small box inside the query - candidate IS within query
        Geometry bigQuery = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        Geometry smallCandidate = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");

        JtsGeometryFilter filter = JtsGeometryFilter.within(COL, bigQuery);

        assertThat(filter.gate(toWkb(smallCandidate)))
                .as("small candidate that is within the big query should pass 'candidate within query'")
                .isPresent();
    }

    @Test
    void within_asymmetric_bigCandidateContainsSmallQuery_gateEmpty() {
        // query = small box; candidate = big box that contains the query - candidate is NOT within query
        Geometry smallQuery = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");
        Geometry bigCandidate = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.within(COL, smallQuery);

        assertThat(filter.gate(toWkb(bigCandidate)))
                .as("big candidate that contains the small query does not satisfy 'candidate within query'")
                .isEmpty();
    }

    @Test
    void within_pruningPredicateIsBboxCoveredBy() {
        Geometry query = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.within(COL, query);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("within should provide a BboxCoveredBy pruning predicate")
                .isPresent();
        assertThat(pruning.get())
                .as("pruning predicate type should be BboxCoveredBy")
                .isInstanceOf(Spatial.BboxCoveredBy.class);
    }

    // --- covers / coveredBy inversion pin ---

    @Test
    void covers_bigCandidateCoversSmallQuery_gatePresent() {
        Geometry smallQuery = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");
        Geometry bigCandidate = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.covers(COL, smallQuery);

        assertThat(filter.gate(toWkb(bigCandidate)))
                .as("big candidate covering the small query should pass 'candidate covers query'")
                .isPresent();
    }

    @Test
    void covers_pruningPredicateIsBboxContains() {
        Geometry query = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");

        JtsGeometryFilter filter = JtsGeometryFilter.covers(COL, query);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("covers should provide a BboxContains pruning predicate")
                .isPresent();
        assertThat(pruning.get())
                .as("pruning predicate type should be BboxContains")
                .isInstanceOf(Spatial.BboxContains.class);
    }

    @Test
    void coveredBy_smallCandidateInsideBigQuery_gatePresent() {
        Geometry bigQuery = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");
        Geometry smallCandidate = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");

        JtsGeometryFilter filter = JtsGeometryFilter.coveredBy(COL, bigQuery);

        assertThat(filter.gate(toWkb(smallCandidate)))
                .as("small candidate inside the big query should pass 'candidate coveredBy query'")
                .isPresent();
    }

    @Test
    void coveredBy_pruningPredicateIsBboxCoveredBy() {
        Geometry query = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.coveredBy(COL, query);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("coveredBy should provide a BboxCoveredBy pruning predicate")
                .isPresent();
        assertThat(pruning.get())
                .as("pruning predicate type should be BboxCoveredBy")
                .isInstanceOf(Spatial.BboxCoveredBy.class);
    }

    @Test
    void covers_differs_from_coveredBy_asymmetric() {
        // A big candidate covers a small query, but the big candidate is NOT coveredBy the small query
        Geometry smallQuery = wkt("POLYGON ((2 2, 4 2, 4 4, 2 4, 2 2))");
        Geometry bigCandidate = wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))");

        JtsGeometryFilter coversFilter = JtsGeometryFilter.covers(COL, smallQuery);
        JtsGeometryFilter coveredByFilter = JtsGeometryFilter.coveredBy(COL, smallQuery);

        assertThat(coversFilter.gate(toWkb(bigCandidate)))
                .as("big candidate covers small query: covers() gate should be present")
                .isPresent();
        assertThat(coveredByFilter.gate(toWkb(bigCandidate)))
                .as("big candidate does not fit inside small query: coveredBy() gate should be empty")
                .isEmpty();
    }

    // --- disjoint ---

    @Test
    void disjoint_disjointCandidate_gatePresent() {
        Geometry query = wkt("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");
        Geometry disjointCandidate = wkt("POINT (20 20)");

        JtsGeometryFilter filter = JtsGeometryFilter.disjoint(COL, query);

        assertThat(filter.gate(toWkb(disjointCandidate)))
                .as("a disjoint candidate should be retained by disjoint()")
                .isPresent();
    }

    @Test
    void disjoint_intersectingCandidate_gateEmpty() {
        Geometry query = wkt("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");
        Geometry intersectingCandidate = wkt("POINT (2 2)");

        JtsGeometryFilter filter = JtsGeometryFilter.disjoint(COL, query);

        assertThat(filter.gate(toWkb(intersectingCandidate)))
                .as("an intersecting candidate should be dropped by disjoint()")
                .isEmpty();
    }

    @Test
    void disjoint_pruningPredicateIsEmpty() {
        Geometry query = wkt("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.disjoint(COL, query);

        assertThat(filter.pruningPredicate())
                .as("disjoint has no sound bbox lowering; pruningPredicate must be empty")
                .isEmpty();
    }

    // --- dwithin ---

    @Test
    void dwithin_candidateWithinDistance_gatePresent() {
        Geometry query = wkt("POINT (0 0)");
        Geometry nearCandidate = wkt("POINT (3 4)");
        double distance = 5.1;

        JtsGeometryFilter filter = JtsGeometryFilter.dwithin(COL, query, distance);

        assertThat(filter.gate(toWkb(nearCandidate)))
                .as("candidate at distance ~5 should be retained when dwithin distance is 5.1")
                .isPresent();
    }

    @Test
    void dwithin_candidateBeyondDistance_gateEmpty() {
        Geometry query = wkt("POINT (0 0)");
        Geometry farCandidate = wkt("POINT (3 4)");
        double distance = 4.9;

        JtsGeometryFilter filter = JtsGeometryFilter.dwithin(COL, query, distance);

        assertThat(filter.gate(toWkb(farCandidate)))
                .as("candidate at distance ~5 should be dropped when dwithin distance is 4.9")
                .isEmpty();
    }

    @Test
    void dwithin_pruningPredicateIsBboxIntersects_withExpandedBbox() {
        Geometry query = wkt("POINT (5 5)");
        double distance = 3.0;

        JtsGeometryFilter filter = JtsGeometryFilter.dwithin(COL, query, distance);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("dwithin should provide a BboxIntersects pruning predicate")
                .isPresent();
        Spatial.BboxIntersects bboxPred = (Spatial.BboxIntersects) pruning.get();
        Bbox bbox = bboxPred.bbox();

        assertThat(bbox.minX())
                .as("dwithin bbox minX should be query envelope minX expanded by distance")
                .isEqualTo(5.0 - distance);
        assertThat(bbox.minY())
                .as("dwithin bbox minY should be query envelope minY expanded by distance")
                .isEqualTo(5.0 - distance);
        assertThat(bbox.maxX())
                .as("dwithin bbox maxX should be query envelope maxX expanded by distance")
                .isEqualTo(5.0 + distance);
        assertThat(bbox.maxY())
                .as("dwithin bbox maxY should be query envelope maxY expanded by distance")
                .isEqualTo(5.0 + distance);
    }

    // --- equalsExact ---

    @Test
    void equalsExact_identicalGeometry_gatePresent() {
        Geometry query = wkt("LINESTRING (0 0, 10 10)");
        Geometry identical = wkt("LINESTRING (0 0, 10 10)");

        JtsGeometryFilter filter = JtsGeometryFilter.equalsExact(COL, query);

        assertThat(filter.gate(toWkb(identical)))
                .as("geometry identical to the query should be retained by equalsExact")
                .isPresent();
    }

    @Test
    void equalsExact_differentGeometry_gateEmpty() {
        Geometry query = wkt("LINESTRING (0 0, 10 10)");
        Geometry different = wkt("LINESTRING (0 0, 5 5)");

        JtsGeometryFilter filter = JtsGeometryFilter.equalsExact(COL, query);

        assertThat(filter.gate(toWkb(different)))
                .as("geometry different from the query should be dropped by equalsExact")
                .isEmpty();
    }

    @Test
    void equalsExact_pruningPredicateIsBboxEquals() {
        Geometry query = wkt("POLYGON ((0 0, 5 0, 5 5, 0 5, 0 0))");

        JtsGeometryFilter filter = JtsGeometryFilter.equalsExact(COL, query);

        Optional<Spatial> pruning = filter.pruningPredicate();
        assertThat(pruning)
                .as("equalsExact should provide a BboxEquals pruning predicate")
                .isPresent();
        assertThat(pruning.get())
                .as("pruning predicate type should be BboxEquals")
                .isInstanceOf(Spatial.BboxEquals.class);
    }

    // --- column() accessor ---

    @Test
    void column_returnsTheColumnPassedToFactory() {
        Geometry query = wkt("POINT (0 0)");

        JtsGeometryFilter filter = JtsGeometryFilter.intersects(COL, query);

        assertThat(filter.column())
                .as("column() should return the column path passed to the factory")
                .isEqualTo(COL);
    }
}
