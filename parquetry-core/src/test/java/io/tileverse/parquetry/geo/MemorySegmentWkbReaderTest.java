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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * Correctness check for {@link MemorySegmentWkbReader} against JTS's own {@link WKBReader}. Each fixture geometry is
 * encoded to WKB by {@link WKBWriter} in both byte orders, then decoded by both readers; the results must agree on 2D
 * structure ({@link Geometry#equalsExact}) and, for Z/M fixtures, on every Z/M ordinate (which {@code equalsExact}
 * ignores).
 */
class MemorySegmentWkbReaderTest {

    private static final GeometryFactory PACKED_FACTORY = new GeometryFactory(new PackedCoordinateSequenceFactory());

    private final MemorySegmentWkbReader reader = new MemorySegmentWkbReader(PACKED_FACTORY);

    /**
     * One corpus entry: the parsed geometry, the output dimension for the WKB writer (2/3/4), the ordinate set the
     * writer must emit, and whether the fixture has Z and / or M ordinates that must be compared beyond the XY that
     * {@code equalsExact} already checks. The explicit ordinate set matters for the XYM case: a plain
     * {@code WKBWriter(3)} emits XYZ, dropping the measure; only {@code setOutputOrdinates(X, Y, M)} encodes it.
     */
    private record Fixture(
            String name,
            Geometry geometry,
            int outputDimension,
            EnumSet<Ordinate> ordinates,
            boolean hasZ,
            boolean hasM,
            int srid) {
        @Override
        public String toString() {
            return name;
        }
    }

    static Stream<Arguments> corpus() {
        List<Fixture> fixtures = buildCorpus();
        List<Arguments> args = new ArrayList<>();
        int[] byteOrders = {ByteOrderValues.LITTLE_ENDIAN, ByteOrderValues.BIG_ENDIAN};
        for (Fixture fixture : fixtures) {
            for (int byteOrder : byteOrders) {
                String label = fixture.name() + (byteOrder == ByteOrderValues.LITTLE_ENDIAN ? "/LE" : "/BE");
                args.add(Arguments.of(label, fixture, byteOrder));
            }
        }
        return args.stream();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void decodesSameAsJtsWkbReader(String label, Fixture fixture, int byteOrder) throws ParseException {
        WKBWriter writer = new WKBWriter(fixture.outputDimension(), byteOrder, fixture.srid() != 0);
        writer.setOutputOrdinates(fixture.ordinates());
        byte[] wkb = writer.write(fixture.geometry());
        MemorySegment segment = MemorySegment.ofArray(wkb).asReadOnly();

        Geometry expected = new WKBReader(PACKED_FACTORY).read(wkb);
        Geometry actual = reader.read(segment);

        assertThat(actual.equalsExact(expected))
                .as(
                        "%s: custom decode must equal WKBReader decode (2D); expected=%s actual=%s",
                        label, expected, actual)
                .isTrue();

        assertSameZmOrdinates(label, fixture, expected, actual);
        assertSameSrid(label, expected, actual);
    }

    @Test
    void readsGeometryFromNonZeroBackingOffset() throws ParseException {
        Geometry geometry = new WKTReader(PACKED_FACTORY).read("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))");
        byte[] wkb = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(geometry);
        byte[] padded = new byte[7 + wkb.length];
        System.arraycopy(wkb, 0, padded, 7, wkb.length);
        MemorySegment backing = MemorySegment.ofArray(padded).asReadOnly();

        Geometry atOffset = reader.read(backing, 7L, wkb.length);
        Geometry atZero = reader.read(MemorySegment.ofArray(wkb).asReadOnly());

        assertThat(atOffset.equalsExact(atZero))
                .as("decode from a non-zero backing offset must equal the offset-0 decode")
                .isTrue();
    }

    /**
     * Compares the SRID at every geometry node. EWKB encodes the SRID once on the top geometry, but JTS WKBReader
     * builds every member with that SRID; the custom reader must produce the same SRID throughout the tree (plain WKB
     * yields 0 on both).
     */
    private static void assertSameSrid(String label, Geometry expected, Geometry actual) {
        assertThat(actual.getSRID()).as("%s: SRID", label).isEqualTo(expected.getSRID());
        if (expected instanceof org.locationtech.jts.geom.GeometryCollection) {
            assertThat(actual.getNumGeometries()).as("%s: child count", label).isEqualTo(expected.getNumGeometries());
            for (int i = 0; i < expected.getNumGeometries(); i++) {
                assertSameSrid(label + "[" + i + "]", expected.getGeometryN(i), actual.getGeometryN(i));
            }
        }
    }

    /**
     * Compares Z and / or M ordinates, which {@code equalsExact} ignores. The {@link CoordinateSequence#getZ(int)} /
     * {@link CoordinateSequence#getM(int)} accessors resolve the right packed-array index for each sequence's
     * dimension, unlike the raw {@code Z}/{@code M} ordinate-index constants which differ between XYM (dim 3) and XYZM
     * (dim 4).
     */
    private static void assertSameZmOrdinates(String label, Fixture fixture, Geometry expected, Geometry actual) {
        if (!fixture.hasZ() && !fixture.hasM()) {
            return;
        }
        List<CoordinateSequence> expectedSeqs = sequencesOf(expected);
        List<CoordinateSequence> actualSeqs = sequencesOf(actual);
        assertThat(actualSeqs)
                .as("%s: same number of coordinate sequences", label)
                .hasSameSizeAs(expectedSeqs);
        for (int s = 0; s < expectedSeqs.size(); s++) {
            CoordinateSequence expectedSeq = expectedSeqs.get(s);
            CoordinateSequence actualSeq = actualSeqs.get(s);
            assertThat(actualSeq.size()).as("%s: sequence %d size", label, s).isEqualTo(expectedSeq.size());
            for (int i = 0; i < expectedSeq.size(); i++) {
                if (fixture.hasZ()) {
                    assertThat(actualSeq.getZ(i))
                            .as("%s: seq %d coord %d Z", label, s, i)
                            .isEqualTo(expectedSeq.getZ(i));
                }
                if (fixture.hasM()) {
                    assertThat(actualSeq.getM(i))
                            .as("%s: seq %d coord %d M", label, s, i)
                            .isEqualTo(expectedSeq.getM(i));
                }
            }
        }
    }

    /** Flattens a geometry into its constituent coordinate sequences in deterministic order. */
    private static List<CoordinateSequence> sequencesOf(Geometry geometry) {
        List<CoordinateSequence> out = new ArrayList<>();
        collectSequences(geometry, out);
        return out;
    }

    private static void collectSequences(Geometry geometry, List<CoordinateSequence> out) {
        switch (geometry) {
            case org.locationtech.jts.geom.Point point -> out.add(point.getCoordinateSequence());
            case LineString line -> out.add(line.getCoordinateSequence());
            case Polygon polygon -> {
                out.add(polygon.getExteriorRing().getCoordinateSequence());
                for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
                    out.add(polygon.getInteriorRingN(i).getCoordinateSequence());
                }
            }
            default -> {
                for (int i = 0; i < geometry.getNumGeometries(); i++) {
                    collectSequences(geometry.getGeometryN(i), out);
                }
            }
        }
    }

    // --- corpus ---

    private static List<Fixture> buildCorpus() {
        WKTReader wkt = new WKTReader(PACKED_FACTORY);
        List<Fixture> fixtures = new ArrayList<>();

        fixtures.add(xy("point", wkt, "POINT (30 10)"));
        fixtures.add(xy("linestring", wkt, "LINESTRING (30 10, 10 30, 40 40)"));
        fixtures.add(xy("polygon", wkt, "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"));
        fixtures.add(xy(
                "polygonWithHole", wkt, "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"));
        fixtures.add(xy("multipoint", wkt, "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"));
        fixtures.add(
                xy("multilinestring", wkt, "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"));
        fixtures.add(xy(
                "multipolygon",
                wkt,
                "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"));
        fixtures.add(xy(
                "multipolygonWithHole",
                wkt,
                "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), "
                        + "((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"));
        fixtures.add(xy(
                "geometryCollection",
                wkt,
                "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10), "
                        + "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)))"));
        fixtures.add(xy(
                "nestedGeometryCollection", wkt, "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2)), POINT (3 4))"));

        fixtures.add(xy("pointEmpty", wkt, "POINT EMPTY"));
        fixtures.add(xy("polygonEmpty", wkt, "POLYGON EMPTY"));
        fixtures.add(xy("linestringEmpty", wkt, "LINESTRING EMPTY"));
        fixtures.add(xy("multipolygonEmpty", wkt, "MULTIPOLYGON EMPTY"));
        fixtures.add(xy("geometryCollectionEmpty", wkt, "GEOMETRYCOLLECTION EMPTY"));

        fixtures.add(z("pointZ", wkt, "POINT Z (30 10 5)"));
        fixtures.add(z("linestringZ", wkt, "LINESTRING Z (30 10 1, 10 30 2, 40 40 3)"));
        fixtures.add(z("polygonZ", wkt, "POLYGON Z ((30 10 1, 40 40 2, 20 40 3, 10 20 4, 30 10 1))"));
        fixtures.add(z("multipointZ", wkt, "MULTIPOINT Z ((10 40 1), (40 30 2))"));

        fixtures.add(m("pointM", wkt, "POINT M (30 10 7)"));
        fixtures.add(m("linestringM", wkt, "LINESTRING M (30 10 1, 10 30 2, 40 40 3)"));

        fixtures.add(z(
                "multilinestringZ",
                wkt,
                "MULTILINESTRING Z ((10 10 1, 20 20 2, 10 40 3), (40 40 4, 30 30 5, 40 20 6))"));
        fixtures.add(z(
                "multipolygonZ",
                wkt,
                "MULTIPOLYGON Z (((30 20 1, 45 40 2, 10 40 3, 30 20 1)), "
                        + "((15 5 4, 40 10 5, 10 20 6, 5 10 7, 15 5 4)))"));
        fixtures.add(z(
                "geometryCollectionZ",
                wkt,
                "GEOMETRYCOLLECTION Z (POINT Z (4 6 8), LINESTRING Z (4 6 8, 7 10 11), "
                        + "POLYGON Z ((30 10 1, 40 40 2, 20 40 3, 10 20 4, 30 10 1)))"));

        fixtures.add(zm("pointZM", wkt, "POINT ZM (30 10 5 7)"));
        fixtures.add(zm("linestringZM", wkt, "LINESTRING ZM (30 10 1 2, 10 30 3 4, 40 40 5 6)"));
        fixtures.add(zm("polygonZM", wkt, "POLYGON ZM ((30 10 1 9, 40 40 2 8, 20 40 3 7, 10 20 4 6, 30 10 1 9))"));
        fixtures.add(zm(
                "multipolygonZM",
                wkt,
                "MULTIPOLYGON ZM (((30 20 1 5, 45 40 2 6, 10 40 3 7, 30 20 1 5)), "
                        + "((15 5 4 1, 40 10 5 2, 10 20 6 3, 5 10 7 4, 15 5 4 1)))"));
        fixtures.add(zm(
                "geometryCollectionZM",
                wkt,
                "GEOMETRYCOLLECTION ZM (POINT ZM (4 6 8 1), LINESTRING ZM (4 6 8 1, 7 10 11 2))"));

        // EWKB: the WKB encodes an SRID on the geometry; MemorySegmentWkbReader must preserve it.
        fixtures.add(ewkb(xy("point", wkt, "POINT (30 10)"), 4326));
        fixtures.add(ewkb(xy("polygon", wkt, "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"), 3857));
        fixtures.add(ewkb(z("pointZ", wkt, "POINT Z (30 10 5)"), 4326));
        fixtures.add(
                ewkb(xy("geometryCollection", wkt, "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))"), 4326));
        fixtures.add(ewkb(
                xy(
                        "multipolygon",
                        wkt,
                        "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
                4326));

        return fixtures;
    }

    private static Fixture xy(String name, WKTReader wkt, String wktText) {
        return new Fixture(name, parse(wkt, wktText), 2, EnumSet.of(Ordinate.X, Ordinate.Y), false, false, 0);
    }

    private static Fixture z(String name, WKTReader wkt, String wktText) {
        return new Fixture(
                name, parse(wkt, wktText), 3, EnumSet.of(Ordinate.X, Ordinate.Y, Ordinate.Z), true, false, 0);
    }

    private static Fixture m(String name, WKTReader wkt, String wktText) {
        return new Fixture(
                name, parse(wkt, wktText), 3, EnumSet.of(Ordinate.X, Ordinate.Y, Ordinate.M), false, true, 0);
    }

    private static Fixture zm(String name, WKTReader wkt, String wktText) {
        return new Fixture(
                name,
                parse(wkt, wktText),
                4,
                EnumSet.of(Ordinate.X, Ordinate.Y, Ordinate.Z, Ordinate.M),
                true,
                true,
                0);
    }

    /**
     * Stamps {@code srid} onto a base fixture's geometry and marks it for EWKB encoding. The WKB writer emits the EWKB
     * SRID flag and the SRID value; {@code WKBReader} reads it back, and {@link MemorySegmentWkbReader} must preserve
     * it. JTS's {@code WKTReader} cannot parse EWKT, hence the SRID is applied here rather than in the WKT text.
     */
    private static Fixture ewkb(Fixture base, int srid) {
        base.geometry().setSRID(srid);
        return new Fixture(
                base.name() + "Ewkb",
                base.geometry(),
                base.outputDimension(),
                base.ordinates(),
                base.hasZ(),
                base.hasM(),
                srid);
    }

    private static Geometry parse(WKTReader wkt, String wktText) {
        try {
            return wkt.read(wktText);
        } catch (ParseException e) {
            throw new IllegalStateException("Bad WKT fixture: " + wktText, e);
        }
    }

    // --- error path ---

    @Test
    void malformedByteOrderByteThrows() {
        byte[] bogus = {0x07, 0x00, 0x00, 0x00, 0x00};
        MemorySegment segment = MemorySegment.ofArray(bogus).asReadOnly();
        assertThatThrownBy(() -> reader.read(segment))
                .as("invalid byte-order marker must throw MalformedFileException")
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Failed to decode WKB geometry");
    }

    @Test
    void truncatedPayloadThrows() {
        byte[] truncated = {0x01, 0x01, 0x00, 0x00, 0x00, 0x00};
        MemorySegment segment = MemorySegment.ofArray(truncated).asReadOnly();
        assertThatThrownBy(() -> reader.read(segment))
                .as("a point WKB missing its ordinates must throw MalformedFileException")
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Failed to decode WKB geometry");
    }

    @Test
    void truncatedValueInLargerBackingThrowsInsteadOfReadingPastItsLength() throws ParseException {
        Geometry point = new WKTReader(PACKED_FACTORY).read("POINT (1 2)");
        byte[] wkb = new WKBWriter(2, ByteOrderValues.LITTLE_ENDIAN).write(point);
        // Put the value in a backing that continues with readable bytes past it; a decoder that ignored the value
        // length would read those adjacent bytes as the missing Y ordinate instead of failing.
        byte[] backingBytes = new byte[wkb.length + 8];
        System.arraycopy(wkb, 0, backingBytes, 0, wkb.length);
        MemorySegment backing = MemorySegment.ofArray(backingBytes).asReadOnly();

        long truncatedLength = wkb.length - 8L; // drops the 8-byte Y ordinate from the value

        assertThatThrownBy(() -> reader.read(backing, 0L, truncatedLength))
                .as("a value truncated within a larger backing must throw, not read past its length")
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Failed to decode WKB geometry");
    }

    @Test
    void garbageElementCountIsRejectedWithoutAllocating() {
        // A MultiPoint header (type 4) that claims ~2.1 billion members in a 9-byte value. Bounding the count to the
        // value's remaining bytes rejects it instead of allocating a multi-gigabyte array.
        byte[] bytes = {0x01, 0x04, 0x00, 0x00, 0x00, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f};
        MemorySegment segment = MemorySegment.ofArray(bytes).asReadOnly();
        assertThatThrownBy(() -> reader.read(segment))
                .as("an element count that cannot fit the value must throw, not over-allocate")
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Failed to decode WKB geometry");
    }
}
