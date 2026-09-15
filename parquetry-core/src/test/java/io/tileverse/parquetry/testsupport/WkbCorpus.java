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
package io.tileverse.parquetry.testsupport;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.Ordinate;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

/**
 * The WKB corpus shared by the reader and the envelope tests: every base kind, holes, nested collections, the five
 * empties, Z/M/ZM, and EWKB with an SRID, each encoded by JTS in both byte orders, plus ISO-coded Z/M/ZM entries that
 * JTS cannot emit. Each entry pairs a WKB value with its source JTS geometry, which is the oracle.
 *
 * <p>JTS is the encoder on purpose: a bug shared with parquetry's own writer would otherwise slip through both sides of
 * a round trip.
 */
public final class WkbCorpus {

    private static final GeometryFactory PACKED_FACTORY = new GeometryFactory(new PackedCoordinateSequenceFactory());
    private static final int[] BOTH_ORDERS = {ByteOrderValues.LITTLE_ENDIAN, ByteOrderValues.BIG_ENDIAN};

    /**
     * One WKB value and its source JTS geometry. {@code hasZ} / {@code hasM} say which extra ordinates the geometry
     * has; {@code srid} is the EWKB SRID, 0 when the value has none.
     */
    public record Entry(String label, Geometry geometry, byte[] bytes, boolean hasZ, boolean hasM, int srid) {

        /** The value as a read-only heap segment. */
        public MemorySegment wkb() {
            return MemorySegment.ofArray(bytes).asReadOnly();
        }

        @Override
        public String toString() {
            return label;
        }
    }

    /** A geometry and how JTS must encode it: output dimension, the ordinate set, and the flags of the result. */
    private record Fixture(
            String name,
            Geometry geometry,
            int outputDimension,
            EnumSet<Ordinate> ordinates,
            boolean hasZ,
            boolean hasM,
            int srid) {}

    private WkbCorpus() {}

    /** Every fixture in both byte orders, then the ISO-coded entries. */
    public static List<Entry> entries() {
        List<Entry> entries = new ArrayList<>();
        for (Fixture fixture : fixtures()) {
            for (int byteOrder : BOTH_ORDERS) {
                entries.add(encode(fixture, byteOrder));
            }
        }
        entries.addAll(isoEntries());
        return entries;
    }

    private static Entry encode(Fixture fixture, int byteOrder) {
        WKBWriter writer = new WKBWriter(fixture.outputDimension(), byteOrder, fixture.srid() != 0);
        writer.setOutputOrdinates(fixture.ordinates());
        byte[] bytes = writer.write(fixture.geometry());
        String label = fixture.name() + (byteOrder == ByteOrderValues.LITTLE_ENDIAN ? "/LE" : "/BE");
        return new Entry(label, fixture.geometry(), bytes, fixture.hasZ(), fixture.hasM(), fixture.srid());
    }

    /**
     * ISO-coded Z/M/ZM entries. JTS writes the EWKB high-bit flags; patching the type code of a single (non-collection)
     * geometry to the ISO code yields the same coordinates under the other encoding.
     */
    private static List<Entry> isoEntries() {
        List<Entry> entries = new ArrayList<>();
        for (int byteOrder : BOTH_ORDERS) {
            entries.add(iso(encode(z("pointZ", "POINT Z (30 10 5)"), byteOrder), 1001));
            entries.add(iso(encode(m("linestringM", "LINESTRING M (30 10 1, 10 30 2, 40 40 3)"), byteOrder), 2002));
            entries.add(iso(
                    encode(
                            zm("polygonZM", "POLYGON ZM ((30 10 1 9, 40 40 2 8, 20 40 3 7, 10 20 4 6, 30 10 1 9))"),
                            byteOrder),
                    3003));
        }
        return entries;
    }

    private static Entry iso(Entry ewkb, int isoType) {
        byte[] bytes = ewkb.bytes().clone();
        ByteOrder order = bytes[0] == 0x01 ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN;
        ByteBuffer.wrap(bytes).order(order).putInt(1, isoType);
        String label = ewkb.label().replace("/", "Iso/");
        return new Entry(label, ewkb.geometry(), bytes, ewkb.hasZ(), ewkb.hasM(), 0);
    }

    private static List<Fixture> fixtures() {
        List<Fixture> fixtures = new ArrayList<>();

        fixtures.add(xy("point", "POINT (30 10)"));
        fixtures.add(xy("linestring", "LINESTRING (30 10, 10 30, 40 40)"));
        fixtures.add(xy("polygon", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"));
        fixtures.add(
                xy("polygonWithHole", "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"));
        fixtures.add(xy("multipoint", "MULTIPOINT ((10 40), (40 30), (20 20), (30 10))"));
        fixtures.add(xy("multilinestring", "MULTILINESTRING ((10 10, 20 20, 10 40), (40 40, 30 30, 40 20, 30 10))"));
        fixtures.add(xy(
                "multipolygon", "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"));
        fixtures.add(xy(
                "multipolygonWithHole",
                "MULTIPOLYGON (((40 40, 20 45, 45 30, 40 40)), "
                        + "((20 35, 10 30, 10 10, 30 5, 45 20, 20 35), (30 20, 20 15, 20 25, 30 20)))"));
        fixtures.add(xy(
                "geometryCollection",
                "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10), "
                        + "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10)))"));
        fixtures.add(
                xy("nestedGeometryCollection", "GEOMETRYCOLLECTION (GEOMETRYCOLLECTION (POINT (1 2)), POINT (3 4))"));

        fixtures.add(xy("pointEmpty", "POINT EMPTY"));
        fixtures.add(xy("polygonEmpty", "POLYGON EMPTY"));
        fixtures.add(xy("linestringEmpty", "LINESTRING EMPTY"));
        fixtures.add(xy("multipolygonEmpty", "MULTIPOLYGON EMPTY"));
        fixtures.add(xy("geometryCollectionEmpty", "GEOMETRYCOLLECTION EMPTY"));

        fixtures.add(z("pointZ", "POINT Z (30 10 5)"));
        fixtures.add(z("linestringZ", "LINESTRING Z (30 10 1, 10 30 2, 40 40 3)"));
        fixtures.add(z("polygonZ", "POLYGON Z ((30 10 1, 40 40 2, 20 40 3, 10 20 4, 30 10 1))"));
        fixtures.add(z("multipointZ", "MULTIPOINT Z ((10 40 1), (40 30 2))"));

        fixtures.add(m("pointM", "POINT M (30 10 7)"));
        fixtures.add(m("linestringM", "LINESTRING M (30 10 1, 10 30 2, 40 40 3)"));

        fixtures.add(
                z("multilinestringZ", "MULTILINESTRING Z ((10 10 1, 20 20 2, 10 40 3), (40 40 4, 30 30 5, 40 20 6))"));
        fixtures.add(z(
                "multipolygonZ",
                "MULTIPOLYGON Z (((30 20 1, 45 40 2, 10 40 3, 30 20 1)), "
                        + "((15 5 4, 40 10 5, 10 20 6, 5 10 7, 15 5 4)))"));
        fixtures.add(z(
                "geometryCollectionZ",
                "GEOMETRYCOLLECTION Z (POINT Z (4 6 8), LINESTRING Z (4 6 8, 7 10 11), "
                        + "POLYGON Z ((30 10 1, 40 40 2, 20 40 3, 10 20 4, 30 10 1)))"));

        fixtures.add(zm("pointZM", "POINT ZM (30 10 5 7)"));
        fixtures.add(zm("linestringZM", "LINESTRING ZM (30 10 1 2, 10 30 3 4, 40 40 5 6)"));
        fixtures.add(zm("polygonZM", "POLYGON ZM ((30 10 1 9, 40 40 2 8, 20 40 3 7, 10 20 4 6, 30 10 1 9))"));
        fixtures.add(zm(
                "multipolygonZM",
                "MULTIPOLYGON ZM (((30 20 1 5, 45 40 2 6, 10 40 3 7, 30 20 1 5)), "
                        + "((15 5 4 1, 40 10 5 2, 10 20 6 3, 5 10 7 4, 15 5 4 1)))"));
        fixtures.add(zm(
                "geometryCollectionZM",
                "GEOMETRYCOLLECTION ZM (POINT ZM (4 6 8 1), LINESTRING ZM (4 6 8 1, 7 10 11 2))"));

        fixtures.add(ewkb(xy("point", "POINT (30 10)"), 4326));
        fixtures.add(ewkb(xy("polygon", "POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))"), 3857));
        fixtures.add(ewkb(z("pointZ", "POINT Z (30 10 5)"), 4326));
        fixtures.add(ewkb(xy("geometryCollection", "GEOMETRYCOLLECTION (POINT (4 6), LINESTRING (4 6, 7 10))"), 4326));
        fixtures.add(ewkb(
                xy("multipolygon", "MULTIPOLYGON (((30 20, 45 40, 10 40, 30 20)), ((15 5, 40 10, 10 20, 5 10, 15 5)))"),
                4326));

        return fixtures;
    }

    private static Fixture xy(String name, String wkt) {
        return new Fixture(name, parse(wkt), 2, EnumSet.of(Ordinate.X, Ordinate.Y), false, false, 0);
    }

    private static Fixture z(String name, String wkt) {
        return new Fixture(name, parse(wkt), 3, EnumSet.of(Ordinate.X, Ordinate.Y, Ordinate.Z), true, false, 0);
    }

    private static Fixture m(String name, String wkt) {
        return new Fixture(name, parse(wkt), 3, EnumSet.of(Ordinate.X, Ordinate.Y, Ordinate.M), false, true, 0);
    }

    private static Fixture zm(String name, String wkt) {
        return new Fixture(
                name, parse(wkt), 4, EnumSet.of(Ordinate.X, Ordinate.Y, Ordinate.Z, Ordinate.M), true, true, 0);
    }

    /**
     * Stamps {@code srid} onto a base fixture's geometry and marks it for EWKB encoding: the writer then emits the SRID
     * flag and value. JTS's {@code WKTReader} cannot parse EWKT, hence the SRID is applied here rather than in the WKT.
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

    private static Geometry parse(String wkt) {
        try {
            return new WKTReader(PACKED_FACTORY).read(wkt);
        } catch (ParseException e) {
            throw new IllegalStateException("Bad WKT fixture: " + wkt, e);
        }
    }
}
