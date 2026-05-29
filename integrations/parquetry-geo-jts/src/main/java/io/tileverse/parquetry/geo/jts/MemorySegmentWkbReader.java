/*
 * Copyright (c) 2026 Multivers.io
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
package io.tileverse.parquetry.geo.jts;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;

/**
 * Thread-safe WKB-to-JTS reader that reads directly from a {@link MemorySegment} onto packed coordinate sequences,
 * without {@code WKBReader}, {@code InStream}, or any intermediate {@code byte[]} copy; state-free per call.
 *
 * <p>The reader holds only an immutable {@link GeometryFactory} (default backed by
 * {@link PackedCoordinateSequenceFactory}); all cursor state lives in locals of {@link #read(MemorySegment)}, hence one
 * instance is safe to share across threads.
 *
 * <p>The decode walks the WKB structure exactly as the wire format mandates: a leading byte-order byte (0x00
 * big-endian, 0x01 little-endian) at the start of each geometry, followed by a 4-byte type code in the declared byte
 * order. Both ISO encodings (Z/M/ZM via the {@code +1000}/{@code +2000}/{@code +3000} type-code offsets) and EWKB
 * encodings (Z/M via the {@code 0x80000000}/{@code 0x40000000} high-bit flags, with an optional {@code 0x20000000} SRID
 * flag whose 4-byte value is read and stamped on the geometry) are accepted, matching what JTS's own {@code WKBReader}
 * accepts.
 *
 * <p>The key optimization is that a ring or component of N points with D dimensions is a contiguous block of N*D
 * little- or big-endian doubles in the segment, which is exactly the backing layout of a
 * {@link PackedCoordinateSequence.Double}. Each ring is read with one bulk {@link MemorySegment#copy} into a
 * {@code double[]}, with no per-coordinate loop, no {@code Coordinate} allocation, and no {@code byte[]} copy.
 */
public final class MemorySegmentWkbReader {

    private static final ValueLayout.OfInt INT_LE = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfInt INT_BE = JAVA_INT_UNALIGNED.withOrder(BIG_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_LE = JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_BE = JAVA_DOUBLE_UNALIGNED.withOrder(BIG_ENDIAN);

    private static final byte WKB_BIG_ENDIAN = 0x00;
    private static final byte WKB_LITTLE_ENDIAN = 0x01;

    private static final int TYPE_POINT = 1;
    private static final int TYPE_LINESTRING = 2;
    private static final int TYPE_POLYGON = 3;
    private static final int TYPE_MULTIPOINT = 4;
    private static final int TYPE_MULTILINESTRING = 5;
    private static final int TYPE_MULTIPOLYGON = 6;
    private static final int TYPE_GEOMETRYCOLLECTION = 7;

    private static final int Z_OFFSET = 1000;
    private static final int M_OFFSET = 2000;
    private static final int ZM_OFFSET = 3000;

    private static final int EWKB_FLAG_Z = 0x80000000;
    private static final int EWKB_FLAG_M = 0x40000000;
    private static final int EWKB_FLAG_SRID = 0x20000000;
    private static final int EWKB_FLAG_MASK = EWKB_FLAG_Z | EWKB_FLAG_M | EWKB_FLAG_SRID;

    private static final GeometryFactory DEFAULT_FACTORY = new GeometryFactory(new PackedCoordinateSequenceFactory());

    private final GeometryFactory geometryFactory;

    /** Creates a reader backed by a {@link PackedCoordinateSequenceFactory}. */
    public MemorySegmentWkbReader() {
        this(DEFAULT_FACTORY);
    }

    /**
     * Creates a reader that builds geometries with {@code geometryFactory}. The factory must be immutable and
     * thread-safe (the JTS packed and array factories both are).
     */
    public MemorySegmentWkbReader(GeometryFactory geometryFactory) {
        this.geometryFactory = geometryFactory;
    }

    /**
     * Decodes the WKB geometry that begins at offset 0 of {@code wkb} (the byte-order byte). The segment is read
     * forward by structure; any trailing bytes are ignored.
     *
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0
     * @return the decoded JTS geometry
     * @throws IllegalStateException when the bytes are not valid WKB (including out-of-bounds reads on truncated input)
     */
    public Geometry read(MemorySegment wkb) {
        try {
            Cursor cursor = new Cursor(wkb, 0L);
            return readGeometry(cursor);
        } catch (RuntimeException e) {
            throw new IllegalStateException("Failed to decode WKB geometry: " + e.getMessage(), e);
        }
    }

    /**
     * Reads one geometry at the cursor's current position. The cursor advances past the byte-order byte, the type code,
     * any EWKB SRID, and every coordinate belonging to the geometry (recursing through containers). An EWKB SRID is
     * stamped on the result via {@link Geometry#setSRID(int)}, which JTS propagates to a collection's members; plain
     * WKB leaves the default SRID of 0.
     */
    private Geometry readGeometry(Cursor cursor) {
        cursor.resetByteOrder();
        int rawType = cursor.readInt();
        Dimensions dims = dimensionsFor(rawType);
        int baseType = baseType(rawType);
        int srid = ((rawType & EWKB_FLAG_SRID) != 0) ? cursor.readInt() : 0;
        Geometry geometry =
                switch (baseType) {
                    case TYPE_POINT -> readPoint(cursor, dims);
                    case TYPE_LINESTRING -> readLineString(cursor, dims);
                    case TYPE_POLYGON -> readPolygon(cursor, dims);
                    case TYPE_MULTIPOINT -> readMultiPoint(cursor);
                    case TYPE_MULTILINESTRING -> readMultiLineString(cursor);
                    case TYPE_MULTIPOLYGON -> readMultiPolygon(cursor);
                    case TYPE_GEOMETRYCOLLECTION -> readGeometryCollection(cursor);
                    default -> throw new IllegalArgumentException("Unsupported WKB geometry type: " + baseType);
                };
        if (srid != 0) {
            stampSrid(geometry, srid);
        }
        return geometry;
    }

    /**
     * Stamps {@code srid} on {@code geometry} and, for a collection, on every member. JTS's {@code setSRID} does not
     * propagate to members, but {@code WKBReader} builds them with a factory that already holds the SRID, hence the
     * whole tree must hold the SRID to match it.
     */
    private static void stampSrid(Geometry geometry, int srid) {
        geometry.setSRID(srid);
        if (geometry instanceof GeometryCollection collection) {
            int count = collection.getNumGeometries();
            for (int i = 0; i < count; i++) {
                stampSrid(collection.getGeometryN(i), srid);
            }
        }
    }

    private Point readPoint(Cursor cursor, Dimensions dims) {
        CoordinateSequence seq = readCoordinates(cursor, 1, dims);
        if (isEmptyPoint(seq)) {
            return geometryFactory.createPoint();
        }
        return geometryFactory.createPoint(seq);
    }

    private LineString readLineString(Cursor cursor, Dimensions dims) {
        int numPoints = cursor.readInt();
        if (numPoints == 0) {
            return geometryFactory.createLineString();
        }
        CoordinateSequence seq = readCoordinates(cursor, numPoints, dims);
        return geometryFactory.createLineString(seq);
    }

    private Polygon readPolygon(Cursor cursor, Dimensions dims) {
        int numRings = cursor.readInt();
        if (numRings == 0) {
            return geometryFactory.createPolygon();
        }
        LinearRing shell = readRing(cursor, dims);
        LinearRing[] holes = new LinearRing[numRings - 1];
        for (int i = 0; i < holes.length; i++) {
            holes[i] = readRing(cursor, dims);
        }
        return geometryFactory.createPolygon(shell, holes);
    }

    private LinearRing readRing(Cursor cursor, Dimensions dims) {
        int numPoints = cursor.readInt();
        CoordinateSequence seq = readCoordinates(cursor, numPoints, dims);
        return geometryFactory.createLinearRing(seq);
    }

    private Geometry readMultiPoint(Cursor cursor) {
        int numElements = cursor.readInt();
        Point[] points = new Point[numElements];
        for (int i = 0; i < numElements; i++) {
            points[i] = (Point) readGeometry(cursor);
        }
        return geometryFactory.createMultiPoint(points);
    }

    private Geometry readMultiLineString(Cursor cursor) {
        int numElements = cursor.readInt();
        LineString[] lines = new LineString[numElements];
        for (int i = 0; i < numElements; i++) {
            lines[i] = (LineString) readGeometry(cursor);
        }
        return geometryFactory.createMultiLineString(lines);
    }

    private Geometry readMultiPolygon(Cursor cursor) {
        int numElements = cursor.readInt();
        Polygon[] polygons = new Polygon[numElements];
        for (int i = 0; i < numElements; i++) {
            polygons[i] = (Polygon) readGeometry(cursor);
        }
        return geometryFactory.createMultiPolygon(polygons);
    }

    private Geometry readGeometryCollection(Cursor cursor) {
        int numElements = cursor.readInt();
        Geometry[] elements = new Geometry[numElements];
        for (int i = 0; i < numElements; i++) {
            elements[i] = readGeometry(cursor);
        }
        return geometryFactory.createGeometryCollection(elements);
    }

    /**
     * Reads {@code numPoints} coordinates of the given dimensions into a packed sequence with one bulk copy. The
     * {@code numPoints * dim} doubles are contiguous in the segment and land directly in the backing array; the cursor
     * advances past every copied byte.
     */
    private CoordinateSequence readCoordinates(Cursor cursor, int numPoints, Dimensions dims) {
        int dimension = dims.dimension();
        double[] packed = new double[numPoints * dimension];
        cursor.copyDoubles(packed);
        return new PackedCoordinateSequence.Double(packed, dimension, dims.measures());
    }

    /** A WKB {@code POINT EMPTY} encodes a single coordinate whose ordinates are all NaN; JTS emits an empty point. */
    private static boolean isEmptyPoint(CoordinateSequence seq) {
        if (seq.size() != 1) {
            return false;
        }
        return Double.isNaN(seq.getX(0)) && Double.isNaN(seq.getY(0));
    }

    /**
     * Whether a type code uses the EWKB encoding. EWKB sets one or more of the three high-bit flags (bits 29-31); ISO
     * type codes are small positive ints that never reach those bits, hence a high-bit-set test distinguishes the two
     * encodings unambiguously.
     */
    private static boolean isEwkb(int rawType) {
        return (rawType & EWKB_FLAG_MASK) != 0;
    }

    private static Dimensions dimensionsFor(int rawType) {
        if (isEwkb(rawType)) {
            boolean hasZ = (rawType & EWKB_FLAG_Z) != 0;
            boolean hasM = (rawType & EWKB_FLAG_M) != 0;
            if (hasZ && hasM) {
                return Dimensions.ZM;
            }
            if (hasZ) {
                return Dimensions.Z;
            }
            if (hasM) {
                return Dimensions.M;
            }
            return Dimensions.XY;
        }
        if (rawType >= ZM_OFFSET) {
            return Dimensions.ZM;
        }
        if (rawType >= M_OFFSET) {
            return Dimensions.M;
        }
        if (rawType >= Z_OFFSET) {
            return Dimensions.Z;
        }
        return Dimensions.XY;
    }

    private static int baseType(int rawType) {
        if (isEwkb(rawType)) {
            return rawType & ~EWKB_FLAG_MASK;
        }
        if (rawType >= ZM_OFFSET) {
            return rawType - ZM_OFFSET;
        }
        if (rawType >= M_OFFSET) {
            return rawType - M_OFFSET;
        }
        if (rawType >= Z_OFFSET) {
            return rawType - Z_OFFSET;
        }
        return rawType;
    }

    /**
     * Cursor over a WKB payload. Tracks the current byte-order context (each contained geometry can flip the order),
     * the underlying segment, and the running read offset. Confined to one {@link #read(MemorySegment)} call.
     */
    private static final class Cursor {

        private final MemorySegment segment;
        private long offset;

        private ValueLayout.OfInt intLayout;
        private ValueLayout.OfDouble doubleLayout;

        Cursor(MemorySegment segment, long offset) {
            this.segment = segment;
            this.offset = offset;
        }

        /** Reads the byte-order byte at the cursor and sets the int/double layouts; called before each geometry. */
        void resetByteOrder() {
            byte order = segment.get(JAVA_BYTE, offset++);
            switch (order) {
                case WKB_LITTLE_ENDIAN -> {
                    intLayout = INT_LE;
                    doubleLayout = DOUBLE_LE;
                }
                case WKB_BIG_ENDIAN -> {
                    intLayout = INT_BE;
                    doubleLayout = DOUBLE_BE;
                }
                default ->
                    throw new IllegalArgumentException(
                            "Invalid WKB byte-order byte: 0x" + Integer.toHexString(order & 0xff));
            }
        }

        int readInt() {
            int value = segment.get(intLayout, offset);
            offset += Integer.BYTES;
            return value;
        }

        /** Bulk-copies {@code dst.length} doubles in the current byte order, advancing past every copied byte. */
        void copyDoubles(double[] dst) {
            MemorySegment.copy(segment, doubleLayout, offset, dst, 0, dst.length);
            offset += (long) dst.length * Double.BYTES;
        }
    }

    /**
     * Whether a WKB type code includes Z and / or M ordinates per coordinate, and the resulting packed
     * {@code (dimension, measures)} pair: XY -> (2, 0), XYZ -> (3, 0), XYM -> (3, 1), XYZM -> (4, 1).
     */
    private enum Dimensions {
        XY(2, 0),
        Z(3, 0),
        M(3, 1),
        ZM(4, 1);

        private final int dimension;
        private final int measures;

        Dimensions(int dimension, int measures) {
            this.dimension = dimension;
            this.measures = measures;
        }

        int dimension() {
            return dimension;
        }

        int measures() {
            return measures;
        }
    }
}
