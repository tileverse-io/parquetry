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
package io.tileverse.parquetry.geo;

import java.lang.foreign.MemorySegment;

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

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.internal.wkb.CoordinateRun;
import io.tileverse.parquetry.internal.wkb.Dimensions;
import io.tileverse.parquetry.internal.wkb.WkbCursor;
import io.tileverse.parquetry.internal.wkb.WkbTypeCode;

/**
 * Thread-safe WKB-to-JTS reader that reads directly from a {@link MemorySegment} onto packed coordinate sequences,
 * without {@code WKBReader}, {@code InStream}, or any intermediate {@code byte[]} copy; state-free per call.
 *
 * <p>The reader holds only an immutable {@link GeometryFactory} (default backed by
 * {@link PackedCoordinateSequenceFactory}); all walk state lives in a per-call {@link WkbCursor}, hence one instance is
 * safe to share across threads.
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
     * @throws MalformedFileException when the bytes are not valid WKB (including out-of-bounds reads on truncated
     *     input)
     */
    public Geometry read(MemorySegment wkb) {
        return read(wkb, 0L, wkb.byteSize());
    }

    /**
     * Decodes the WKB geometry in {@code backing} at {@code [offset, offset + length)}. The structure walk reads
     * forward from {@code offset} and every read is bounded by {@code length}: a truncated or length-violating value
     * throws rather than reading past its end into the adjacent bytes of {@code backing}. This lets a caller decode
     * straight from a column's backing window without minting a per-value slice.
     *
     * @param backing the segment holding the WKB, with the byte-order byte at {@code offset}
     * @param offset the byte offset where this geometry's WKB begins
     * @param length the byte length of this geometry's value within {@code backing}
     * @return the decoded JTS geometry
     * @throws MalformedFileException when the bytes are not valid WKB (including reads past the value's length on
     *     truncated input)
     */
    public Geometry read(MemorySegment backing, long offset, long length) {
        try {
            WkbCursor cursor = new WkbCursor(backing, offset, length);
            return readGeometry(cursor);
        } catch (RuntimeException e) {
            throw new MalformedFileException("Failed to decode WKB geometry: " + e.getMessage(), e);
        }
    }

    /**
     * Reads one geometry at the cursor's current position: its header, then its coordinates, recursing through
     * containers. An EWKB SRID is stamped on the result via {@link Geometry#setSRID(int)} and on every member of a
     * collection; plain WKB leaves the default SRID of 0.
     */
    private Geometry readGeometry(WkbCursor cursor) {
        cursor.readHeader();
        int srid = cursor.srid();
        Dimensions dims = cursor.headerDimensions();
        Geometry geometry =
                switch (cursor.baseType()) {
                    case WkbTypeCode.POINT -> readPoint(cursor, dims);
                    case WkbTypeCode.LINESTRING -> readLineString(cursor, dims);
                    case WkbTypeCode.POLYGON -> readPolygon(cursor, dims);
                    case WkbTypeCode.MULTIPOINT -> readMultiPoint(cursor);
                    case WkbTypeCode.MULTILINESTRING -> readMultiLineString(cursor);
                    case WkbTypeCode.MULTIPOLYGON -> readMultiPolygon(cursor);
                    case WkbTypeCode.GEOMETRYCOLLECTION -> readGeometryCollection(cursor);
                    default -> throw new MalformedFileException("Unsupported WKB geometry type: " + cursor.baseType());
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

    private Point readPoint(WkbCursor cursor, Dimensions dims) {
        CoordinateSequence seq = readCoordinates(cursor, 1, dims);
        if (isEmptyPoint(seq)) {
            return geometryFactory.createPoint();
        }
        return geometryFactory.createPoint(seq);
    }

    private LineString readLineString(WkbCursor cursor, Dimensions dims) {
        int numPoints = cursor.readCount(dims.count() * Double.BYTES);
        if (numPoints == 0) {
            return geometryFactory.createLineString();
        }
        CoordinateSequence seq = readCoordinates(cursor, numPoints, dims);
        return geometryFactory.createLineString(seq);
    }

    private Polygon readPolygon(WkbCursor cursor, Dimensions dims) {
        int numRings = cursor.readCount(Integer.BYTES);
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

    private LinearRing readRing(WkbCursor cursor, Dimensions dims) {
        int numPoints = cursor.readCount(dims.count() * Double.BYTES);
        CoordinateSequence seq = readCoordinates(cursor, numPoints, dims);
        return geometryFactory.createLinearRing(seq);
    }

    private Geometry readMultiPoint(WkbCursor cursor) {
        int numElements = cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES);
        Point[] points = new Point[numElements];
        for (int i = 0; i < numElements; i++) {
            points[i] = (Point) readGeometry(cursor);
        }
        return geometryFactory.createMultiPoint(points);
    }

    private Geometry readMultiLineString(WkbCursor cursor) {
        int numElements = cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES);
        LineString[] lines = new LineString[numElements];
        for (int i = 0; i < numElements; i++) {
            lines[i] = (LineString) readGeometry(cursor);
        }
        return geometryFactory.createMultiLineString(lines);
    }

    private Geometry readMultiPolygon(WkbCursor cursor) {
        int numElements = cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES);
        Polygon[] polygons = new Polygon[numElements];
        for (int i = 0; i < numElements; i++) {
            polygons[i] = (Polygon) readGeometry(cursor);
        }
        return geometryFactory.createMultiPolygon(polygons);
    }

    private Geometry readGeometryCollection(WkbCursor cursor) {
        int numElements = cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES);
        Geometry[] elements = new Geometry[numElements];
        for (int i = 0; i < numElements; i++) {
            elements[i] = readGeometry(cursor);
        }
        return geometryFactory.createGeometryCollection(elements);
    }

    /**
     * Reads {@code numPoints} coordinates of the given dimensions into a packed sequence with one bulk copy: the run's
     * doubles are contiguous in the segment and land directly in the array that backs the sequence.
     */
    private CoordinateSequence readCoordinates(WkbCursor cursor, int numPoints, Dimensions dims) {
        CoordinateRun run = cursor.run(numPoints);
        double[] packed = new double[numPoints * dims.count()];
        run.copyTo(packed);
        return new PackedCoordinateSequence.Double(packed, dims.count(), dims.measures());
    }

    /** A WKB {@code POINT EMPTY} encodes a single coordinate whose ordinates are all NaN; JTS emits an empty point. */
    private static boolean isEmptyPoint(CoordinateSequence seq) {
        if (seq.size() != 1) {
            return false;
        }
        return Double.isNaN(seq.getX(0)) && Double.isNaN(seq.getY(0));
    }
}
