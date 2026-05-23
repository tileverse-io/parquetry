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
package io.tileverse.parquetry.data.write;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.BIG_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.TreeSet;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;

import lombok.NonNull;

/**
 * Per-geometry-column accumulator that walks WKB payloads to expand a running bounding box (X, Y, optional Z, optional
 * M) and to record the set of WKB geometry type codes observed.
 *
 * <p>The WKB format mandates a leading byte-order byte (0x00 big-endian, 0x01 little-endian) followed by a 4-byte type
 * code in the declared byte order; the type code identifies the geometry kind and its coordinate dimensions. This
 * accumulator walks the structure directly through the input {@link MemorySegment} so no intermediate byte array is
 * materialized; no JTS dependency is introduced here.
 *
 * <p>{@link #finish()} returns the assembled {@link GeospatialStatistics}: the bounding box is absent when no
 * coordinates were observed, and the geometry-type set is absent when no payload was supplied. Z and M extents are
 * tracked separately so a stream of mixed 2D / 3D / measured geometries still yields a faithful box.
 */
public final class GeospatialStatisticsAccumulator {

    private static final ValueLayout.OfInt INT_BE = JAVA_INT_UNALIGNED.withOrder(BIG_ENDIAN);
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

    private boolean hasObservation;

    private double xmin = Double.POSITIVE_INFINITY;
    private double ymin = Double.POSITIVE_INFINITY;
    private double xmax = Double.NEGATIVE_INFINITY;
    private double ymax = Double.NEGATIVE_INFINITY;

    private boolean hasZ;
    private double zmin = Double.POSITIVE_INFINITY;
    private double zmax = Double.NEGATIVE_INFINITY;

    private boolean hasM;
    private double mmin = Double.POSITIVE_INFINITY;
    private double mmax = Double.NEGATIVE_INFINITY;

    private final TreeSet<Integer> geospatialTypes = new TreeSet<>();

    /**
     * Walks the WKB payload at {@code wkb}, expanding the running bounding box for every coordinate it carries and
     * recording the geometry's type code. Callers retain ownership of the segment; the accumulator only reads from it.
     */
    public void update(@NonNull MemorySegment wkb) {
        Cursor cursor = new Cursor(wkb, 0L);
        consumeGeometry(cursor);
    }

    /**
     * Returns the assembled {@link GeospatialStatistics} for the current accumulation window. An accumulator that has
     * not been updated reports both fields absent. Calling this method does not modify the accumulator -- use
     * {@link #reset()} to start a new window.
     */
    public GeospatialStatistics finish() {
        Optional<BoundingBox> bbox = hasObservation ? Optional.of(buildBoundingBox()) : Optional.empty();
        Optional<List<Integer>> types =
                geospatialTypes.isEmpty() ? Optional.empty() : Optional.of(new ArrayList<>(geospatialTypes));
        return new GeospatialStatistics(bbox, types);
    }

    /** Clears every accumulated coordinate and type code; the accumulator behaves as freshly constructed. */
    public void reset() {
        hasObservation = false;
        xmin = Double.POSITIVE_INFINITY;
        ymin = Double.POSITIVE_INFINITY;
        xmax = Double.NEGATIVE_INFINITY;
        ymax = Double.NEGATIVE_INFINITY;
        hasZ = false;
        zmin = Double.POSITIVE_INFINITY;
        zmax = Double.NEGATIVE_INFINITY;
        hasM = false;
        mmin = Double.POSITIVE_INFINITY;
        mmax = Double.NEGATIVE_INFINITY;
        geospatialTypes.clear();
    }

    private BoundingBox buildBoundingBox() {
        return new BoundingBox(
                xmin,
                xmax,
                ymin,
                ymax,
                hasZ ? OptionalDouble.of(zmin) : OptionalDouble.empty(),
                hasZ ? OptionalDouble.of(zmax) : OptionalDouble.empty(),
                hasM ? OptionalDouble.of(mmin) : OptionalDouble.empty(),
                hasM ? OptionalDouble.of(mmax) : OptionalDouble.empty());
    }

    /**
     * Consumes one geometry starting at the cursor's current position. The cursor advances past the byte-order byte,
     * the type code, and every coordinate belonging to the geometry (recursing through containers).
     */
    private void consumeGeometry(Cursor cursor) {
        cursor.byteOrder = readByteOrder(cursor);
        int rawType = cursor.readInt();
        Dimensions dims = dimensionsFor(rawType);
        int baseType = baseType(rawType);
        geospatialTypes.add(rawType);
        switch (baseType) {
            case TYPE_POINT -> consumePoint(cursor, dims);
            case TYPE_LINESTRING -> consumeLineString(cursor, dims);
            case TYPE_POLYGON -> consumePolygon(cursor, dims);
            case TYPE_MULTIPOINT, TYPE_MULTILINESTRING, TYPE_MULTIPOLYGON, TYPE_GEOMETRYCOLLECTION ->
                consumeCollection(cursor);
            default -> {
                /* unknown case; coordinate walk skipped, type code already recorded */
            }
        }
    }

    private void consumePoint(Cursor cursor, Dimensions dims) {
        consumeCoordinate(cursor, dims);
    }

    private void consumeLineString(Cursor cursor, Dimensions dims) {
        int numPoints = cursor.readInt();
        for (int i = 0; i < numPoints; i++) {
            consumeCoordinate(cursor, dims);
        }
    }

    private void consumePolygon(Cursor cursor, Dimensions dims) {
        int numRings = cursor.readInt();
        for (int i = 0; i < numRings; i++) {
            int numPoints = cursor.readInt();
            for (int j = 0; j < numPoints; j++) {
                consumeCoordinate(cursor, dims);
            }
        }
    }

    /**
     * Walks a collection (MULTIPOINT / MULTILINESTRING / MULTIPOLYGON / GEOMETRYCOLLECTION). Every element carries its
     * own byte-order byte and type code, so we recurse from the top of {@link #consumeGeometry}.
     */
    private void consumeCollection(Cursor cursor) {
        int numElements = cursor.readInt();
        for (int i = 0; i < numElements; i++) {
            consumeGeometry(cursor);
        }
    }

    private void consumeCoordinate(Cursor cursor, Dimensions dims) {
        double x = cursor.readDouble();
        double y = cursor.readDouble();
        expandXY(x, y);
        if (dims.hasZ) {
            expandZ(cursor.readDouble());
        }
        if (dims.hasM) {
            expandM(cursor.readDouble());
        }
    }

    private void expandXY(double x, double y) {
        hasObservation = true;
        if (x < xmin) {
            xmin = x;
        }
        if (x > xmax) {
            xmax = x;
        }
        if (y < ymin) {
            ymin = y;
        }
        if (y > ymax) {
            ymax = y;
        }
    }

    private void expandZ(double z) {
        hasZ = true;
        if (z < zmin) {
            zmin = z;
        }
        if (z > zmax) {
            zmax = z;
        }
    }

    private void expandM(double m) {
        hasM = true;
        if (m < mmin) {
            mmin = m;
        }
        if (m > mmax) {
            mmax = m;
        }
    }

    private static byte readByteOrder(Cursor cursor) {
        byte order = cursor.segment.get(JAVA_BYTE, cursor.offset);
        cursor.offset += 1;
        if (order != WKB_BIG_ENDIAN && order != WKB_LITTLE_ENDIAN) {
            throw new ParquetWriteException("Invalid WKB byte-order byte: 0x" + Integer.toHexString(order & 0xff));
        }
        return order;
    }

    private static Dimensions dimensionsFor(int rawType) {
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
     * the underlying segment, and the running read offset.
     */
    private static final class Cursor {

        private final MemorySegment segment;
        private long offset;
        private byte byteOrder = WKB_LITTLE_ENDIAN;

        Cursor(MemorySegment segment, long offset) {
            this.segment = segment;
            this.offset = offset;
        }

        int readInt() {
            int value = byteOrder == WKB_LITTLE_ENDIAN ? segment.get(INT32, offset) : segment.get(INT_BE, offset);
            offset += 4;
            return value;
        }

        double readDouble() {
            double value =
                    byteOrder == WKB_LITTLE_ENDIAN ? segment.get(DOUBLE, offset) : segment.get(DOUBLE_BE, offset);
            offset += 8;
            return value;
        }
    }

    /**
     * Whether a WKB type code carries Z and / or M ordinates per coordinate. The +1000 / +2000 / +3000 type-code
     * prefixes signal Z, M, and ZM variants of the seven base geometry kinds.
     */
    private enum Dimensions {
        XY(false, false),
        Z(true, false),
        M(false, true),
        ZM(true, true);

        private final boolean hasZ;
        private final boolean hasM;

        Dimensions(boolean hasZ, boolean hasM) {
            this.hasZ = hasZ;
            this.hasM = hasM;
        }
    }
}
