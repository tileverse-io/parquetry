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
package io.tileverse.parquetry.internal.filter.spatial;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.BIG_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.format.ParquetFormatException;

/**
 * Structural walk over a WKB (Well-Known Binary) geometry payload. This is the single WKB parser shared by the write
 * side (which expands bounding-box statistics) and the read side (which computes a 2D envelope).
 *
 * <p>The WKB format mandates a leading byte-order byte (0x00 big-endian, 0x01 little-endian) followed by a 4-byte type
 * code in the declared byte order; the type code identifies the geometry kind and its coordinate dimensions. The walk
 * reads directly from the input {@link MemorySegment} without materializing an intermediate byte array, and introduces
 * no JTS dependency.
 *
 * <p>Both type-code encodings are accepted on input: ISO (Z/M/ZM signalled by the {@code +1000}/{@code +2000}/
 * {@code +3000} offsets) and EWKB (Z/M signalled by the {@code 0x80000000}/{@code 0x40000000} high-bit flags, with an
 * optional {@code 0x20000000} SRID flag whose 4-byte payload is skipped). Whichever encoding the input uses, the type
 * code reported to a {@link Visitor} is normalized to its ISO form, which is what the parquet-format
 * {@code geospatial_types} statistic mandates.
 *
 * <p>Every entry point reads the geometry beginning at offset 0 of the input segment (the byte-order byte) and walks
 * forward by structure; it never inspects the segment's size, and any bytes past the geometry are left untouched. The
 * read path supplies an exact, read-only per-row slice -- one geometry's WKB -- which satisfies this.
 */
public final class WkbEnvelope {

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

    private static final int EWKB_FLAG_Z = 0x80000000;
    private static final int EWKB_FLAG_M = 0x40000000;
    private static final int EWKB_FLAG_SRID = 0x20000000;
    private static final int EWKB_FLAG_MASK = EWKB_FLAG_Z | EWKB_FLAG_M | EWKB_FLAG_SRID;

    private WkbEnvelope() {}

    /**
     * Receives the geometry data of a WKB payload during a structural walk. The parser reports each geometry's raw type
     * code (via {@link #geometryType}) before streaming that geometry's coordinates.
     */
    public interface Visitor {

        /**
         * The ISO WKB type code (base kind plus any {@code +1000}/{@code +2000}/{@code +3000} Z/M offset) of a geometry
         * about to be traversed. An EWKB-flagged input is reported in its equivalent ISO form.
         */
        default void geometryType(int isoType) {}

        /**
         * Receives one coordinate. {@code z} and {@code m} are meaningful only when {@code hasZ} / {@code hasM} are
         * true. Return {@code false} to stop the walk immediately.
         */
        boolean coordinate(double x, double y, double z, double m, boolean hasZ, boolean hasM);
    }

    /** A 2D coordinate sink; return {@code false} to stop the walk early. */
    @FunctionalInterface
    public interface XyVisitor {
        boolean accept(double x, double y);
    }

    /**
     * Walks the WKB geometry, driving {@code visitor}; stops early when a coordinate callback returns false.
     *
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0; read forward by structure (any trailing
     *     bytes are ignored)
     * @param visitor receives each geometry's type code and coordinates
     */
    public static void walk(MemorySegment wkb, Visitor visitor) {
        Cursor cursor = new Cursor(wkb, 0L);
        consumeGeometry(cursor, visitor);
    }

    /**
     * Walks every 2D coordinate, ignoring Z/M; stops early when {@code visitor} returns false.
     *
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0; read forward by structure (any trailing
     *     bytes are ignored)
     * @param visitor receives each 2D coordinate
     */
    public static void walk(MemorySegment wkb, XyVisitor visitor) {
        walk(wkb, (x, y, z, m, hasZ, hasM) -> visitor.accept(x, y));
    }

    /**
     * Computes the full 2D envelope of the WKB geometry without building an engine geometry; Z and M are ignored. The
     * spatial decimation gate uses this to bound a row's geometry directly from its WKB.
     *
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0; read forward by structure (any trailing
     *     bytes are ignored)
     * @return the 2D bounding box spanning every coordinate
     */
    public static Bbox compute(MemorySegment wkb) {
        EnvelopeVisitor visitor = new EnvelopeVisitor();
        walk(wkb, visitor);
        return visitor.toBbox();
    }

    /**
     * Decides a bbox relation between the WKB geometry and the predicate's query box, stopping the coordinate walk the
     * moment the answer is fixed.
     *
     * <p>The three open-ended relations exploit the running 2D envelope only ever widening as the walk proceeds: each
     * overlap or enclosure condition is monotone, hence once it holds it stays true, and the first escaping vertex of a
     * coveredBy test fixes the answer. Equality needs the exact final extent, hence it walks to completion.
     *
     * @param p the spatial relation and its query box
     * @param wkb one geometry's WKB, with the byte-order byte at offset 0; read forward by structure (any trailing
     *     bytes are ignored)
     * @return whether the geometry's 2D bbox satisfies the relation
     */
    public static boolean matches(Predicate.Spatial p, MemorySegment wkb) {
        Bbox q = p.bbox();
        return switch (p) {
            case Predicate.Spatial.BboxIntersects _ -> intersectsShortCircuit(wkb, q);
            case Predicate.Spatial.BboxContains _ -> containsShortCircuit(wkb, q);
            case Predicate.Spatial.BboxCoveredBy _ -> coveredByShortCircuit(wkb, q);
            case Predicate.Spatial.BboxEquals _ -> compute(wkb).sameBox2d(q);
        };
    }

    /**
     * True when the geometry's 2D bbox overlaps {@code q}. The walk stops at the first coordinate whose update makes
     * all four overlap conditions hold.
     */
    private static boolean intersectsShortCircuit(MemorySegment wkb, Bbox q) {
        IntersectsVisitor visitor = new IntersectsVisitor(q);
        walk(wkb, visitor);
        return visitor.decided;
    }

    /**
     * True when the geometry's 2D bbox encloses {@code q}. The walk stops at the first coordinate whose update makes
     * the running envelope cover {@code q} on all four edges.
     */
    private static boolean containsShortCircuit(MemorySegment wkb, Bbox q) {
        ContainsVisitor visitor = new ContainsVisitor(q);
        walk(wkb, visitor);
        return visitor.decided;
    }

    /**
     * True when at least one real vertex exists and every vertex lies inside {@code q}. The walk stops at the first
     * escaping vertex, which fixes the answer as false; completing the walk with no escape and at least one real vertex
     * fixes it as true. An empty geometry has no real vertex, hence it is covered by nothing.
     */
    private static boolean coveredByShortCircuit(MemorySegment wkb, Bbox q) {
        CoveredByVisitor visitor = new CoveredByVisitor(q);
        walk(wkb, visitor);
        return visitor.coveredBy();
    }

    /**
     * Consumes one geometry starting at the cursor's current position. The cursor advances past the byte-order byte,
     * the type code, and every coordinate belonging to the geometry (recursing through containers). Returns
     * {@code false} when the visitor requested an early stop.
     */
    private static boolean consumeGeometry(Cursor cursor, Visitor visitor) {
        cursor.resetByteOrder();
        final int rawType = cursor.readInt();
        Dimensions dims = dimensionsFor(rawType);
        int baseType = baseType(rawType);
        if ((rawType & EWKB_FLAG_SRID) != 0) {
            cursor.readInt(); // EWKB embeds a 4-byte SRID after the type code; bbox computation ignores it
        }
        visitor.geometryType(toIsoType(rawType));
        return switch (baseType) {
            case TYPE_POINT -> consumePoint(cursor, dims, visitor);
            case TYPE_LINESTRING -> consumeLineString(cursor, dims, visitor);
            case TYPE_POLYGON -> consumePolygon(cursor, dims, visitor);
            case TYPE_MULTIPOINT, TYPE_MULTILINESTRING, TYPE_MULTIPOLYGON, TYPE_GEOMETRYCOLLECTION ->
                consumeCollection(cursor, visitor);
            default -> true;
        };
    }

    private static boolean consumePoint(Cursor cursor, Dimensions dims, Visitor visitor) {
        return consumeCoordinate(cursor, dims, visitor);
    }

    private static boolean consumeLineString(Cursor cursor, Dimensions dims, Visitor visitor) {
        int numPoints = cursor.readInt();
        for (int i = 0; i < numPoints; i++) {
            if (!consumeCoordinate(cursor, dims, visitor)) {
                return false;
            }
        }
        return true;
    }

    private static boolean consumePolygon(Cursor cursor, Dimensions dims, Visitor visitor) {
        int numRings = cursor.readInt();
        for (int i = 0; i < numRings; i++) {
            int numPoints = cursor.readInt();
            for (int j = 0; j < numPoints; j++) {
                if (!consumeCoordinate(cursor, dims, visitor)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Walks a collection (MULTIPOINT / MULTILINESTRING / MULTIPOLYGON / GEOMETRYCOLLECTION). Every element has its own
     * byte-order byte and type code; each one recurses from the top of {@link #consumeGeometry}.
     */
    private static boolean consumeCollection(Cursor cursor, Visitor visitor) {
        int numElements = cursor.readInt();
        for (int i = 0; i < numElements; i++) {
            if (!consumeGeometry(cursor, visitor)) {
                return false;
            }
        }
        return true;
    }

    private static boolean consumeCoordinate(Cursor cursor, Dimensions dims, Visitor visitor) {
        double x = cursor.readDouble();
        double y = cursor.readDouble();
        double z = Double.NaN;
        double m = Double.NaN;
        if (dims.hasZ) {
            z = cursor.readDouble();
        }
        if (dims.hasM) {
            m = cursor.readDouble();
        }
        return visitor.coordinate(x, y, z, m, dims.hasZ, dims.hasM);
    }

    /**
     * Whether a type code uses the EWKB encoding. EWKB sets one or more of the three high-bit flags (bits 29-31); ISO
     * type codes are small positive ints (a few thousand at most) that never reach those bits, hence a high-bit-set
     * test distinguishes the two encodings unambiguously.
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
     * Returns the ISO form of a WKB type code (Z/M/ZM via the {@code +1000}/{@code +2000}/{@code +3000} offsets the
     * parquet-format {@code geospatial_types} statistic mandates). An EWKB-flagged input is reported to downstream
     * consumers in the same form as an ISO input.
     */
    private static int toIsoType(int rawType) {
        if (!isEwkb(rawType)) {
            return rawType;
        }
        int base = baseType(rawType);
        return switch (dimensionsFor(rawType)) {
            case XY -> base;
            case Z -> base + Z_OFFSET;
            case M -> base + M_OFFSET;
            case ZM -> base + ZM_OFFSET;
        };
    }

    /** Running 2D min/max envelope; the per-coordinate widen logic shared by the envelope-based visitors. */
    private abstract static class RunningEnvelope {
        double minX = Double.POSITIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;

        final void widen(double x, double y) {
            if (x < minX) {
                minX = x;
            }
            if (y < minY) {
                minY = y;
            }
            if (x > maxX) {
                maxX = x;
            }
            if (y > maxY) {
                maxY = y;
            }
        }
    }

    /** Accumulates the full 2D envelope; ignores Z/M. */
    private static final class EnvelopeVisitor extends RunningEnvelope implements Visitor {
        @Override
        public boolean coordinate(double x, double y, double z, double m, boolean hasZ, boolean hasM) {
            widen(x, y);
            return true;
        }

        Bbox toBbox() {
            return Bbox.of2d(minX, minY, maxX, maxY);
        }
    }

    /** Stops true once the running envelope overlaps the query box on all four edges. */
    private static final class IntersectsVisitor extends RunningEnvelope implements Visitor {
        private final Bbox q;
        boolean decided;

        IntersectsVisitor(Bbox q) {
            this.q = q;
        }

        @Override
        public boolean coordinate(double x, double y, double z, double m, boolean hasZ, boolean hasM) {
            widen(x, y);
            if (minX <= q.maxX() && maxX >= q.minX() && minY <= q.maxY() && maxY >= q.minY()) {
                decided = true;
                return false;
            }
            return true;
        }
    }

    /** Stops true once the running envelope encloses the query box on all four edges. */
    private static final class ContainsVisitor extends RunningEnvelope implements Visitor {
        private final Bbox q;
        boolean decided;

        ContainsVisitor(Bbox q) {
            this.q = q;
        }

        @Override
        public boolean coordinate(double x, double y, double z, double m, boolean hasZ, boolean hasM) {
            widen(x, y);
            if (minX <= q.minX() && maxX >= q.maxX() && minY <= q.minY() && maxY >= q.maxY()) {
                decided = true;
                return false;
            }
            return true;
        }
    }

    /**
     * True when at least one real vertex exists and none escapes the query box. Non-finite ordinates are not real
     * vertices.
     */
    private static final class CoveredByVisitor implements Visitor {
        private final Bbox q;
        private boolean sawVertex;
        private boolean escaped;

        CoveredByVisitor(Bbox q) {
            this.q = q;
        }

        @Override
        public boolean coordinate(double x, double y, double z, double m, boolean hasZ, boolean hasM) {
            if (Double.isNaN(x) || Double.isNaN(y)) {
                return true;
            }
            sawVertex = true;
            if (x < q.minX() || x > q.maxX() || y < q.minY() || y > q.maxY()) {
                escaped = true;
                return false;
            }
            return true;
        }

        boolean coveredBy() {
            return sawVertex && !escaped;
        }
    }

    /**
     * Cursor over a WKB payload. Tracks the current byte-order context (each contained geometry can flip the order),
     * the underlying segment, and the running read offset.
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

        /** Must be called before reading each geometry */
        void resetByteOrder() {
            final byte order = segment.get(JAVA_BYTE, offset++);
            switch (order) {
                case WKB_LITTLE_ENDIAN -> {
                    intLayout = INT32;
                    doubleLayout = DOUBLE;
                }
                case WKB_BIG_ENDIAN -> {
                    intLayout = INT_BE;
                    doubleLayout = DOUBLE_BE;
                }
                default -> {
                    throw new ParquetFormatException(
                            "Invalid WKB byte-order byte: 0x" + Integer.toHexString(order & 0xff));
                }
            }
        }

        int readInt() {
            int value = segment.get(intLayout, offset);
            offset += 4;
            return value;
        }

        double readDouble() {
            double value = segment.get(doubleLayout, offset);
            offset += 8;
            return value;
        }
    }

    /**
     * Whether a WKB type code includes Z and / or M ordinates per coordinate. ISO encoding signals Z, M, and ZM forms
     * of the seven base geometry kinds with the {@code +1000}/{@code +2000}/{@code +3000} type-code offsets; EWKB
     * encoding signals them with the {@code 0x80000000} (Z) and {@code 0x40000000} (M) high-bit flags.
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
