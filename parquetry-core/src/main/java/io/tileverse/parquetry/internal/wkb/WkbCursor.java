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
package io.tileverse.parquetry.internal.wkb;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_DOUBLE_UNALIGNED;
import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

import io.tileverse.parquetry.format.MalformedFileException;

/**
 * Walker over one WKB value bounded to {@code [offset, offset + length)} of a backing segment. It decodes each
 * geometry's header (byte order, type, optional EWKB SRID), reads element counts checked against the remaining bytes,
 * and exposes each coordinate run as a {@link CoordinateRun} view. It is the only code that reads WKB coordinates; the
 * JTS reader and the envelope computation are typed walks over it. It never inspects bytes past the value's end: a
 * truncated or count-violating value throws instead of reading adjacent bytes.
 *
 * <p>Every read goes through a {@code static final} layout named literally at its call site, chosen by a branch on the
 * byte-order flag. A {@code MemorySegment.get} compiles to a plain load only when its layout is a compile-time
 * constant; a layout held in a field or a local takes the generic VarHandle path (access-mode resolution, offset
 * computation, enclosing-layout, alignment and liveness checks) on every ordinate, which is what made the previous
 * envelope walk the bottleneck of a whole-world read.
 *
 * <p>Each ordinate accessor reads the segment directly, and the walks built on this cursor stay flat, because the read
 * sits at the bottom of a long chain of inlined frames: the JDK's layout access ends in an alignment check that decides
 * whether the read compiles to a plain load, and one extra frame above the accessor pushes that check past the JIT's
 * inlining depth limit, which turns every ordinate into a call worth about 3 ns per coordinate.
 *
 * <p>One instance walks one value on one thread: it is created per walk and never shared.
 */
public final class WkbCursor implements CoordinateRun {

    /** Smallest on-wire size of one nested geometry: the byte-order byte plus the 4-byte type code. */
    public static final int MIN_GEOMETRY_BYTES = Byte.BYTES + Integer.BYTES;

    private static final ValueLayout.OfInt INT_LE = JAVA_INT_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfInt INT_BE = JAVA_INT_UNALIGNED.withOrder(BIG_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_LE = JAVA_DOUBLE_UNALIGNED.withOrder(LITTLE_ENDIAN);
    private static final ValueLayout.OfDouble DOUBLE_BE = JAVA_DOUBLE_UNALIGNED.withOrder(BIG_ENDIAN);

    private static final byte WKB_BIG_ENDIAN = 0x00;
    private static final byte WKB_LITTLE_ENDIAN = 0x01;

    private final MemorySegment segment;
    private final long limit;
    private long offset;

    private boolean bigEndian;
    private int baseType;
    private Dimensions dimensions = Dimensions.XY;
    private int isoType;
    private int srid;

    private long runOffset;
    private long runStride;
    private int runSize;
    private Dimensions runDimensions = Dimensions.XY;

    /**
     * A cursor over the value at {@code [offset, offset + length)} of {@code segment}, positioned at the byte-order
     * byte of its top-level geometry.
     */
    public WkbCursor(MemorySegment segment, long offset, long length) {
        this.segment = Objects.requireNonNull(segment, "segment");
        this.offset = offset;
        this.limit = offset + length;
    }

    /**
     * Reads the byte-order byte, the type code and the EWKB SRID when flagged, of the geometry at the cursor, and
     * exposes them through {@link #baseType()}, {@link #headerDimensions()}, {@link #isoType()} and {@link #srid()}
     * until the next header is read. A nested geometry overwrites them: a consumer that needs the outer values after
     * descending copies them into locals first.
     *
     * <p>The seven base kinds, {@link WkbTypeCode#POINT} through {@link WkbTypeCode#GEOMETRYCOLLECTION}, are the only
     * accepted types: a well-formed CIRCULARSTRING, CURVEPOLYGON, TIN, TRIANGLE or other geometry whose type code is
     * above 7 is rejected rather than skipped, because this cursor cannot walk its body.
     *
     * @throws MalformedFileException on a byte-order byte other than 0x00/0x01, a base type outside the seven kinds, or
     *     a value too short to hold the header
     */
    public void readHeader() {
        readByteOrder();
        int rawType = readInt();
        baseType = WkbTypeCode.baseType(rawType);
        if (baseType < WkbTypeCode.POINT || baseType > WkbTypeCode.GEOMETRYCOLLECTION) {
            throw new MalformedFileException("Unsupported WKB geometry type: " + baseType);
        }
        dimensions = WkbTypeCode.dimensions(rawType);
        isoType = WkbTypeCode.isoType(rawType);
        srid = WkbTypeCode.hasSrid(rawType) ? readInt() : 0;
    }

    /** The base kind ({@link WkbTypeCode#POINT} .. {@link WkbTypeCode#GEOMETRYCOLLECTION}) of the last header. */
    public int baseType() {
        return baseType;
    }

    /** The ordinates per coordinate declared by the last header. */
    public Dimensions headerDimensions() {
        return dimensions;
    }

    /** The ordinates per coordinate of the current run, as declared by the header that preceded it. */
    @Override
    public Dimensions dimensions() {
        return runDimensions;
    }

    /** The ISO type code of the last header (an EWKB input normalized to its ISO form). */
    public int isoType() {
        return isoType;
    }

    /** The EWKB SRID of the last header, or 0 when the header has none. */
    public int srid() {
        return srid;
    }

    /**
     * Reads a 4-byte element count in the current byte order and rejects one that cannot fit the remaining bytes,
     * {@code minBytesPerElement} being the smallest on-wire size of one element. Bounding the count before any
     * allocation is sized from it stops a garbage count in a malformed value from forcing a huge array.
     *
     * @throws MalformedFileException on a negative count or one whose elements cannot fit the value
     */
    public int readCount(int minBytesPerElement) {
        int count = readInt();
        if (count < 0 || (long) count * minBytesPerElement > remaining()) {
            throw new MalformedFileException(
                    "WKB element count " + count + " exceeds the value's remaining " + remaining() + " byte(s)");
        }
        return count;
    }

    /**
     * Positions the run view over the next {@code pointCount} coordinates of the last header's dimensions and advances
     * the cursor past them. The returned view is this cursor; it stays valid until the next {@code run} call.
     *
     * @throws MalformedFileException when the run does not fit the value
     */
    public CoordinateRun run(int pointCount) {
        if (pointCount < 0) {
            throw new MalformedFileException("WKB coordinate count " + pointCount + " is negative");
        }
        long stride = (long) dimensions.count() * Double.BYTES;
        long bytes = pointCount * stride;
        requireRemaining(bytes);
        runOffset = offset;
        runStride = stride;
        runSize = pointCount;
        runDimensions = dimensions;
        offset += bytes;
        return this;
    }

    @Override
    public int size() {
        return runSize;
    }

    @Override
    public double x(int i) {
        long at = runOffset + i * runStride;
        if (bigEndian) {
            return segment.get(DOUBLE_BE, at);
        }
        return segment.get(DOUBLE_LE, at);
    }

    @Override
    public double y(int i) {
        long at = runOffset + i * runStride + Double.BYTES;
        if (bigEndian) {
            return segment.get(DOUBLE_BE, at);
        }
        return segment.get(DOUBLE_LE, at);
    }

    @Override
    public double z(int i) {
        long at = runOffset + i * runStride + 2L * Double.BYTES;
        if (bigEndian) {
            return segment.get(DOUBLE_BE, at);
        }
        return segment.get(DOUBLE_LE, at);
    }

    @Override
    public double m(int i) {
        long ordinateIndex = runDimensions.hasZ() ? 3L : 2L;
        long at = runOffset + i * runStride + ordinateIndex * Double.BYTES;
        if (bigEndian) {
            return segment.get(DOUBLE_BE, at);
        }
        return segment.get(DOUBLE_LE, at);
    }

    @Override
    public void copyTo(double[] dst) {
        int count = runSize * runDimensions.count();
        if (bigEndian) {
            MemorySegment.copy(segment, DOUBLE_BE, runOffset, dst, 0, count);
            return;
        }
        MemorySegment.copy(segment, DOUBLE_LE, runOffset, dst, 0, count);
    }

    private int readInt() {
        requireRemaining(Integer.BYTES);
        int value;
        if (bigEndian) {
            value = segment.get(INT_BE, offset);
        } else {
            value = segment.get(INT_LE, offset);
        }
        offset += Integer.BYTES;
        return value;
    }

    private void readByteOrder() {
        requireRemaining(Byte.BYTES);
        byte order = segment.get(JAVA_BYTE, offset++);
        switch (order) {
            case WKB_LITTLE_ENDIAN -> bigEndian = false;
            case WKB_BIG_ENDIAN -> bigEndian = true;
            default ->
                throw new MalformedFileException("Invalid WKB byte-order byte: 0x" + Integer.toHexString(order & 0xff));
        }
    }

    private long remaining() {
        return limit - offset;
    }

    private void requireRemaining(long bytes) {
        if (bytes > remaining()) {
            throw new MalformedFileException("WKB value is truncated: need " + bytes
                    + " more byte(s) but the value has " + remaining() + " remaining");
        }
    }
}
