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
package io.tileverse.parquetry.testsupport;

import java.lang.foreign.MemorySegment;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ByteOrderValues;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

/**
 * Builds WKB payloads from WKT strings for test fixtures, via JTS. Using JTS as the WKB encoder validates our reader
 * against an independent reference implementation; a bug we share with our own encoder would otherwise slip through
 * both sides of a round-trip test.
 *
 * <p>Output dimension controls how many ordinates JTS writes and how it tags the type code. The 2D default emits a type
 * code with no flags (identical bytes under ISO and EWKB). Output dimension 3 emits EWKB {@code 0x80000000}-flagged Z,
 * and output dimension 4 emits EWKB {@code 0xc0000000}-flagged ZM. JTS does not emit ISO {@code +1000/+2000/+3000} type
 * codes, nor an M-only EWKB flag (its {@code WKBWriter} tags a measured coordinate's third ordinate as Z); tests that
 * need those keep a hand-rolled fixture.
 */
public final class Wkb {

    private static final int DIMENSION_2D = 2;

    private Wkb() {}

    /** Little-endian 2D WKB from a WKT string. */
    public static MemorySegment fromWkt(String wkt) {
        return fromWkt(wkt, ByteOrderValues.LITTLE_ENDIAN);
    }

    /**
     * 2D WKB from a WKT string in the given byte order ({@link ByteOrderValues#LITTLE_ENDIAN} or
     * {@link ByteOrderValues#BIG_ENDIAN}).
     */
    public static MemorySegment fromWkt(String wkt, int byteOrder) {
        Geometry geom = geometry(wkt);
        WKBWriter writer = new WKBWriter(DIMENSION_2D, byteOrder);
        byte[] bytes = writer.write(geom);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    /**
     * Little-endian WKB from a WKT string at the given output dimension: 2 for plain XY, 3 for EWKB-flagged Z (e.g.
     * {@code fromWktDimension("POINT Z (1 2 3)", 3)}), 4 for EWKB-flagged ZM (e.g. {@code fromWktDimension("POINT ZM (1
     * 2 3 4)", 4)}).
     *
     * <p>This is a separate name from {@link #fromWkt(String, int)} because both an output dimension and a JTS byte
     * order are plain {@code int}s; overloading would be ambiguous at the call site.
     */
    public static MemorySegment fromWktDimension(String wkt, int outputDimension) {
        Geometry geom = geometry(wkt);
        WKBWriter writer = new WKBWriter(outputDimension, ByteOrderValues.LITTLE_ENDIAN);
        byte[] bytes = writer.write(geom);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    /** The parsed JTS geometry, exposed for cross-checking our 2D answers against JTS's reference. */
    public static Geometry geometry(String wkt) {
        try {
            return new WKTReader().read(wkt);
        } catch (ParseException e) {
            throw new IllegalArgumentException("invalid WKT: " + wkt, e);
        }
    }
}
