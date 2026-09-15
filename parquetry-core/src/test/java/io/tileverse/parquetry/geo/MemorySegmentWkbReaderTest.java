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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
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
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.testsupport.WkbCorpus;

/**
 * Correctness check for {@link MemorySegmentWkbReader} against JTS's own {@link WKBReader}. Each fixture geometry is
 * encoded to WKB by {@link WKBWriter} in both byte orders, then decoded by both readers; the results must agree on 2D
 * structure ({@link Geometry#equalsExact}) and, for Z/M fixtures, on every Z/M ordinate (which {@code equalsExact}
 * ignores).
 */
class MemorySegmentWkbReaderTest {

    private static final GeometryFactory PACKED_FACTORY = new GeometryFactory(new PackedCoordinateSequenceFactory());

    private final MemorySegmentWkbReader reader = new MemorySegmentWkbReader(PACKED_FACTORY);

    static Stream<Arguments> corpus() {
        return WkbCorpus.entries().stream().map(entry -> Arguments.of(entry.label(), entry));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("corpus")
    void decodesSameAsJtsWkbReader(String label, WkbCorpus.Entry entry) throws ParseException {
        Geometry expected = new WKBReader(PACKED_FACTORY).read(entry.bytes());
        Geometry actual = reader.read(entry.wkb());

        assertThat(actual.equalsExact(expected))
                .as(
                        "%s: custom decode must equal WKBReader decode (2D); expected=%s actual=%s",
                        label, expected, actual)
                .isTrue();

        assertSameZmOrdinates(label, entry.hasZ(), entry.hasM(), expected, actual);
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
    private static void assertSameZmOrdinates(
            String label, boolean hasZ, boolean hasM, Geometry expected, Geometry actual) {
        if (!hasZ && !hasM) {
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
                if (hasZ) {
                    assertThat(actualSeq.getZ(i))
                            .as("%s: seq %d coord %d Z", label, s, i)
                            .isEqualTo(expectedSeq.getZ(i));
                }
                if (hasM) {
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
