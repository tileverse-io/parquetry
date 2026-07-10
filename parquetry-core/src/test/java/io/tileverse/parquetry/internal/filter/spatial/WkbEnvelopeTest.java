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
package io.tileverse.parquetry.internal.filter.spatial;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ByteOrderValues;

import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.testsupport.Wkb;

class WkbEnvelopeTest {

    @Test
    void pointEnvelopeIsTheSinglePoint() {
        String wkt = "POINT (3 4)";
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt(wkt));

        assertThat(env.minX()).isEqualTo(3.0);
        assertThat(env.maxX()).isEqualTo(3.0);
        assertThat(env.minY()).isEqualTo(4.0);
        assertThat(env.maxY()).isEqualTo(4.0);
        assertThat(env.is3d()).isFalse();

        // Cross-check our 2D answer against JTS's reference envelope: from "we read what we write" to "we read what
        // JTS writes AND we agree with JTS's 2D answer".
        Envelope reference = Wkb.geometry(wkt).getEnvelopeInternal();
        assertThat(env.minX()).isEqualTo(reference.getMinX());
        assertThat(env.maxX()).isEqualTo(reference.getMaxX());
        assertThat(env.minY()).isEqualTo(reference.getMinY());
        assertThat(env.maxY()).isEqualTo(reference.getMaxY());
    }

    @Test
    void lineStringEnvelopeSpansAllVertices() {
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt("LINESTRING (0 0, 10 5, 20 0)"));

        assertThat(env.minX()).isZero();
        assertThat(env.maxX()).isEqualTo(20.0);
        assertThat(env.minY()).isZero();
        assertThat(env.maxY()).isEqualTo(5.0);
    }

    @Test
    void polygonEnvelopeSpansAllVertices() {
        String wkt = """
                POLYGON ((
                  0 0,
                  10 0,
                  10 8,
                  0 8,
                  0 0
                ))
                """;
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt(wkt));

        assertThat(env.minX()).isEqualTo(0.0);
        assertThat(env.maxX()).isEqualTo(10.0);
        assertThat(env.minY()).isEqualTo(0.0);
        assertThat(env.maxY()).isEqualTo(8.0);

        Envelope reference = Wkb.geometry(wkt).getEnvelopeInternal();
        assertThat(env.minX()).isEqualTo(reference.getMinX());
        assertThat(env.maxX()).isEqualTo(reference.getMaxX());
        assertThat(env.minY()).isEqualTo(reference.getMinY());
        assertThat(env.maxY()).isEqualTo(reference.getMaxY());
    }

    @Test
    void multiPointEnvelopeUnionsEveryChild() {
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt("MULTIPOINT ((-5 1), (7 9))"));

        assertThat(env.minX()).isEqualTo(-5.0);
        assertThat(env.maxX()).isEqualTo(7.0);
        assertThat(env.minY()).isEqualTo(1.0);
        assertThat(env.maxY()).isEqualTo(9.0);
    }

    @Test
    void multiLineStringEnvelopeUnionsEveryChild() {
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt("""
                MULTILINESTRING (
                  (0 0, 5 5),
                  (-1 -2, 10 20)
                )
                """));

        assertThat(env.minX()).isEqualTo(-1.0);
        assertThat(env.maxX()).isEqualTo(10.0);
        assertThat(env.minY()).isEqualTo(-2.0);
        assertThat(env.maxY()).isEqualTo(20.0);
    }

    @Test
    void multiPolygonEnvelopeUnionsEveryChild() {
        // Two unit-side squares: SW corner (0,0) side 4 and SW corner (10,10) side 5.
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt("""
                MULTIPOLYGON (
                  ((0 0, 4 0, 4 4, 0 4, 0 0)),
                  ((10 10, 15 10, 15 15, 10 15, 10 10))
                )
                """));

        assertThat(env.minX()).isEqualTo(0.0);
        assertThat(env.maxX()).isEqualTo(15.0);
        assertThat(env.minY()).isEqualTo(0.0);
        assertThat(env.maxY()).isEqualTo(15.0);
    }

    @Test
    void geometryCollectionEnvelopeUnionsEveryMember() {
        Bbox env = WkbEnvelope.compute(Wkb.fromWkt("""
                GEOMETRYCOLLECTION (
                  POINT (100 100),
                  LINESTRING (-3 -4, 2 2)
                )
                """));

        assertThat(env.minX()).isEqualTo(-3.0);
        assertThat(env.maxX()).isEqualTo(100.0);
        assertThat(env.minY()).isEqualTo(-4.0);
        assertThat(env.maxY()).isEqualTo(100.0);
    }

    /**
     * Z ordinates must be parsed-and-skipped under the ISO encoding (PointZ = type code 1001). This fixture stays
     * hand-rolled because JTS's WKBWriter emits EWKB high-bit flags for Z, not the ISO {@code +1000} offset;
     * {@link #ewkbZOrdinatesAreIgnored} exercises the EWKB form via JTS.
     */
    @Test
    void isoZOrdinatesAreIgnored() {
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 1001); // PointZ
        wkb.writeDouble(3.0);
        wkb.writeDouble(4.0);
        wkb.writeDouble(99.0); // Z

        Bbox env = WkbEnvelope.compute(wkb.toSegment());

        assertThat(env.minX()).isEqualTo(3.0);
        assertThat(env.maxX()).isEqualTo(3.0);
        assertThat(env.minY()).isEqualTo(4.0);
        assertThat(env.maxY()).isEqualTo(4.0);
    }

    /**
     * The same PointZ in EWKB encoding (JTS writes the {@code 0x80000001} high-bit flag at output dimension 3, not the
     * ISO 1001 offset). The reader must accept both encodings and ignore Z in the 2D envelope either way.
     */
    @Test
    void ewkbZOrdinatesAreIgnored() {
        MemorySegment wkb = Wkb.fromWktDimension("POINT Z (3 4 99)", 3);
        Bbox env = WkbEnvelope.compute(wkb);

        assertThat(env.minX()).isEqualTo(3.0);
        assertThat(env.maxX()).isEqualTo(3.0);
        assertThat(env.minY()).isEqualTo(4.0);
        assertThat(env.maxY()).isEqualTo(4.0);
    }

    /**
     * An EWKB Point whose SRID flag ({@code 0x20000000}) is set prepends a 4-byte SRID to the coordinates. The reader
     * must recognize the flag, skip the SRID payload, and read the coordinates at the correct offset.
     */
    @Test
    void ewkbPointWithSridSkipsSridAndReadsCoordinates() {
        byte[] bytes = ByteBuffer.allocate(1 + 4 + 4 + 8 + 8 + 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 0x01)
                .putInt(0x80000001 | 0x20000000) // Point + Z + SRID
                .putInt(4326)
                .putDouble(1.0)
                .putDouble(2.0)
                .putDouble(3.0)
                .array();

        Bbox env = WkbEnvelope.compute(MemorySegment.ofArray(bytes).asReadOnly());

        assertThat(env.minX()).isEqualTo(1.0);
        assertThat(env.maxX()).isEqualTo(1.0);
        assertThat(env.minY()).isEqualTo(2.0);
        assertThat(env.maxY()).isEqualTo(2.0);
    }

    @Test
    void bigEndianPointParsesIdenticallyToLittleEndian() {
        MemorySegment wkbBytes = Wkb.fromWkt("POINT (7.5 -3.25)", ByteOrderValues.BIG_ENDIAN);
        Bbox env = WkbEnvelope.compute(wkbBytes);

        assertThat(env.minX()).isEqualTo(7.5);
        assertThat(env.minY()).isEqualTo(-3.25);
    }

    @Test
    void xyWalkVisitsEveryCoordinate() {
        int[] count = {0};
        WkbEnvelope.walk(Wkb.fromWkt("LINESTRING (0 0, 10 5, 20 0)"), (WkbEnvelope.XyVisitor) (x, y) -> {
            count[0]++;
            return true;
        });

        assertThat(count[0]).isEqualTo(3);
    }

    @Test
    void xyWalkStopsEarlyWhenVisitorReturnsFalse() {
        int[] count = {0};
        WkbEnvelope.walk(Wkb.fromWkt("LINESTRING (0 0, 10 5, 20 0)"), (WkbEnvelope.XyVisitor) (x, y) -> {
            count[0]++;
            return false;
        });

        assertThat(count[0]).isEqualTo(1);
    }
}
