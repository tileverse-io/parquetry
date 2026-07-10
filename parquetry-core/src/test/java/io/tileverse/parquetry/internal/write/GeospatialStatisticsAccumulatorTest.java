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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;
import org.locationtech.jts.io.ByteOrderValues;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;
import io.tileverse.parquetry.testsupport.Wkb;

class GeospatialStatisticsAccumulatorTest {

    @Test
    void emptyAccumulatorReportsBothFieldsAbsent() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();

        GeospatialStatistics stats = acc.finish();

        assertThat(stats.bbox()).as("bbox").isEmpty();
        assertThat(stats.geospatialTypes()).as("types").isEmpty();
    }

    @Test
    void singlePointExpandsTo1by1Bbox() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT (1 2)"));

        GeospatialStatistics stats = acc.finish();

        BoundingBox bbox = stats.bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(1.0);
        assertThat(bbox.xmax()).isEqualTo(1.0);
        assertThat(bbox.ymin()).isEqualTo(2.0);
        assertThat(bbox.ymax()).isEqualTo(2.0);
        assertThat(bbox.zmin()).isEmpty();
        assertThat(bbox.mmin()).isEmpty();
        assertThat(stats.geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(1));
    }

    @Test
    void lineStringExpandsBboxThroughEveryVertex() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("LINESTRING (0 0, 10 5, 20 0)"));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isZero();
        assertThat(bbox.xmax()).isEqualTo(20.0);
        assertThat(bbox.ymin()).isZero();
        assertThat(bbox.ymax()).isEqualTo(5.0);
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(2));
    }

    @Test
    void polygonWalksOuterRingVertices() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("""
                POLYGON ((
                  0 0,
                  10 0,
                  10 10,
                  0 0
                ))
                """));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isZero();
        assertThat(bbox.xmax()).isEqualTo(10.0);
        assertThat(bbox.ymin()).isZero();
        assertThat(bbox.ymax()).isEqualTo(10.0);
    }

    @Test
    void multiLineStringWalksEveryChildVertex() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("""
                MULTILINESTRING (
                  (0 0, 5 5),
                  (-1 -2, 10 20)
                )
                """));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(-1.0);
        assertThat(bbox.xmax()).isEqualTo(10.0);
        assertThat(bbox.ymin()).isEqualTo(-2.0);
        assertThat(bbox.ymax()).isEqualTo(20.0);
    }

    @Test
    void mixedPointAndPolygonUnionsCoordsAndRecordsBothTypes() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT (50 50)"));
        acc.update(Wkb.fromWkt("""
                POLYGON ((
                  0 0,
                  10 0,
                  10 10,
                  0 0
                ))
                """));

        GeospatialStatistics stats = acc.finish();
        BoundingBox bbox = stats.bbox().orElseThrow();
        assertThat(bbox.xmin()).isZero();
        assertThat(bbox.xmax()).isEqualTo(50.0);
        assertThat(bbox.ymin()).isZero();
        assertThat(bbox.ymax()).isEqualTo(50.0);
        assertThat(stats.geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(1, 3));
    }

    /**
     * ISO PointZ (type code 1001), hand-rolled because JTS's WKBWriter encodes Z via EWKB high-bit flags rather than
     * the ISO {@code +1000} offset. The companion {@link #ewkbPointZRecordsZmin} feeds the EWKB form and must record
     * the same ISO type code, proving input-encoding normalization.
     */
    @Test
    void isoPointZRecordsZmin() {
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 1001);
        wkb.writeDouble(1.0);
        wkb.writeDouble(2.0);
        wkb.writeDouble(3.0);

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.zmin()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.zmax()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.mmin()).isEmpty();
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(1001));
    }

    /**
     * EWKB PointZ via JTS ({@code 0x80000001} high-bit flag at output dimension 3). The accumulator must normalize the
     * flagged type code to the ISO form (1001) the parquet-format {@code geospatial_types} statistic mandates, and read
     * the Z extent through to zmin/zmax.
     */
    @Test
    void ewkbPointZRecordsZmin() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWktDimension("POINT Z (1 2 3)", 3));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.zmin()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.zmax()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.mmin()).isEmpty();
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(1001));
    }

    /** ISO PointM (type code 2001); see {@link #isoPointZRecordsZmin} for why JTS cannot produce this fixture. */
    @Test
    void isoPointMRecordsMminAndLeavesZEmpty() {
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 2001);
        wkb.writeDouble(1.0);
        wkb.writeDouble(2.0);
        wkb.writeDouble(99.0);

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.zmin()).isEmpty();
        assertThat(bbox.zmax()).isEmpty();
        assertThat(bbox.mmin()).isEqualTo(OptionalDouble.of(99.0));
        assertThat(bbox.mmax()).isEqualTo(OptionalDouble.of(99.0));
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(2001));
    }

    /**
     * EWKB PointM via the {@code 0x40000001} M flag. This EWKB fixture is hand-rolled because JTS's WKBWriter cannot
     * produce an M-only flag (it tags a measured coordinate's third ordinate as Z). The accumulator must normalize to
     * the ISO type code (2001) and read the third ordinate as M, leaving Z empty.
     */
    @Test
    void ewkbPointMRecordsMmin() {
        byte[] bytes = ByteBuffer.allocate(1 + 4 + 8 + 8 + 8)
                .order(ByteOrder.LITTLE_ENDIAN)
                .put((byte) 0x01)
                .putInt(0x40000001) // Point + M
                .putDouble(1.0)
                .putDouble(2.0)
                .putDouble(99.0)
                .array();

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(MemorySegment.ofArray(bytes).asReadOnly());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.zmin()).isEmpty();
        assertThat(bbox.zmax()).isEmpty();
        assertThat(bbox.mmin()).isEqualTo(OptionalDouble.of(99.0));
        assertThat(bbox.mmax()).isEqualTo(OptionalDouble.of(99.0));
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(2001));
    }

    /** ISO PointZM (type code 3001); see {@link #isoPointZRecordsZmin} for why JTS cannot produce this fixture. */
    @Test
    void isoPointZmRecordsBothZAndM() {
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 3001);
        wkb.writeDouble(1.0);
        wkb.writeDouble(2.0);
        wkb.writeDouble(3.0);
        wkb.writeDouble(4.0);

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.zmin()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.zmax()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.mmin()).isEqualTo(OptionalDouble.of(4.0));
        assertThat(bbox.mmax()).isEqualTo(OptionalDouble.of(4.0));
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(3001));
    }

    /**
     * EWKB PointZM via JTS ({@code 0xc0000001} Z+M flags at output dimension 4). The accumulator must normalize to the
     * ISO type code (3001) and read both the Z and M extents.
     */
    @Test
    void ewkbPointZmRecordsBothZAndM() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWktDimension("POINT ZM (1 2 3 4)", 4));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.zmin()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.zmax()).isEqualTo(OptionalDouble.of(3.0));
        assertThat(bbox.mmin()).isEqualTo(OptionalDouble.of(4.0));
        assertThat(bbox.mmax()).isEqualTo(OptionalDouble.of(4.0));
        assertThat(acc.finish().geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(3001));
    }

    /**
     * Input encoding must not leak into the written statistics: an ISO PointZ and an EWKB PointZ over the same
     * coordinates produce identical {@code geospatial_types} (both 1001) and identical bbox extents.
     */
    @Test
    void isoAndEwkbPointZProduceIdenticalStatistics() {
        WkbWriter iso = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 1001);
        iso.writeDouble(1.0);
        iso.writeDouble(2.0);
        iso.writeDouble(3.0);

        GeospatialStatisticsAccumulator isoAcc = new GeospatialStatisticsAccumulator();
        isoAcc.update(iso.toSegment());
        GeospatialStatistics isoStats = isoAcc.finish();

        GeospatialStatisticsAccumulator ewkbAcc = new GeospatialStatisticsAccumulator();
        ewkbAcc.update(Wkb.fromWktDimension("POINT Z (1 2 3)", 3));
        GeospatialStatistics ewkbStats = ewkbAcc.finish();

        assertThat(ewkbStats.geospatialTypes()).isEqualTo(isoStats.geospatialTypes());
        assertThat(ewkbStats.bbox()).isEqualTo(isoStats.bbox());
        assertThat(ewkbStats.geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(1001));
    }

    @Test
    void bigEndianPointParsesIdenticallyToLittleEndian() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT (7.5 -3.25)", ByteOrderValues.BIG_ENDIAN));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(7.5);
        assertThat(bbox.ymin()).isEqualTo(-3.25);
    }

    @Test
    void emptyPointRecordsNoBboxButRecordsItsType() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT EMPTY"));

        GeospatialStatistics stats = acc.finish();
        assertThat(stats.bbox()).as("an empty point contributes no coordinates").isEmpty();
        assertThat(stats.geospatialTypes())
                .as("an empty point is still a Point (type 1)")
                .hasValueSatisfying(types -> assertThat(types).contains(1));
    }

    @Test
    void emptyPointDoesNotCorruptTheBboxOfRealGeometries() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT EMPTY"));
        acc.update(Wkb.fromWkt("POINT (5 5)"));

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(5.0);
        assertThat(bbox.xmax()).isEqualTo(5.0);
        assertThat(bbox.ymin()).isEqualTo(5.0);
        assertThat(bbox.ymax()).isEqualTo(5.0);
    }

    @Test
    void allEmptyGeometriesProduceNoBbox() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT EMPTY"));
        acc.update(Wkb.fromWkt("LINESTRING EMPTY"));

        assertThat(acc.finish().bbox()).isEmpty();
    }

    @Test
    void resetClearsBboxAndTypeSet() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(Wkb.fromWkt("POINT (1 2)"));
        acc.reset();

        GeospatialStatistics stats = acc.finish();
        assertThat(stats.bbox()).isEmpty();
        assertThat(stats.geospatialTypes()).isEmpty();
    }

    /**
     * Minimal hand-rolled WKB builder, retained only to produce ISO PointZ / PointM / PointZM fixtures (type codes 1001
     * / 2001 / 3001). JTS's WKBWriter encodes Z/M via EWKB high-bit flags and cannot emit the ISO {@code +1000}
     * type-code offsets the reader and accumulator dispatch on.
     */
    private static final class WkbWriter {

        private final ByteBuffer buffer;

        WkbWriter(ByteOrder order, int wkbType) {
            this.buffer = ByteBuffer.allocate(64).order(order);
            byte orderByte = (byte) (order == ByteOrder.LITTLE_ENDIAN ? 0x01 : 0x00);
            buffer.put(orderByte);
            buffer.putInt(wkbType);
        }

        void writeDouble(double value) {
            buffer.putDouble(value);
        }

        MemorySegment toSegment() {
            byte[] copy = new byte[buffer.position()];
            buffer.flip();
            buffer.get(copy);
            return MemorySegment.ofArray(copy).asReadOnly();
        }
    }
}
