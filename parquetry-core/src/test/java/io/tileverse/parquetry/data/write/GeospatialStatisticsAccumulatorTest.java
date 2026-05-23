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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.OptionalDouble;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.GeospatialStatistics;

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
        acc.update(point2D(1.0, 2.0));

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
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 2);
        wkb.writeInt(3); // numPoints
        wkb.writeDouble(0.0);
        wkb.writeDouble(0.0);
        wkb.writeDouble(10.0);
        wkb.writeDouble(5.0);
        wkb.writeDouble(20.0);
        wkb.writeDouble(0.0);

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

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
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 3);
        wkb.writeInt(1); // numRings
        wkb.writeInt(4); // numPoints in outer ring
        wkb.writeDouble(0.0);
        wkb.writeDouble(0.0);
        wkb.writeDouble(10.0);
        wkb.writeDouble(0.0);
        wkb.writeDouble(10.0);
        wkb.writeDouble(10.0);
        wkb.writeDouble(0.0);
        wkb.writeDouble(0.0);

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isZero();
        assertThat(bbox.xmax()).isEqualTo(10.0);
        assertThat(bbox.ymin()).isZero();
        assertThat(bbox.ymax()).isEqualTo(10.0);
    }

    @Test
    void multiLineStringWalksEveryChildVertex() {
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 5);
        wkb.writeInt(2); // numLineStrings
        wkb.writeChildLineString(ByteOrder.LITTLE_ENDIAN, new double[] {0.0, 0.0, 5.0, 5.0});
        wkb.writeChildLineString(ByteOrder.LITTLE_ENDIAN, new double[] {-1.0, -2.0, 10.0, 20.0});

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(-1.0);
        assertThat(bbox.xmax()).isEqualTo(10.0);
        assertThat(bbox.ymin()).isEqualTo(-2.0);
        assertThat(bbox.ymax()).isEqualTo(20.0);
    }

    @Test
    void mixedPointAndPolygonUnionsCoordsAndRecordsBothTypes() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(point2D(50.0, 50.0));

        WkbWriter polygon = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 3);
        polygon.writeInt(1);
        polygon.writeInt(4);
        polygon.writeDouble(0.0);
        polygon.writeDouble(0.0);
        polygon.writeDouble(10.0);
        polygon.writeDouble(0.0);
        polygon.writeDouble(10.0);
        polygon.writeDouble(10.0);
        polygon.writeDouble(0.0);
        polygon.writeDouble(0.0);
        acc.update(polygon.toSegment());

        GeospatialStatistics stats = acc.finish();
        BoundingBox bbox = stats.bbox().orElseThrow();
        assertThat(bbox.xmin()).isZero();
        assertThat(bbox.xmax()).isEqualTo(50.0);
        assertThat(bbox.ymin()).isZero();
        assertThat(bbox.ymax()).isEqualTo(50.0);
        assertThat(stats.geospatialTypes())
                .hasValueSatisfying(types -> assertThat(types).containsExactly(1, 3));
    }

    @Test
    void pointZRecordsZmin() {
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

    @Test
    void pointMRecordsMminAndLeavesZEmpty() {
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

    @Test
    void pointZmRecordsBothZAndM() {
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

    @Test
    void bigEndianPointParsesIdenticallyToLittleEndian() {
        WkbWriter wkb = new WkbWriter(ByteOrder.BIG_ENDIAN, 1);
        wkb.writeDouble(7.5);
        wkb.writeDouble(-3.25);

        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(wkb.toSegment());

        BoundingBox bbox = acc.finish().bbox().orElseThrow();
        assertThat(bbox.xmin()).isEqualTo(7.5);
        assertThat(bbox.ymin()).isEqualTo(-3.25);
    }

    @Test
    void resetClearsBboxAndTypeSet() {
        GeospatialStatisticsAccumulator acc = new GeospatialStatisticsAccumulator();
        acc.update(point2D(1.0, 2.0));
        acc.reset();

        GeospatialStatistics stats = acc.finish();
        assertThat(stats.bbox()).isEmpty();
        assertThat(stats.geospatialTypes()).isEmpty();
    }

    private static MemorySegment point2D(double x, double y) {
        WkbWriter wkb = new WkbWriter(ByteOrder.LITTLE_ENDIAN, 1);
        wkb.writeDouble(x);
        wkb.writeDouble(y);
        return wkb.toSegment();
    }

    /**
     * Builds a WKB payload byte-by-byte. The constructor emits the byte-order byte and the LE/BE-coded type code; the
     * {@code write...} helpers append payload doubles and integers in the chosen byte order.
     */
    private static final class WkbWriter {

        private final ByteBuffer buffer;

        WkbWriter(ByteOrder order, int wkbType) {
            this.buffer = ByteBuffer.allocate(4096).order(order);
            byte orderByte = (byte) (order == ByteOrder.LITTLE_ENDIAN ? 0x01 : 0x00);
            buffer.put(orderByte);
            buffer.putInt(wkbType);
        }

        void writeInt(int value) {
            buffer.putInt(value);
        }

        void writeDouble(double value) {
            buffer.putDouble(value);
        }

        /**
         * Appends a nested LineString geometry (used inside MULTI* / GEOMETRYCOLLECTION containers). Each child carries
         * its own byte-order byte and type code per the WKB spec.
         */
        void writeChildLineString(ByteOrder childOrder, double[] xyPairs) {
            byte orderByte = (byte) (childOrder == ByteOrder.LITTLE_ENDIAN ? 0x01 : 0x00);
            buffer.put(orderByte);
            ByteOrder previous = buffer.order();
            buffer.order(childOrder);
            buffer.putInt(2); // LineString type code
            buffer.putInt(xyPairs.length / 2);
            for (double v : xyPairs) {
                buffer.putDouble(v);
            }
            buffer.order(previous);
        }

        MemorySegment toSegment() {
            byte[] copy = new byte[buffer.position()];
            buffer.flip();
            buffer.get(copy);
            return MemorySegment.ofArray(copy).asReadOnly();
        }
    }
}
