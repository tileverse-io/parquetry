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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.MalformedFileException;

class WkbCursorTest {

    private static final List<ByteOrder> BOTH_ORDERS = List.of(ByteOrder.LITTLE_ENDIAN, ByteOrder.BIG_ENDIAN);

    static Stream<Arguments> headers() {
        return Stream.of(
                Arguments.of("iso point", 1, 0, WkbTypeCode.POINT, Dimensions.XY, 1, 0),
                Arguments.of("iso pointZ", 1001, 0, WkbTypeCode.POINT, Dimensions.Z, 1001, 0),
                Arguments.of("iso linestringM", 2002, 0, WkbTypeCode.LINESTRING, Dimensions.M, 2002, 0),
                Arguments.of("iso polygonZM", 3003, 0, WkbTypeCode.POLYGON, Dimensions.ZM, 3003, 0),
                Arguments.of("ewkb pointZ", 0x80000001, 0, WkbTypeCode.POINT, Dimensions.Z, 1001, 0),
                Arguments.of("ewkb linestringM", 0x40000002, 0, WkbTypeCode.LINESTRING, Dimensions.M, 2002, 0),
                Arguments.of("ewkb polygonZM", 0xC0000003, 0, WkbTypeCode.POLYGON, Dimensions.ZM, 3003, 0),
                Arguments.of("ewkb point with srid", 0x20000001, 4326, WkbTypeCode.POINT, Dimensions.XY, 1, 4326),
                Arguments.of(
                        "ewkb multipolygonZ with srid",
                        0xA0000006,
                        3857,
                        WkbTypeCode.MULTIPOLYGON,
                        Dimensions.Z,
                        1006,
                        3857));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("headers")
    void decodesTheHeaderInBothByteOrders(
            String label, int rawType, int srid, int base, Dimensions dimensions, int isoType, int expectedSrid) {
        for (ByteOrder order : BOTH_ORDERS) {
            MemorySegment wkb = header(order, rawType, srid);
            WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());

            cursor.readHeader();

            assertThat(cursor.baseType()).as("%s base (%s)", label, order).isEqualTo(base);
            assertThat(cursor.headerDimensions())
                    .as("%s dimensions (%s)", label, order)
                    .isEqualTo(dimensions);
            assertThat(cursor.isoType()).as("%s iso (%s)", label, order).isEqualTo(isoType);
            assertThat(cursor.srid()).as("%s srid (%s)", label, order).isEqualTo(expectedSrid);
        }
    }

    @Test
    void runViewReadsEveryOrdinateAndCopiesTheRunInWireOrder() {
        for (ByteOrder order : BOTH_ORDERS) {
            // LINESTRING ZM (1 2 3 4, 5 6 7 8) under the ISO code 3002
            ByteBuffer buffer =
                    ByteBuffer.allocate(1 + 4 + 4 + 2 * 4 * Double.BYTES).order(order);
            buffer.put(orderByte(order)).putInt(3002).putInt(2);
            for (double ordinate : new double[] {1, 2, 3, 4, 5, 6, 7, 8}) {
                buffer.putDouble(ordinate);
            }
            MemorySegment wkb = MemorySegment.ofArray(buffer.array()).asReadOnly();
            WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());
            cursor.readHeader();
            int points = cursor.readCount(cursor.headerDimensions().count() * Double.BYTES);

            CoordinateRun run = cursor.run(points);

            assertThat(run.size()).as("size (%s)", order).isEqualTo(2);
            assertThat(run.dimensions()).as("dimensions (%s)", order).isEqualTo(Dimensions.ZM);
            assertThat(new double[] {run.x(0), run.y(0), run.z(0), run.m(0)}).containsExactly(1, 2, 3, 4);
            assertThat(new double[] {run.x(1), run.y(1), run.z(1), run.m(1)}).containsExactly(5, 6, 7, 8);
            double[] copy = new double[8];
            run.copyTo(copy);
            assertThat(copy).as("copy (%s)", order).containsExactly(1, 2, 3, 4, 5, 6, 7, 8);
        }
    }

    @Test
    void measureOfAnXymRunIsTheThirdOrdinate() {
        // POINT M (1 2 9) under the ISO code 2001
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 3 * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x01).putInt(2001).putDouble(1).putDouble(2).putDouble(9);
        MemorySegment wkb = MemorySegment.ofArray(buffer.array()).asReadOnly();
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());
        cursor.readHeader();

        CoordinateRun run = cursor.run(1);

        assertThat(run.dimensions()).isEqualTo(Dimensions.M);
        assertThat(run.m(0)).isEqualTo(9.0);
    }

    @Test
    void byteOrderByteOtherThanZeroOrOneIsRejected() {
        MemorySegment wkb =
                MemorySegment.ofArray(new byte[] {0x07, 0x01, 0x00, 0x00, 0x00}).asReadOnly();
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());

        assertThatThrownBy(cursor::readHeader)
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("byte-order");
    }

    @Test
    void unsupportedGeometryTypeIsRejected() {
        MemorySegment wkb = header(ByteOrder.LITTLE_ENDIAN, 8, 0);
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());

        assertThatThrownBy(cursor::readHeader)
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("Unsupported WKB geometry type: 8");
    }

    @Test
    void countThatCannotFitTheValueIsRejectedBeforeAnyAllocation() {
        // a MultiPoint header claiming ~2.1 billion members in a 9-byte value
        byte[] bytes = {0x01, 0x04, 0x00, 0x00, 0x00, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f};
        MemorySegment wkb = MemorySegment.ofArray(bytes).asReadOnly();
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());
        cursor.readHeader();

        assertThatThrownBy(() -> cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("count");
    }

    @Test
    void negativeElementCountIsRejected() {
        // a MultiPoint header whose member count is -1
        byte[] bytes = {0x01, 0x04, 0x00, 0x00, 0x00, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff};
        MemorySegment wkb = MemorySegment.ofArray(bytes).asReadOnly();
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());
        cursor.readHeader();

        assertThatThrownBy(() -> cursor.readCount(WkbCursor.MIN_GEOMETRY_BYTES))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("count");
    }

    @Test
    void negativeCoordinateCountIsRejected() {
        // a valid POINT header, then a run of a negative number of coordinates
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 2 * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x01).putInt(WkbTypeCode.POINT).putDouble(1).putDouble(2);
        MemorySegment wkb = MemorySegment.ofArray(buffer.array()).asReadOnly();
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());
        cursor.readHeader();

        assertThatThrownBy(() -> cursor.run(-1))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("negative");
    }

    @Test
    void runPastTheValueEndIsRejected() {
        // a LINESTRING claiming three points but holding one
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4 + 2 * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x01)
                .putInt(WkbTypeCode.LINESTRING)
                .putInt(3)
                .putDouble(1)
                .putDouble(2);
        MemorySegment wkb = MemorySegment.ofArray(buffer.array()).asReadOnly();
        WkbCursor cursor = new WkbCursor(wkb, 0L, wkb.byteSize());
        cursor.readHeader();

        assertThatThrownBy(() -> cursor.run(3))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining("truncated");
    }

    @Test
    void readsOnlyWithinTheWindowOfALargerBacking() {
        // POINT (1 2) at offset 7 of a backing that continues with readable bytes; a window that drops Y must throw
        ByteBuffer value = ByteBuffer.allocate(1 + 4 + 2 * Double.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        value.put((byte) 0x01).putInt(WkbTypeCode.POINT).putDouble(1).putDouble(2);
        byte[] padded = new byte[7 + value.capacity() + 8];
        System.arraycopy(value.array(), 0, padded, 7, value.capacity());
        MemorySegment backing = MemorySegment.ofArray(padded).asReadOnly();

        WkbCursor whole = new WkbCursor(backing, 7L, value.capacity());
        whole.readHeader();
        assertThat(whole.run(1).y(0)).isEqualTo(2.0);

        WkbCursor truncated = new WkbCursor(backing, 7L, value.capacity() - Double.BYTES);
        truncated.readHeader();
        assertThatThrownBy(() -> truncated.run(1)).isInstanceOf(MalformedFileException.class);
    }

    /** The byte-order byte and the type code, plus the SRID when {@code srid} is non-zero. */
    private static MemorySegment header(ByteOrder order, int rawType, int srid) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + 4).order(order);
        buffer.put(orderByte(order)).putInt(rawType);
        if (srid != 0) {
            buffer.putInt(srid);
        }
        byte[] bytes = new byte[buffer.position()];
        buffer.flip();
        buffer.get(bytes);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static byte orderByte(ByteOrder order) {
        return (byte) (order == ByteOrder.LITTLE_ENDIAN ? 0x01 : 0x00);
    }
}
