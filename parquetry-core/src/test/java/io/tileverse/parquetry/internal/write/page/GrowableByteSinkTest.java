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
package io.tileverse.parquetry.internal.write.page;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Test;

class GrowableByteSinkTest {

    @Test
    void writesEveryByteFormAndReportsSize() {
        GrowableByteSink sink = new GrowableByteSink(4);
        sink.writeByte(0x01);
        sink.write(new byte[] {0x02, 0x03, 0x04}, 1, 2); // 0x03, 0x04
        sink.write(new byte[] {0x05});
        assertThat(sink.size()).isEqualTo(4);
        assertThat(sink.toByteArray()).containsExactly(0x01, 0x03, 0x04, 0x05);
    }

    @Test
    void writeIntEmitsFourLittleEndianBytes() {
        GrowableByteSink sink = new GrowableByteSink(4);
        sink.writeInt(0x04030201);
        assertThat(sink.size()).isEqualTo(4);
        assertThat(sink.toByteArray()).containsExactly(0x01, 0x02, 0x03, 0x04);
    }

    @Test
    void writeLongEmitsEightLittleEndianBytes() {
        GrowableByteSink sink = new GrowableByteSink(8);
        sink.writeLong(0x0807060504030201L);
        assertThat(sink.size()).isEqualTo(8);
        assertThat(sink.toByteArray()).containsExactly(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08);
    }

    @Test
    void writeFloatEmitsRawBitsLittleEndian() {
        GrowableByteSink sink = new GrowableByteSink(4);
        sink.writeFloat(1.0f); // Float.floatToRawIntBits(1.0f) == 0x3F800000
        assertThat(sink.toByteArray()).containsExactly(0x00, 0x00, 0x80, 0x3F);
    }

    @Test
    void writeDoubleEmitsRawBitsLittleEndian() {
        GrowableByteSink sink = new GrowableByteSink(8);
        sink.writeDouble(1.0); // Double.doubleToRawLongBits(1.0) == 0x3FF0000000000000L
        assertThat(sink.toByteArray()).containsExactly(0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F);
    }

    @Test
    void growsAcrossInitialCapacityWithoutTruncating() {
        GrowableByteSink sink = new GrowableByteSink(2);
        byte[] payload = new byte[1000];
        for (int i = 0; i < payload.length; i++) {
            payload[i] = (byte) i;
        }
        sink.write(payload);
        assertThat(sink.toByteArray()).isEqualTo(payload);
    }

    @Test
    void resetKeepsCapacityAndClearsContent() {
        GrowableByteSink sink = new GrowableByteSink(8);
        sink.write(new byte[] {1, 2, 3, 4, 5, 6});
        byte[] backingBefore = sink.array();
        sink.reset();
        assertThat(sink.size()).isEqualTo(0);
        sink.write(new byte[] {9});
        assertThat(sink.array()).isSameAs(backingBefore); // no reallocation on reuse
        assertThat(sink.toByteArray()).containsExactly(9);
    }

    @Test
    void codecSegmentViewsCurrentContentWritable() {
        GrowableByteSink sink = new GrowableByteSink(16);
        sink.write(new byte[] {0x11, 0x22, 0x33});

        MemorySegment seg = sink.codecSegment();

        assertThat(seg.byteSize()).isEqualTo(3); // the written prefix, not the 16-byte capacity
        assertThat(seg.isReadOnly()).isFalse();
        assertThat(seg.toArray(ValueLayout.JAVA_BYTE)).containsExactly(0x11, 0x22, 0x33);
    }

    @Test
    void codecSegmentAliasesTheSinkBackingArray() {
        GrowableByteSink sink = new GrowableByteSink(16);
        sink.write(new byte[] {0x11, 0x22, 0x33});

        MemorySegment seg = sink.codecSegment();
        seg.set(ValueLayout.JAVA_BYTE, 1, (byte) 0x44);

        assertThat(sink.array()[1]).isEqualTo((byte) 0x44);
    }

    @Test
    void twoDifferentLengthPagesThroughOneReusedSinkStayIndependent() {
        GrowableByteSink sink = new GrowableByteSink(16);
        sink.write(new byte[] {1, 2, 3, 4, 5});
        byte[] first = sink.toByteArray();
        sink.reset();
        sink.write(new byte[] {9, 9});
        byte[] second = sink.toByteArray();
        assertThat(first).containsExactly(1, 2, 3, 4, 5);
        assertThat(second).containsExactly(9, 9); // no trailing bytes from the longer first page
    }

    @Test
    void writeWholeMemorySegmentMatchesByteArrayWrite() {
        byte[] payload = {0x0A, 0x0B, 0x0C, 0x0D};

        GrowableByteSink viaArray = new GrowableByteSink(4);
        viaArray.write(payload);

        GrowableByteSink viaSegment = new GrowableByteSink(4);
        viaSegment.write(MemorySegment.ofArray(payload));

        assertThat(viaSegment.toByteArray()).isEqualTo(viaArray.toByteArray());
        assertThat(viaSegment.size()).isEqualTo(payload.length);
    }

    @Test
    void writeMemorySegmentSubRangeMatchesByteArrayWrite() {
        byte[] payload = {0x01, 0x02, 0x03, 0x04, 0x05};

        GrowableByteSink viaArray = new GrowableByteSink(2);
        viaArray.write(payload, 1, 3); // 0x02, 0x03, 0x04

        GrowableByteSink viaSegment = new GrowableByteSink(2);
        viaSegment.write(MemorySegment.ofArray(payload), 1L, 3L);

        assertThat(viaSegment.toByteArray()).isEqualTo(viaArray.toByteArray());
        assertThat(viaSegment.toByteArray()).containsExactly(0x02, 0x03, 0x04);
    }

    @Test
    void writeZeroLengthMemorySegmentIsNoOp() {
        GrowableByteSink sink = new GrowableByteSink(4);
        sink.writeByte(0x7F);
        sink.write(MemorySegment.ofArray(new byte[] {1, 2, 3}), 1L, 0L);
        assertThat(sink.size()).isEqualTo(1);
        assertThat(sink.toByteArray()).containsExactly(0x7F);
    }

    @Test
    void writeMemorySegmentInterleavesWithPrimitiveWrites() {
        GrowableByteSink sink = new GrowableByteSink(4);
        sink.writeInt(0x04030201);
        sink.write(MemorySegment.ofArray(new byte[] {(byte) 0xAA, (byte) 0xBB}));
        sink.writeByte(0xCC);
        assertThat(sink.size()).isEqualTo(7);
        assertThat(sink.toByteArray()).containsExactly(0x01, 0x02, 0x03, 0x04, 0xAA, 0xBB, 0xCC);
    }
}
