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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class UuidConverterTest {

    @Test
    void roundTripsThroughBytes() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        assertThat(UuidConverter.fromSegment(UuidConverter.toReadOnlySegment(uuid)))
                .isEqualTo(uuid);
    }

    @Test
    void encodesMostSignificantBitsFirstBigEndian() {
        UUID uuid = new UUID(0x0011223344556677L, 0x8899aabbccddeeffL);
        byte[] bytes = UuidConverter.toBytes(uuid);
        assertThat(bytes[0]).isEqualTo((byte) 0x00);
        assertThat(bytes[7]).isEqualTo((byte) 0x77);
        assertThat(bytes[8]).isEqualTo((byte) 0x88);
        assertThat(bytes[15]).isEqualTo((byte) 0xff);
        assertThat(bytes).hasSize(UuidConverter.BYTES);
    }

    @Test
    void comparesUnsignedNotSigned() {
        // 0xFF...-prefixed sorts ABOVE 0x00...-prefixed under unsigned byte order, the opposite of signed long order.
        UUID low = new UUID(0x0000000000000000L, 0L);
        UUID high = new UUID(0xFFFFFFFFFFFFFFFFL, 0L);
        MemorySegment lowSeg = UuidConverter.toReadOnlySegment(low);
        MemorySegment highSeg = UuidConverter.toReadOnlySegment(high);
        assertThat(UuidConverter.compareSegmentToUuid(highSeg, low)).isPositive();
        assertThat(UuidConverter.compareSegmentToUuid(lowSeg, high)).isNegative();
        assertThat(UuidConverter.compareSegmentToUuid(lowSeg, low)).isZero();
        // signed UUID.compareTo DISAGREES here: it says high < low
        assertThat(high.compareTo(low)).isNegative();
    }

    @Test
    void readsAtNonZeroOffsetIgnoringPadding() {
        UUID uuid = UUID.fromString("0123456f-89ab-cdef-fedc-ba9876543210");
        int offset = 8;
        MemorySegment padded = MemorySegment.ofArray(new byte[32]);
        fillWith(padded, (byte) 0xFF);
        MemorySegment.copy(MemorySegment.ofArray(UuidConverter.toBytes(uuid)), 0L, padded, offset, UuidConverter.BYTES);

        assertThat(UuidConverter.fromSegment(padded, offset)).isEqualTo(uuid);
    }

    @Test
    void comparesLowWordUnsignedWhenHighWordIsEqual() {
        UUID low = new UUID(7L, 0L);
        UUID high = new UUID(7L, 0xFFFFFFFFFFFFFFFFL);
        assertThat(UuidConverter.compareSegmentToUuid(UuidConverter.toReadOnlySegment(high), low))
                .isPositive();
        assertThat(UuidConverter.compareSegmentToUuid(UuidConverter.toReadOnlySegment(low), high))
                .isNegative();
        assertThat(UuidConverter.compareSegmentToUuid(UuidConverter.toReadOnlySegment(high), high))
                .isZero();
        // signed UUID.compareTo DISAGREES on the low word too: it says high < low
        assertThat(high.compareTo(low)).isNegative();
    }

    private static void fillWith(MemorySegment segment, byte value) {
        for (long i = 0; i < segment.byteSize(); i++) {
            segment.set(ValueLayout.JAVA_BYTE, i, value);
        }
    }
}
