/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

class AvroBinaryEncoderWriteTest {

    private AvroBinaryDecoder decoderOf(AvroBinaryEncoder encoder) {
        return new AvroBinaryDecoder(MemorySegment.ofArray(encoder.toByteArray()));
    }

    @Test
    void roundTripsLongsAcrossZigzagBoundaries() {
        AvroBinaryEncoder encoder = new AvroBinaryEncoder();
        long[] values = {0, -1, 1, 63, -64, 64, Long.MAX_VALUE, Long.MIN_VALUE};
        for (long value : values) {
            encoder.writeLong(value);
        }
        AvroBinaryDecoder decoder = decoderOf(encoder);
        for (long value : values) {
            assertThat(decoder.readLong()).isEqualTo(value);
        }
    }

    @Test
    void roundTripsFloatsDoublesStringsAndBytes() {
        AvroBinaryEncoder encoder = new AvroBinaryEncoder();
        encoder.writeFloat(3.5f);
        encoder.writeDouble(-2.25d);
        encoder.writeBoolean(true);
        encoder.writeString("healthé");
        encoder.writeBytes(MemorySegment.ofArray(new byte[] {9, 8, 7}));
        encoder.writeFixed(new byte[] {1, 2, 3, 4});

        AvroBinaryDecoder decoder = decoderOf(encoder);
        assertThat(decoder.readFloat()).isEqualTo(3.5f);
        assertThat(decoder.readDouble()).isEqualTo(-2.25d);
        assertThat(decoder.readBoolean()).isTrue();
        assertThat(decoder.readString()).isEqualTo("healthé");
        assertThat(decoder.readBytes().toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                .containsExactly(9, 8, 7);
        assertThat(decoder.fixed(4).toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                .containsExactly(1, 2, 3, 4);
    }
}
