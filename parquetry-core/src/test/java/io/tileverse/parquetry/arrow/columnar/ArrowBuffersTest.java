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
package io.tileverse.parquetry.arrow.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Validity;

class ArrowBuffersTest {

    @Test
    void alignsUpToEightBytes() {
        assertThat(ArrowBuffers.align(0)).isZero();
        assertThat(ArrowBuffers.align(1)).isEqualTo(8);
        assertThat(ArrowBuffers.align(8)).isEqualTo(8);
        assertThat(ArrowBuffers.align(9)).isEqualTo(16);
    }

    @Test
    void allValidValidityEncodesToEmptyBufferAndRestores() {
        Validity allValid = Validity.allValid(5);
        MemorySegment buffer = ArrowBuffers.encodeValidity(allValid);
        assertThat(buffer.byteSize()).isZero();
        Validity restored = ArrowBuffers.decodeValidity(buffer, allValid.nullCount(), 5);
        assertThat(restored.hasNulls()).isFalse();
        assertThat(restored.size()).isEqualTo(5);
    }

    @Test
    void nullsValidityRoundTripsLsbFirst() {
        BitSet valid = new BitSet(5);
        valid.set(0);
        valid.set(2);
        valid.set(4); // rows 1 and 3 null
        Validity withNulls = Validity.of(valid, 5);
        MemorySegment buffer = ArrowBuffers.encodeValidity(withNulls);
        assertThat(buffer.byteSize()).isEqualTo(8);
        Validity restored = ArrowBuffers.decodeValidity(buffer, withNulls.nullCount(), 5);
        assertThat(restored.isNull(1)).isTrue();
        assertThat(restored.isNull(3)).isTrue();
        assertThat(restored.isValid(0)).isTrue();
        assertThat(restored.nullCount()).isEqualTo(2);
    }

    @Test
    void encodeIntsIsLittleEndianAndPaddedToEight() {
        IntVector vector = IntVector.materialized(new int[] {1, 2, 3}, Validity.allValid(3));
        MemorySegment data = ArrowBuffers.encodeInts(vector);

        assertThat(data.byteSize()).isEqualTo(16); // align(3 * 4) == 16
        byte[] bytes = data.toArray(ValueLayout.JAVA_BYTE);
        assertThat(bytes)
                .containsExactly(
                        0x01, 0x00, 0x00, 0x00, // 1, little-endian
                        0x02, 0x00, 0x00, 0x00, // 2
                        0x03, 0x00, 0x00, 0x00, // 3
                        0x00, 0x00, 0x00, 0x00); // alignment pad
    }

    @Test
    void encodeOffsetsAreZeroBasedCumulativeInt32() {
        MemorySegment offsets = ArrowBuffers.encodeOffsets(new int[] {2, 0, 5});

        assertThat(offsets.byteSize()).isEqualTo(16); // align(4 entries * 4) == 16
        int[] decoded = ArrowBuffers.decodeOffsets(offsets, 3);
        assertThat(decoded).containsExactly(0, 2, 2, 7);
    }

    @Test
    void allValidNodeOmitsTheValidityBitmap() {
        IntVector vector = IntVector.materialized(new int[] {7, 8}, Validity.allValid(2));
        EncodedNode node = ArrowBufferCodec.encode(vector);

        assertThat(node.length()).isEqualTo(2);
        assertThat(node.nullCount()).isZero();
        EncodedBuffer validity = node.buffers().get(0);
        assertThat(validity.role()).isEqualTo(EncodedBuffer.BufferRole.VALIDITY);
        assertThat(validity.bytes().byteSize()).isZero();
    }
}
