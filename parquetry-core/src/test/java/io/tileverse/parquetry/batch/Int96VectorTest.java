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
package io.tileverse.parquetry.batch;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class Int96VectorTest {

    private static MemorySegment twelveBytes(int firstByte) {
        byte[] b = new byte[12];
        b[0] = (byte) firstByte;
        b[11] = (byte) (firstByte + 1);
        return MemorySegment.ofArray(b).asReadOnly();
    }

    @Nested
    class DictionaryMode {

        @Test
        void readsThroughIndicesAndSharesEntries() {
            MemorySegment[] dict = {twelveBytes(7), twelveBytes(9)};
            int[] indices = {0, 1, 0};
            BitSet validBits = new BitSet(3);
            validBits.set(0, 3);
            Validity validity = Validity.of(validBits, 3);

            Int96Vector vec = Int96Vector.dictionary(dict, indices, validity);

            assertThat(vec.size()).isEqualTo(3);
            assertThat(vec.get(0).toArray(JAVA_BYTE)[0]).isEqualTo((byte) 7);
            assertThat(vec.get(1).toArray(JAVA_BYTE)[0]).isEqualTo((byte) 9);
            assertThat(vec.get(2)).isSameAs(dict[0]); // shared entry, no slice
        }

        @Test
        void nullRowReturnsNull() {
            MemorySegment[] dict = {twelveBytes(7)};
            int[] indices = {0, 0};
            BitSet validBits = new BitSet(2);
            validBits.set(0);
            Validity validity = Validity.of(validBits, 2);

            Int96Vector vec = Int96Vector.dictionary(dict, indices, validity);

            assertThat(vec.get(1)).isNull();
            assertThat(vec.get(0).toArray(JAVA_BYTE)[0]).isEqualTo((byte) 7);
        }
    }

    @Test
    void readsTwelveByteRowsWithNullSlot() {
        BitSet validBits = new BitSet(3);
        validBits.set(0);
        validBits.set(2);
        Validity validity = Validity.of(validBits, 3);
        MemorySegment[] values = {twelveBytes(7), null, twelveBytes(9)};

        Int96Vector vec = Int96Vector.materialized(values, validity);

        assertThat(vec.size()).isEqualTo(3);
        assertThat(vec.get(0).byteSize()).isEqualTo(12);
        assertThat(vec.get(0).toArray(JAVA_BYTE)[0]).isEqualTo((byte) 7);
        assertThat(vec.get(0).toArray(JAVA_BYTE)[11]).isEqualTo((byte) 8);
        assertThat(vec.get(2).toArray(JAVA_BYTE)[0]).isEqualTo((byte) 9);
        assertThat(vec.get(1)).isNull();
        assertThat(vec.get(0).isReadOnly()).isTrue();
    }

    @Test
    void approximateHeapBytesIsBackingPlusValidity() {
        BitSet validBits = new BitSet(2);
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        MemorySegment[] values = {twelveBytes(1), twelveBytes(2)};

        Int96Vector vec = Int96Vector.materialized(values, validity);

        assertThat(vec.approximateHeapBytes()).isEqualTo(24L + vec.validity().heapBytes());
    }
}
