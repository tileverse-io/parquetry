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

class FixedLenBinaryVectorTest {

    private static MemorySegment seg(byte... b) {
        return MemorySegment.ofArray(b).asReadOnly();
    }

    @Nested
    class DictionaryMode {

        @Test
        void readsThroughIndicesAndSharesEntries() {
            MemorySegment[] dict = {seg((byte) 1, (byte) 2), seg((byte) 3, (byte) 4)};
            int[] indices = {0, 1, 0};
            BitSet validity = new BitSet(3);
            validity.set(0, 3);

            FixedLenBinaryVector vec = FixedLenBinaryVector.dictionary(dict, indices, 2, validity);

            assertThat(vec.size()).isEqualTo(3);
            assertThat(vec.byteWidth()).isEqualTo(2);
            assertThat(vec.get(0).toArray(JAVA_BYTE)).containsExactly(1, 2);
            assertThat(vec.get(1).toArray(JAVA_BYTE)).containsExactly(3, 4);
            assertThat(vec.get(2)).isSameAs(dict[0]); // shared entry, no slice
        }

        @Test
        void nullRowReturnsNull() {
            MemorySegment[] dict = {seg((byte) 1, (byte) 2)};
            int[] indices = {0, 0};
            BitSet validity = new BitSet(2);
            validity.set(0);

            FixedLenBinaryVector vec = FixedLenBinaryVector.dictionary(dict, indices, 2, validity);

            assertThat(vec.getOrNull(1)).isNull();
            assertThat(vec.get(0).toArray(JAVA_BYTE)).containsExactly(1, 2);
        }
    }

    @Test
    void readsFixedWidthRowsWithNullSlot() {
        BitSet validity = new BitSet(3);
        validity.set(0);
        validity.set(2);
        MemorySegment[] values = {seg((byte) 1, (byte) 2), null, seg((byte) 5, (byte) 6)};

        FixedLenBinaryVector vec = FixedLenBinaryVector.materialized(values, 2, validity);

        assertThat(vec.size()).isEqualTo(3);
        assertThat(vec.byteWidth()).isEqualTo(2);
        assertThat(vec.get(0).toArray(JAVA_BYTE)).containsExactly(1, 2);
        assertThat(vec.get(2).toArray(JAVA_BYTE)).containsExactly(5, 6);
        assertThat(vec.getOrNull(1)).isNull();
        assertThat(vec.get(0).isReadOnly()).isTrue();
    }

    @Test
    void approximateHeapBytesIsBackingPlusValidity() {
        BitSet validity = new BitSet(2);
        validity.set(0, 2);
        MemorySegment[] values = {seg((byte) 1, (byte) 2), seg((byte) 3, (byte) 4)};

        FixedLenBinaryVector vec = FixedLenBinaryVector.materialized(values, 2, validity);

        assertThat(vec.approximateHeapBytes()).isEqualTo(4L + ColumnVector.validityBytes(2));
    }

    @Test
    void ofBackingReadsRows() {
        MemorySegment backing = MemorySegment.ofArray(new byte[] {1, 2, 3, 4}).asReadOnly();
        BitSet validity = new BitSet(2);
        validity.set(0, 2);

        FixedLenBinaryVector vec = FixedLenBinaryVector.of(backing, 2, validity);

        assertThat(vec.get(0).toArray(JAVA_BYTE)).containsExactly(1, 2);
        assertThat(vec.get(1).toArray(JAVA_BYTE)).containsExactly(3, 4);
    }
}
