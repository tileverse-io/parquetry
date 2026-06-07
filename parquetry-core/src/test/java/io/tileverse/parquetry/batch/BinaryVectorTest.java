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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.BitSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class BinaryVectorTest {

    private static MemorySegment seg(String s) {
        return MemorySegment.ofArray(s.getBytes()).asReadOnly();
    }

    private static String text(MemorySegment s) {
        return new String(s.toArray(JAVA_BYTE));
    }

    @Nested
    class ConsolidatingFactory {

        @Test
        void roundTripsValuesAndDropsNulls() {
            BitSet validBits = new BitSet(3);
            validBits.set(0);
            validBits.set(2);
            Validity validity = Validity.of(validBits, 3);
            MemorySegment[] values = {seg("alpha"), null, seg("gamma")};

            BinaryVector vec = BinaryVector.materialized(values, validity);

            assertThat(vec.size()).isEqualTo(3);
            assertThat(text(vec.get(0))).isEqualTo("alpha");
            assertThat(vec.get(0).isReadOnly()).isTrue();
            assertThat(text(vec.get(2))).isEqualTo("gamma");
            assertThat(vec.get(1)).isNull();
            assertThat(vec.get(0)).isInstanceOf(MemorySegment.class);
        }

        @Test
        void getReturnsNullOnNullRow() {
            BitSet validBits = new BitSet(2);
            validBits.set(1);
            Validity validity = Validity.of(validBits, 2);
            MemorySegment[] values = {null, seg("x")};

            BinaryVector vec = BinaryVector.materialized(values, validity);

            assertThat(vec.get(0))
                    .as("null row returns null, not a zero-length slice")
                    .isNull();
            assertThat(text(vec.get(1))).isEqualTo("x");
        }

        @Test
        void approximateHeapBytesCountsBackingAndOffsetsNotPerValueObjects() {
            BitSet validBits = new BitSet(2);
            validBits.set(0, 2);
            Validity validity = Validity.of(validBits, 2);
            MemorySegment[] values = {seg("ab"), seg("cde")}; // 5 backing bytes

            BinaryVector vec = BinaryVector.materialized(values, validity);

            assertThat(vec.approximateHeapBytes())
                    .isEqualTo(5L + 12L + vec.validity().heapBytes());
        }
    }

    @Nested
    class DictionaryMode {

        @Test
        void readsThroughIndicesAndSharesEntries() {
            MemorySegment[] dict = {seg("red"), seg("green")};
            int[] indices = {0, 1, 0};
            BitSet validBits = new BitSet(3);
            validBits.set(0, 3);
            Validity validity = Validity.of(validBits, 3);

            BinaryVector vec = BinaryVector.dictionary(dict, indices, validity);

            assertThat(vec.size()).isEqualTo(3);
            assertThat(text(vec.get(0))).isEqualTo("red");
            assertThat(text(vec.get(1))).isEqualTo("green");
            assertThat(vec.get(2)).isSameAs(dict[0]); // shared entry, no slice
        }

        @Test
        void nullRowReturnsNull() {
            MemorySegment[] dict = {seg("red")};
            int[] indices = {0, 0};
            BitSet validBits = new BitSet(2);
            validBits.set(0);
            Validity validity = Validity.of(validBits, 2);

            BinaryVector vec = BinaryVector.dictionary(dict, indices, validity);

            assertThat(vec.get(1)).isNull();
            assertThat(text(vec.get(0))).isEqualTo("red");
        }
    }

    @Nested
    class DirectFactory {

        @Test
        void ofBackingAndOffsetsReadsRows() {
            MemorySegment backing =
                    MemorySegment.ofArray("alphagamma".getBytes()).asReadOnly();
            int[] offsets = {0, 5, 10};
            BitSet validBits = new BitSet(2);
            validBits.set(0, 2);
            Validity validity = Validity.of(validBits, 2);

            BinaryVector vec = BinaryVector.of(backing, offsets, validity);

            assertThat(text(vec.get(0))).isEqualTo("alpha");
            assertThat(text(vec.get(1))).isEqualTo("gamma");
        }

        @Test
        void approximateHeapBytesCountsOnlyTheOffsetWindowNotTheSharedBacking() {
            // A page split into batches shares one backing; this sub-vector covers rows whose bytes are the [5, 10)
            // window of a 10-byte backing. It must count 5 window bytes, not the whole shared backing.
            MemorySegment backing =
                    MemorySegment.ofArray("alphagamma".getBytes()).asReadOnly();
            int[] offsets = {5, 10};
            BitSet validBits = new BitSet(1);
            validBits.set(0);
            Validity validity = Validity.of(validBits, 1);

            BinaryVector vec = BinaryVector.of(backing, offsets, validity);

            assertThat(vec.approximateHeapBytes())
                    .isEqualTo(5L
                            + (long) offsets.length * Integer.BYTES
                            + vec.validity().heapBytes());
        }

        @Test
        void consolidatedOffHeapBackingIsNotCountedAsHeap() {
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment data = arena.allocate(5); // two values, ab and cde, five bytes total
                MemorySegment.copy(MemorySegment.ofArray(new byte[] {'a', 'b', 'c', 'd', 'e'}), 0L, data, 0L, 5L);
                int[] offsets = {0, 2, 5};
                BinaryVector vector = BinaryVector.of(data, offsets, Validity.allValid(2));
                assertThat(vector.size()).as("size").isEqualTo(2);
                assertThat(vector.get(0).toArray(ValueLayout.JAVA_BYTE)).containsExactly('a', 'b');
                assertThat(vector.get(1).toArray(ValueLayout.JAVA_BYTE)).containsExactly('c', 'd', 'e');
                assertThat(vector.approximateHeapBytes())
                        .as("off-heap data is not counted as heap")
                        .isEqualTo((long) offsets.length * Integer.BYTES
                                + vector.validity().heapBytes());
            }
        }
    }
}
