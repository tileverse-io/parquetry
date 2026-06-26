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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    private static BitSet survivors(int... rows) {
        BitSet bits = new BitSet();
        for (int row : rows) {
            bits.set(row);
        }
        return bits;
    }

    @Nested
    class Select {

        @Test
        void selectAllReturnsTheSameVector() {
            BinaryVector vec =
                    BinaryVector.materialized(new MemorySegment[] {seg("a"), seg("b")}, Validity.allValid(2));
            assertThat(vec.select(Selection.ALL)).isSameAs(vec);
        }

        @Test
        void selectedConsolidatedReadsSurvivorsPerRow() {
            BinaryVector vec = BinaryVector.materialized(
                    new MemorySegment[] {seg("alpha"), seg("beta"), seg("gamma"), seg("delta")}, Validity.allValid(4));
            BinaryVector selected = (BinaryVector) vec.select(Selection.bits(survivors(1, 3)));

            assertThat(selected.size()).isEqualTo(2);
            assertThat(text(selected.get(0))).isEqualTo("beta");
            assertThat(text(selected.get(1))).isEqualTo("delta");
            assertThat(selected.valueLength(0)).isEqualTo("beta".length());
        }

        @Test
        void selectedDictionaryReadsSurvivorsPerRow() {
            MemorySegment[] dict = {seg("red"), seg("green"), seg("blue")};
            int[] indices = {0, 1, 2, 1};
            BinaryVector vec = BinaryVector.dictionary(dict, IntSequence.of(indices), Validity.allValid(4));
            BinaryVector selected = (BinaryVector) vec.select(Selection.bits(survivors(0, 2, 3)));

            assertThat(selected.size()).isEqualTo(3);
            assertThat(text(selected.get(0))).isEqualTo("red");
            assertThat(text(selected.get(1))).isEqualTo("blue");
            assertThat(text(selected.get(2))).isEqualTo("green");
        }

        @Test
        void selectedViewProjectsValidity() {
            BitSet valid = survivors(0, 2); // physical row 1 is null
            BinaryVector vec =
                    BinaryVector.materialized(new MemorySegment[] {seg("a"), null, seg("c")}, Validity.of(valid, 3));
            BinaryVector selected = (BinaryVector) vec.select(Selection.bits(survivors(0, 1, 2)));
            assertThat(selected.get(0)).isNotNull();
            assertThat(selected.get(1)).as("physical 1 was null").isNull();
            assertThat(text(selected.get(2))).isEqualTo("c");
        }

        @Test
        void bulkBackingOfASelectedVectorIsDeferredToTheArrowBoundary() {
            BinaryVector vec =
                    BinaryVector.materialized(new MemorySegment[] {seg("a"), seg("b"), seg("c")}, Validity.allValid(3));
            BinaryVector selected = (BinaryVector) vec.select(Selection.bits(survivors(0, 2)));
            assertThatThrownBy(selected::consolidatedOffsets)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Arrow export boundary");
        }
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

            BinaryVector vec = BinaryVector.dictionary(dict, IntSequence.of(indices), validity);

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

            BinaryVector vec = BinaryVector.dictionary(dict, IntSequence.of(indices), validity);

            assertThat(vec.get(1)).isNull();
            assertThat(text(vec.get(0))).isEqualTo("red");
        }
    }

    @Nested
    class ReusableTargetAccessors {

        @Test
        void getIntoCopiesConsolidatedValueAndReturnsLength() {
            BitSet bits = new BitSet(3);
            bits.set(0, 3);
            BinaryVector vec = BinaryVector.materialized(
                    new MemorySegment[] {seg("alpha"), seg("be"), seg("gamma")}, Validity.of(bits, 3));

            MemorySegment target = MemorySegment.ofArray(new byte[16]);
            long cursor = 0;
            int n0 = vec.getInto(0, target, cursor);
            cursor += n0;
            int n1 = vec.getInto(1, target, cursor);

            assertThat(n0).as("alpha length").isEqualTo(5);
            assertThat(n1).as("be length").isEqualTo(2);
            assertThat(text(target.asSlice(0, 7)))
                    .as("packed contiguously at the cursor")
                    .isEqualTo("alphabe");
        }

        @Test
        void getIntoReturnsMinusOneOnNullAndWritesNothing() {
            BitSet bits = new BitSet(2);
            bits.set(1);
            BinaryVector vec = BinaryVector.materialized(new MemorySegment[] {null, seg("x")}, Validity.of(bits, 2));

            MemorySegment target = MemorySegment.ofArray(new byte[] {'Z'});
            int n = vec.getInto(0, target, 0L);

            assertThat(n).as("null row returns -1").isEqualTo(-1);
            assertThat(target.get(JAVA_BYTE, 0L)).as("target untouched").isEqualTo((byte) 'Z');
        }

        @Test
        void valueLengthIsMinusOneForNullAndDistinguishesAPresentEmptyValue() {
            BitSet bits = new BitSet(3);
            bits.set(1, 3); // row 0 null, rows 1 (empty) and 2 present
            BinaryVector vec =
                    BinaryVector.materialized(new MemorySegment[] {null, seg(""), seg("gamma")}, Validity.of(bits, 3));

            assertThat(vec.valueLength(0)).as("null row is -1").isEqualTo(-1);
            assertThat(vec.valueLength(1))
                    .as("present empty value is 0, not null")
                    .isZero();
            assertThat(vec.valueLength(2)).as("present row").isEqualTo(5);
            assertThat(vec.getInto(1, MemorySegment.ofArray(new byte[1]), 0L))
                    .as("getInto on a present empty value copies nothing and returns 0")
                    .isZero();
        }

        @Test
        void getIntoReadsThroughDictionaryEntries() {
            MemorySegment[] dict = {seg("red"), seg("green")};
            int[] indices = {1, 0};
            BitSet bits = new BitSet(2);
            bits.set(0, 2);
            BinaryVector vec = BinaryVector.dictionary(dict, IntSequence.of(indices), Validity.of(bits, 2));

            MemorySegment target = MemorySegment.ofArray(new byte[8]);
            int n = vec.getInto(0, target, 0L);

            assertThat(n).isEqualTo(5);
            assertThat(text(target.asSlice(0, 5))).isEqualTo("green");
            assertThat(vec.valueLength(1)).as("dictionary value length").isEqualTo(3);
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

            BinaryVector vec = BinaryVector.of(backing, IntSequence.of(offsets), validity);

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

            BinaryVector vec = BinaryVector.of(backing, IntSequence.of(offsets), validity);

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
                BinaryVector vector = BinaryVector.of(data, IntSequence.of(offsets), Validity.allValid(2));
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
