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
package io.tileverse.parquetry.columnar;

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.testsupport.VectorArrays;

class IntVectorTest {

    @Test
    void segmentBackedReadsValuesWithoutHeapValues() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment values = arena.allocate(3L * Integer.BYTES);
            values.setAtIndex(INT32, 0, 10);
            values.setAtIndex(INT32, 1, 20);
            values.setAtIndex(INT32, 2, 30);
            IntVector vector = IntVector.segmentBacked(values, Validity.allValid(3));
            assertThat(vector.size()).as("size").isEqualTo(3);
            assertThat(vector.getInt(0)).isEqualTo(10);
            assertThat(vector.getInt(2)).isEqualTo(30);
            assertThat(vector.<Integer>get(1)).isEqualTo(20);
            assertThat(vector.approximateHeapBytes())
                    .as("off-heap values are not counted as heap")
                    .isEqualTo(vector.validity().heapBytes());
        }
    }

    @Test
    void materializedHeapModeStillWorks() {
        IntVector vector = IntVector.materialized(new int[] {4, 5}, Validity.allValid(2));
        assertThat(vector.getInt(1)).isEqualTo(5);
        assertThat(VectorArrays.toArray(vector)).containsExactly(4, 5);
    }

    @Test
    void valueAtReadsTheBackingWithoutConsultingValidity() {
        BitSet valid = new BitSet(2);
        valid.set(0);
        IntVector vec = IntVector.materialized(new int[] {10, 99}, Validity.of(valid, 2));
        assertThat(vec.valueAt(0)).isEqualTo(10);
        assertThat(vec.valueAt(1))
                .as("a null row reads back its parked placeholder")
                .isEqualTo(99);
    }

    @Test
    void getIntReturnsTheValueForAValidRow() {
        IntVector vec = IntVector.materialized(new int[] {10, 20}, Validity.allValid(2));
        assertThat(vec.getInt(1)).as("valid row returns its value").isEqualTo(20);
    }

    @Test
    void getIntFailsFastOnANullRow() {
        BitSet bits = new BitSet(2);
        bits.set(0);
        IntVector vec = IntVector.materialized(new int[] {10, 0}, Validity.of(bits, 2));
        assertThatThrownBy(() -> vec.getInt(1))
                .as("a null row must not silently return the parked default 0")
                .isInstanceOf(IllegalStateException.class);
    }

    @Nested
    class CopyInto {

        @Test
        void copiesHeapBackedValuesAtTargetOffset() {
            IntVector vec = IntVector.materialized(new int[] {10, 20, 30, 40}, Validity.allValid(4));

            MemorySegment target = MemorySegment.ofArray(new byte[6 * Integer.BYTES]);
            vec.copyInto(target, (long) Integer.BYTES, 1, 2);

            assertThat(target.getAtIndex(INT32, 1)).isEqualTo(20);
            assertThat(target.getAtIndex(INT32, 2)).isEqualTo(30);
        }

        @Test
        void copiesSegmentBackedValues() {
            MemorySegment src = MemorySegment.ofArray(new byte[3 * Integer.BYTES]);
            MemorySegment.copy(new int[] {7, 8, 9}, 0, src, INT32, 0L, 3);
            IntVector vec = IntVector.segmentBacked(src, Validity.allValid(3));

            MemorySegment target = MemorySegment.ofArray(new byte[3 * Integer.BYTES]);
            vec.copyInto(target, 0L, 0, 3);

            assertThat(target.getAtIndex(INT32, 0)).isEqualTo(7);
            assertThat(target.getAtIndex(INT32, 2)).isEqualTo(9);
        }
    }

    @Nested
    class Select {

        @Test
        void selectAllReturnsTheSameVector() {
            IntVector vec = IntVector.materialized(new int[] {10, 20, 30}, Validity.allValid(3));
            assertThat(vec.select(Selection.ALL)).isSameAs(vec);
        }

        @Test
        void selectedViewExposesOnlySurvivorsInOrder() {
            IntVector vec = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, Validity.allValid(5));
            IntVector selected = (IntVector) vec.select(Selection.bits(survivors(1, 3, 4)));

            assertThat(selected.size()).as("logical size").isEqualTo(3);
            assertThat(selected.getInt(0)).isEqualTo(20);
            assertThat(selected.getInt(1)).isEqualTo(40);
            assertThat(selected.getInt(2)).isEqualTo(50);
            assertThat(selected.<Integer>get(2)).isEqualTo(50);
        }

        @Test
        void selectedViewSharesTheBackingArray() {
            int[] backing = {10, 20, 30, 40};
            IntVector vec = IntVector.materialized(backing, Validity.allValid(4));
            IntVector selected = (IntVector) vec.select(Selection.range(1, 2));
            // mutating the shared backing is visible through the view: no data copy was made
            backing[1] = 99;
            assertThat(selected.getInt(0)).isEqualTo(99);
        }

        @Test
        void selectedViewProjectsValidity() {
            // physical row 1 is null; select physical rows 0,1,3
            BitSet valid = new BitSet(4);
            valid.set(0);
            valid.set(2);
            valid.set(3);
            IntVector vec = IntVector.materialized(new int[] {10, 0, 30, 40}, Validity.of(valid, 4));
            IntVector selected = (IntVector) vec.select(Selection.bits(survivors(0, 1, 3)));

            assertThat(selected.isValid(0)).as("physical 0 valid").isTrue();
            assertThat(selected.isNull(1)).as("physical 1 was null").isTrue();
            assertThat(selected.isValid(2)).as("physical 3 valid").isTrue();
            assertThat(selected.getInt(0)).isEqualTo(10);
            assertThat(selected.getInt(2)).isEqualTo(40);
        }

        @Test
        void selectedAsArrayGathersSurvivors() {
            IntVector vec = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, Validity.allValid(5));
            IntVector selected = (IntVector) vec.select(Selection.bits(survivors(0, 2, 4)));
            assertThat(VectorArrays.toArray(selected)).containsExactly(10, 30, 50);
        }

        @Test
        void selectedCopyIntoScattersSurvivors() {
            IntVector vec = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, Validity.allValid(5));
            IntVector selected = (IntVector) vec.select(Selection.bits(survivors(1, 2, 4)));

            MemorySegment target = MemorySegment.ofArray(new byte[3 * Integer.BYTES]);
            selected.copyInto(target, 0L, 0, 3);

            assertThat(target.getAtIndex(INT32, 0)).isEqualTo(20);
            assertThat(target.getAtIndex(INT32, 1)).isEqualTo(30);
            assertThat(target.getAtIndex(INT32, 2)).isEqualTo(50);
        }

        private BitSet survivors(int... physicalRows) {
            BitSet bits = new BitSet();
            for (int row : physicalRows) {
                bits.set(row);
            }
            return bits;
        }
    }
}
