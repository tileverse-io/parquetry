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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

class ListVectorTest {

    @Test
    void offsetsDefineRowSlices() {
        // 3 rows: row 0 = [10, 20], row 1 = [] (empty), row 2 = [30, 40, 50].
        IntVector child = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, allValid(5));
        int[] offsets = {0, 2, 2, 5};
        BitSet validBits = new BitSet(3);
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);

        ListVector vec = new ListVector(offsets, child, validity, 3);

        assertThat(vec.size()).isEqualTo(3);
        assertThat(vec.rowOffsetStart(0)).isZero();
        assertThat(vec.rowOffsetEnd(0)).isEqualTo(2);
        assertThat(vec.rowOffsetStart(1)).isEqualTo(2);
        assertThat(vec.rowOffsetEnd(1)).isEqualTo(2);
        assertThat(vec.rowOffsetStart(2)).isEqualTo(2);
        assertThat(vec.rowOffsetEnd(2)).isEqualTo(5);
        assertThat(vec.child()).isSameAs(child);
    }

    @Test
    void nullRowsCarryNullValidity() {
        IntVector child = IntVector.materialized(new int[] {7, 8}, allValid(2));
        int[] offsets = {0, 2, 2};
        BitSet validBits = new BitSet(2);
        validBits.set(0);
        Validity validity = Validity.of(validBits, 2);

        ListVector vec = new ListVector(offsets, child, validity, 2);

        assertThat(vec.validity().isValid(0)).isTrue();
        assertThat(vec.validity().isValid(1)).isFalse();
    }

    @Test
    void emptyListRowDistinguishesFromNull() {
        // row 0 = [10, 20], row 1 = [] (empty, NOT null), row 2 = [30].
        IntVector child = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
        int[] offsets = {0, 2, 2, 3};
        BitSet validBits = new BitSet(3);
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3); // all rows non-null, including the empty one

        ListVector vec = new ListVector(offsets, child, validity, 3);

        // Row 1 is non-null but zero-width
        assertThat(vec.validity().isValid(1)).isTrue();
        assertThat(vec.rowOffsetEnd(1) - vec.rowOffsetStart(1)).isZero();
    }

    @Test
    void constructorValidatesOffsetsLength() {
        IntVector child = IntVector.materialized(new int[] {1, 2}, allValid(2));
        int[] wrongOffsets = {0, 2}; // length 2, but size 3 needs length 4
        BitSet validBits = new BitSet(3);
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);

        assertThatThrownBy(() -> new ListVector(wrongOffsets, child, validity, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offsets length must be size + 1");
    }

    @org.junit.jupiter.api.Nested
    class Select {

        @Test
        void selectAllReturnsTheSameVector() {
            IntVector child = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
            ListVector vec = new ListVector(new int[] {0, 2, 3}, child, allValid(2), 2);
            assertThat(vec.select(Selection.ALL)).isSameAs(vec);
        }

        @Test
        void selectedViewTranslatesParentRowsAndKeepsChildWhole() {
            // rows: 0=[10,20] 1=[] 2=[30,40,50] 3=[60]
            IntVector child = IntVector.materialized(new int[] {10, 20, 30, 40, 50, 60}, allValid(6));
            int[] offsets = {0, 2, 2, 5, 6};
            ListVector vec = new ListVector(offsets, child, allValid(4), 4);

            // select parent rows 0 and 2
            ListVector selected = (ListVector) vec.select(Selection.bits(survivors(0, 2)));

            assertThat(selected.size()).isEqualTo(2);
            assertThat(selected.rowOffsetStart(0)).as("row 0 still [0,2)").isZero();
            assertThat(selected.rowOffsetEnd(0)).isEqualTo(2);
            assertThat(selected.rowOffsetStart(1))
                    .as("logical 1 == physical 2 -> [2,5)")
                    .isEqualTo(2);
            assertThat(selected.rowOffsetEnd(1)).isEqualTo(5);
            assertThat(selected.child()).as("child stays whole and shared").isSameAs(child);
        }

        @Test
        void selectedViewProjectsValidity() {
            // physical row 1 is null
            IntVector child = IntVector.materialized(new int[] {10, 20}, allValid(2));
            int[] offsets = {0, 1, 1, 2};
            BitSet valid = new BitSet(3);
            valid.set(0);
            valid.set(2);
            ListVector vec = new ListVector(offsets, child, Validity.of(valid, 3), 3);

            ListVector selected = (ListVector) vec.select(Selection.bits(survivors(1, 2)));
            assertThat(selected.isNull(0)).as("physical 1 was null").isTrue();
            assertThat(selected.isValid(1)).as("physical 2 valid").isTrue();
        }

        @Test
        void offsetsOfASelectedListIsDeferredToTheArrowBoundary() {
            IntVector child = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
            ListVector vec = new ListVector(new int[] {0, 1, 2, 3}, child, allValid(3), 3);
            ListVector selected = (ListVector) vec.select(Selection.bits(survivors(0, 2)));
            assertThatThrownBy(selected::offsets)
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("Arrow export boundary");
        }

        private BitSet survivors(int... physicalRows) {
            BitSet bits = new BitSet();
            for (int row : physicalRows) {
                bits.set(row);
            }
            return bits;
        }
    }

    private static Validity allValid(int n) {
        return Validity.allValid(n);
    }
}
