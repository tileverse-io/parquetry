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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SelectionTest {

    @Nested
    class All {

        @Test
        void physicalIsIdentity() {
            assertThat(Selection.ALL.physical(0)).isZero();
            assertThat(Selection.ALL.physical(7)).isEqualTo(7);
        }

        @Test
        void lengthIsUnsupportedBecauseItHasNoDomain() {
            assertThatThrownBy(() -> Selection.ALL.length())
                    .isInstanceOf(UnsupportedOperationException.class)
                    .hasMessageContaining("== ALL guard");
        }

        @Test
        void forEachPhysicalIsUnsupportedBecauseItHasNoDomain() {
            assertThatThrownBy(() -> Selection.ALL.forEachPhysical(physical -> {}))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void isTheSharedSingleton() {
            assertThat(Selection.ALL).isSameAs(Selection.ALL);
        }
    }

    @Nested
    class Range {

        @Test
        void lengthAndPhysicalTranslateTheWindow() {
            Selection.Range range = Selection.range(5, 3);
            assertThat(range.length()).isEqualTo(3);
            assertThat(range.physical(0)).isEqualTo(5);
            assertThat(range.physical(1)).isEqualTo(6);
            assertThat(range.physical(2)).isEqualTo(7);
        }

        @Test
        void forEachPhysicalVisitsTheWindowInOrder() {
            List<Integer> visited = collect(Selection.range(5, 3));
            assertThat(visited).containsExactly(5, 6, 7);
        }

        @Test
        void emptyWindowIsAllowed() {
            Selection.Range range = Selection.range(4, 0);
            assertThat(range.length()).isZero();
            assertThat(collect(range)).isEmpty();
        }

        @Test
        void rejectsNegativeStartOrLength() {
            assertThatThrownBy(() -> Selection.range(-1, 3)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> Selection.range(0, -3)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class Bits {

        @Test
        void lengthIsTheNumberOfSurvivors() {
            Selection.Bits bits = Selection.bits(survivors(1, 4, 9));
            assertThat(bits.length()).isEqualTo(3);
        }

        @Test
        void physicalMapsLogicalToTheNthSurvivor() {
            Selection.Bits bits = Selection.bits(survivors(1, 4, 9));
            assertThat(bits.physical(0)).isEqualTo(1);
            assertThat(bits.physical(1)).isEqualTo(4);
            assertThat(bits.physical(2)).isEqualTo(9);
        }

        @Test
        void physicalHandlesOutOfOrderAndRepeatedAccess() {
            Selection.Bits bits = Selection.bits(survivors(1, 4, 9, 12));
            assertThat(bits.physical(3)).isEqualTo(12); // jump forward
            assertThat(bits.physical(1)).isEqualTo(4); // jump backward (cursor reset)
            assertThat(bits.physical(1)).isEqualTo(4); // repeat
            assertThat(bits.physical(2)).isEqualTo(9); // step forward
            assertThat(bits.physical(0)).isEqualTo(1); // back to start
        }

        @Test
        void forEachPhysicalVisitsEverySurvivorInAscendingOrder() {
            List<Integer> visited = collect(Selection.bits(survivors(2, 5, 6, 11)));
            assertThat(visited).containsExactly(2, 5, 6, 11);
        }

        @Test
        void emptySurvivorsHaveZeroLength() {
            Selection.Bits bits = Selection.bits(new BitSet());
            assertThat(bits.length()).isZero();
            assertThat(collect(bits)).isEmpty();
        }

        @Test
        void subWindowsTheLogicalSurvivors() {
            // survivors at physical 1,4,9,12 -> logical 0,1,2,3
            Selection sub = Selection.bits(survivors(1, 4, 9, 12)).sub(1, 2); // logical [1,3) -> physical 4,9
            assertThat(sub.length()).isEqualTo(2);
            assertThat(collect(sub)).containsExactly(4, 9);
        }
    }

    @Nested
    class Sub {

        @Test
        void rangeSubShiftsTheStart() {
            Selection sub = Selection.range(5, 4).sub(1, 2); // logical [1,3) of [5,9) -> physical 6,7
            assertThat(sub.length()).isEqualTo(2);
            assertThat(collect(sub)).containsExactly(6, 7);
        }

        @Test
        void bitsSubAtTheStartKeepsLeadingSurvivors() {
            Selection sub = Selection.bits(survivors(2, 5, 6, 11)).sub(0, 2); // physical 2,5
            assertThat(collect(sub)).containsExactly(2, 5);
        }
    }

    private static BitSet survivors(int... physicalRows) {
        BitSet bits = new BitSet();
        for (int row : physicalRows) {
            bits.set(row);
        }
        return bits;
    }

    private static List<Integer> collect(Selection selection) {
        List<Integer> visited = new ArrayList<>();
        selection.forEachPhysical(visited::add);
        return visited;
    }
}
