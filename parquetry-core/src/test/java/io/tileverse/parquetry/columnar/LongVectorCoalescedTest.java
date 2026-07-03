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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

/**
 * The per-row coalesce vector: a non-null physical cell keeps its value; a null cell falls back to a row-position or a
 * constant. The result has no nulls, and a selection re-selects both inputs together.
 */
class LongVectorCoalescedTest {

    @Test
    void aNonNullPhysicalCellKeepsItsValue() {
        LongVector physical = LongVector.materialized(new long[] {10L, 0L, 30L}, valid(3, 0, 2));
        LongVector fallback = LongVector.rowPositions(100L, Selection.ALL, 3);

        LongVector coalesced = LongVector.coalesced(physical, fallback);

        assertThat(coalesced.getLong(0)).isEqualTo(10L);
        assertThat(coalesced.getLong(2)).isEqualTo(30L);
    }

    @Test
    void aNullPhysicalCellFallsBackToTheRowPosition() {
        LongVector physical = LongVector.materialized(new long[] {10L, 0L, 30L}, valid(3, 0, 2));
        LongVector fallback = LongVector.rowPositions(100L, Selection.ALL, 3);

        LongVector coalesced = LongVector.coalesced(physical, fallback);

        assertThat(coalesced.getLong(1))
                .as("row 1 is null; falls back to base 100 + position 1")
                .isEqualTo(101L);
    }

    @Test
    void aNullPhysicalCellFallsBackToTheConstant() {
        LongVector physical = LongVector.materialized(new long[] {0L, 5L, 0L}, valid(3, 1));
        LongVector constant = LongVector.materialized(new long[] {999L, 999L, 999L}, Validity.allValid(3));

        LongVector coalesced = LongVector.coalesced(physical, constant);

        assertThat(coalesced.getLong(0)).isEqualTo(999L);
        assertThat(coalesced.getLong(1)).isEqualTo(5L);
        assertThat(coalesced.getLong(2)).isEqualTo(999L);
    }

    @Test
    void theCoalescedColumnHasNoNulls() {
        LongVector physical = LongVector.materialized(new long[] {0L, 5L, 0L}, valid(3, 1));
        LongVector fallback = LongVector.rowPositions(0L, Selection.ALL, 3);

        LongVector coalesced = LongVector.coalesced(physical, fallback);

        assertThat(coalesced.hasNulls()).isFalse();
        assertThat(coalesced.size()).isEqualTo(3);
        assertThat(coalesced.isNull(0)).isFalse();
    }

    @Test
    void aSelectionReselectsBothInputsTogether() {
        LongVector physical = LongVector.materialized(new long[] {10L, 0L, 30L, 0L, 50L}, valid(5, 0, 2, 4));
        LongVector fallback = LongVector.rowPositions(1000L, Selection.ALL, 5);

        LongVector coalesced = LongVector.coalesced(physical, fallback);
        LongVector selected = (LongVector) coalesced.select(Selection.bits(survivors(1, 3, 4)));

        assertThat(selected.size()).isEqualTo(3);
        assertThat(selected.getLong(0))
                .as("physical 1 null -> base 1000 + position 1")
                .isEqualTo(1001L);
        assertThat(selected.getLong(1))
                .as("physical 3 null -> base 1000 + position 3")
                .isEqualTo(1003L);
        assertThat(selected.getLong(2)).as("physical 4 present").isEqualTo(50L);
    }

    private static Validity valid(int size, int... validRows) {
        return Validity.of(survivors(validRows), size);
    }

    private static BitSet survivors(int... rows) {
        BitSet bits = new BitSet();
        for (int row : rows) {
            bits.set(row);
        }
        return bits;
    }
}
