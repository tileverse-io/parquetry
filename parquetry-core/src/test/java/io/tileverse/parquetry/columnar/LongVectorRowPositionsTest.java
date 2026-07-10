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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

/**
 * Verifies the synthesized absolute row-position backing of {@link LongVector} against identity and page-skipped maps.
 */
class LongVectorRowPositionsTest {

    @Test
    void mapsLogicalRowsToAbsolutePositionsUnderBitsSelection() {
        BitSet decoded = new BitSet();
        decoded.set(0);
        decoded.set(2);
        decoded.set(3);
        Selection sel = Selection.bits(decoded);

        LongVector pos = LongVector.rowPositions(1000L, sel, 3);

        assertThat(pos.getLong(0)).isEqualTo(1000L);
        assertThat(pos.getLong(1)).isEqualTo(1002L);
        assertThat(pos.getLong(2)).isEqualTo(1003L);
    }

    @Test
    void identitySelectionGivesContiguousPositions() {
        LongVector pos = LongVector.rowPositions(0L, Selection.ALL, 3);

        assertThat(pos.getLong(0)).isEqualTo(0L);
        assertThat(pos.getLong(2)).isEqualTo(2L);
    }

    @Test
    void rangeSelectionOffsetsFromTheRangeStart() {
        LongVector pos = LongVector.rowPositions(500L, Selection.range(10, 4), 4);

        assertThat(pos.getLong(0)).isEqualTo(510L);
        assertThat(pos.getLong(3)).isEqualTo(513L);
    }

    @Test
    void everyRowIsNonNull() {
        LongVector pos = LongVector.rowPositions(0L, Selection.ALL, 3);

        assertThat(pos.hasNulls()).isFalse();
        assertThat(pos.isNull(0)).isFalse();
        assertThat(pos.size()).isEqualTo(3);
    }

    @Test
    void selectComposesTheUnderlyingPositionMap() {
        LongVector base = LongVector.rowPositions(1000L, Selection.range(0, 5), 5);

        BitSet keep = new BitSet();
        keep.set(1);
        keep.set(4);
        ColumnVector selected = base.select(Selection.bits(keep));

        assertThat(selected).isInstanceOf(LongVector.class);
        LongVector positions = (LongVector) selected;
        assertThat(positions.size()).isEqualTo(2);
        assertThat(positions.getLong(0)).isEqualTo(1001L);
        assertThat(positions.getLong(1)).isEqualTo(1004L);
    }

    @Test
    void getReturnsBoxedLong() {
        LongVector pos = LongVector.rowPositions(7L, Selection.ALL, 2);

        Long value = pos.get(1);
        assertThat(value).isEqualTo(8L);
    }

    @Test
    void materializesEveryPosition() {
        LongVector pos = LongVector.rowPositions(100L, Selection.range(0, 4), 4);

        assertThat(readAll(pos)).containsExactly(100L, 101L, 102L, 103L);
    }

    @Test
    void materializesSelectedPositions() {
        BitSet keep = new BitSet();
        keep.set(0);
        keep.set(3);
        LongVector selected = (LongVector)
                LongVector.rowPositions(100L, Selection.range(0, 4), 4).select(Selection.bits(keep));

        assertThat(readAll(selected)).containsExactly(100L, 103L);
    }

    private static long[] readAll(LongVector vector) {
        int rows = vector.size();
        long[] out = new long[rows];
        for (int row = 0; row < rows; row++) {
            out[row] = vector.getLong(row);
        }
        return out;
    }
}
