/*
 * Copyright (c) 2026 Tileverse.io
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

import java.util.BitSet;

import org.junit.jupiter.api.Test;

class ListVectorTest {

    @Test
    void offsetsDefineRowSlices() {
        // 3 rows: row 0 = [10, 20], row 1 = [] (empty), row 2 = [30, 40, 50].
        IntVector child = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, allValid(5));
        int[] offsets = {0, 2, 2, 5};
        BitSet validity = new BitSet(3);
        validity.set(0, 3);

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
        BitSet validity = new BitSet(2);
        validity.set(0);

        ListVector vec = new ListVector(offsets, child, validity, 2);

        assertThat(vec.validity().get(0)).isTrue();
        assertThat(vec.validity().get(1)).isFalse();
    }

    @Test
    void emptyListRowDistinguishesFromNull() {
        // row 0 = [10, 20], row 1 = [] (empty, NOT null), row 2 = [30].
        IntVector child = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
        int[] offsets = {0, 2, 2, 3};
        BitSet validity = new BitSet(3);
        validity.set(0, 3); // all rows non-null, including the empty one

        ListVector vec = new ListVector(offsets, child, validity, 3);

        // Row 1 is non-null but zero-width
        assertThat(vec.validity().get(1)).isTrue();
        assertThat(vec.rowOffsetEnd(1) - vec.rowOffsetStart(1)).isZero();
    }

    @Test
    void constructorValidatesOffsetsLength() {
        IntVector child = IntVector.materialized(new int[] {1, 2}, allValid(2));
        int[] wrongOffsets = {0, 2}; // length 2, but size 3 needs length 4
        BitSet validity = new BitSet(3);
        validity.set(0, 3);

        assertThatThrownBy(() -> new ListVector(wrongOffsets, child, validity, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offsets length must be size + 1");
    }

    @Test
    void materializeCascadesToChild() {
        IntVector child = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));
        BitSet rootValidity = new BitSet(1);
        rootValidity.set(0);
        ListVector vec = new ListVector(new int[] {0, 3}, child, rootValidity, 1);

        // child is already materialized via IntVector.materialized() factory
        assertThat(vec.isMaterialized()).isTrue();
        vec.materialize(); // idempotent
        assertThat(vec.isMaterialized()).isTrue();
    }

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }
}
