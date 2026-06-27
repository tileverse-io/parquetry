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
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.ColumnPath;

class StructVectorTest {

    @Test
    void childrenAccessibleByPath() {
        ColumnPath aPath = ColumnPath.of("s", "a");
        ColumnPath bPath = ColumnPath.of("s", "b");
        IntVector a = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));
        IntVector b = IntVector.materialized(new int[] {10, 20, 30}, allValid(3));
        Map<ColumnPath, ColumnVector> children = Map.of(aPath, a, bPath, b);
        BitSet validBits = new BitSet(3);
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);

        StructVector vec = new StructVector(children, validity, 3);

        assertThat(vec.size()).isEqualTo(3);
        assertThat(vec.children()).containsKeys(aPath, bPath);
    }

    @Test
    void nullGroupVsAllNullChildren() {
        // Both rows have null children; row 0 is a non-null group, row 1 is a null group.
        IntVector a = IntVector.materialized(new int[] {0, 0}, Validity.of(new BitSet(2), 2));
        Map<ColumnPath, ColumnVector> children = Map.of(ColumnPath.of("s", "a"), a);

        BitSet groupValidBits = new BitSet(2);
        groupValidBits.set(0); // only row 0 group is non-null
        Validity groupValidity = Validity.of(groupValidBits, 2);

        StructVector vec = new StructVector(children, groupValidity, 2);

        assertThat(vec.validity().isValid(0)).isTrue();
        assertThat(vec.validity().isValid(1)).isFalse();
    }

    @Test
    void selectAllReturnsTheSameVector() {
        IntVector a = IntVector.materialized(new int[] {1, 2, 3}, allValid(3));
        StructVector vec = new StructVector(Map.of(ColumnPath.of("s", "a"), a), allValid(3), 3);
        assertThat(vec.select(Selection.ALL)).isSameAs(vec);
    }

    @Test
    void selectPropagatesIntoRowParallelChildren() {
        ColumnPath aPath = ColumnPath.of("s", "a");
        ColumnPath bPath = ColumnPath.of("s", "b");
        IntVector a = IntVector.materialized(new int[] {1, 2, 3, 4}, allValid(4));
        IntVector b = IntVector.materialized(new int[] {10, 20, 30, 40}, allValid(4));
        BitSet groupValid = new BitSet(4);
        groupValid.set(0, 4);
        StructVector vec = new StructVector(Map.of(aPath, a, bPath, b), Validity.of(groupValid, 4), 4);

        BitSet survivors = new BitSet();
        survivors.set(1);
        survivors.set(3);
        StructVector selected = (StructVector) vec.select(Selection.bits(survivors));

        assertThat(selected.size()).isEqualTo(2);
        IntVector selectedA = (IntVector) selected.children().get(aPath);
        IntVector selectedB = (IntVector) selected.children().get(bPath);
        assertThat(selectedA.getInt(0)).isEqualTo(2);
        assertThat(selectedA.getInt(1)).isEqualTo(4);
        assertThat(selectedB.getInt(0)).isEqualTo(20);
        assertThat(selectedB.getInt(1)).isEqualTo(40);
    }

    private static Validity allValid(int n) {
        return Validity.allValid(n);
    }
}
