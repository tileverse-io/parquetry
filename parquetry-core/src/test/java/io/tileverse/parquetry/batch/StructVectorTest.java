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
        BitSet validity = new BitSet(3);
        validity.set(0, 3);

        StructVector vec = new StructVector(children, validity, 3);

        assertThat(vec.size()).isEqualTo(3);
        assertThat(vec.children()).containsKeys(aPath, bPath);
    }

    @Test
    void nullGroupVsAllNullChildren() {
        // Both rows have null children; row 0 is a non-null group, row 1 is a null group.
        IntVector a = IntVector.materialized(new int[] {0, 0}, new BitSet(2));
        Map<ColumnPath, ColumnVector> children = Map.of(ColumnPath.of("s", "a"), a);

        BitSet groupValidity = new BitSet(2);
        groupValidity.set(0); // only row 0 group is non-null

        StructVector vec = new StructVector(children, groupValidity, 2);

        assertThat(vec.validity().get(0)).isTrue();
        assertThat(vec.validity().get(1)).isFalse();
    }

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }
}
