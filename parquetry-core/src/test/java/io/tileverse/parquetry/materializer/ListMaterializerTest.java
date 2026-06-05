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
package io.tileverse.parquetry.materializer;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.Validity;

class ListMaterializerTest {

    @Test
    void buildsListOfBoxedPrimitives() {
        IntVector child = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, allValid(5));
        int[] offsets = {0, 2, 2, 5};
        BitSet validity = new BitSet(3);
        validity.set(0, 3);
        ListVector vec = new ListVector(offsets, child, Validity.of(validity, 3), 3);

        assertThat(asList(vec, 0)).containsExactly(10, 20);
        assertThat(asList(vec, 1)).isEmpty();
        assertThat(asList(vec, 2)).containsExactly(30, 40, 50);
    }

    @Test
    void nullRowReturnsNull() {
        IntVector child = IntVector.materialized(new int[] {7}, allValid(1));
        int[] offsets = {0, 1, 1};
        BitSet validity = new BitSet(2);
        validity.set(0);

        ListVector vec = new ListVector(offsets, child, Validity.of(validity, 2), 2);

        assertThat(asList(vec, 0)).containsExactly(7);
        assertThat(ListMaterializer.materializeAt(vec, 1, null)).isNull();
    }

    @Test
    void nestedListOfLists() {
        // Inner: 3 entries total - [10,20], [], [30,40,50]
        IntVector innerChild = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, allValid(5));
        int[] innerOffsets = {0, 2, 2, 5};
        BitSet innerValidity = new BitSet(3);
        innerValidity.set(0, 3);
        ListVector inner = new ListVector(innerOffsets, innerChild, Validity.of(innerValidity, 3), 3);

        // Outer: row 0 spans all 3 inner lists; row 1 is empty.
        int[] outerOffsets = {0, 3, 3};
        BitSet outerValidity = new BitSet(2);
        outerValidity.set(0, 2);
        ListVector outer = new ListVector(outerOffsets, inner, Validity.of(outerValidity, 2), 2);

        List<Object> row0 = asList(outer, 0);
        assertThat(row0).hasSize(3);
        assertThat(asList(row0, 0)).containsExactly(10, 20);
        assertThat(asList(row0, 1)).isEmpty();
        assertThat(asList(row0, 2)).containsExactly(30, 40, 50);

        assertThat(ListMaterializer.materializeAt(outer, 1, null)).isEmpty();
    }

    @Test
    void nullPrimitiveElementMaterializesAsNull() {
        // Child element 0 is null (validity bit clear); its backing slot parks the default 0.
        BitSet childValidity = new BitSet(2);
        childValidity.set(1);
        IntVector child = IntVector.materialized(new int[] {0, 1}, Validity.of(childValidity, 2));
        int[] offsets = {0, 2};
        BitSet validity = new BitSet(1);
        validity.set(0);
        ListVector vec = new ListVector(offsets, child, Validity.of(validity, 1), 1);

        assertThat(asList(vec, 0))
                .as("a null list element reads back as null, not the primitive default 0")
                .containsExactly(null, 1);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(ListVector vec, int row) {
        return (List<Object>) ListMaterializer.materializeAt(vec, row, null);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> asList(List<Object> outer, int index) {
        return (List<Object>) outer.get(index);
    }

    private static Validity allValid(int n) {
        return Validity.allValid(n);
    }
}
