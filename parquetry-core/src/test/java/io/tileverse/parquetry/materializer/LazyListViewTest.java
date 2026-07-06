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
package io.tileverse.parquetry.materializer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.ListVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Verifies that {@link ListMaterializer#materializeAt} returns a lazy, immutable, random-access view over the row's
 * slice of the child vector, for primitive and struct elements alike.
 */
class LazyListViewTest {

    private static final ColumnPath AGE = ColumnPath.of("age");

    @Test
    void sizeAndGetMatchChildVectorValues() {
        IntVector child = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, Validity.allValid(5));
        ListVector vec = listVector(new int[] {0, 2, 5}, child, 2);

        List<?> row0 = ListMaterializer.materializeAt(vec, 0, null);
        List<?> row1 = ListMaterializer.materializeAt(vec, 1, null);

        assertThat(row0).hasSize(2);
        assertThat(row0.get(0)).isEqualTo(child.get(0));
        assertThat(row0.get(1)).isEqualTo(child.get(1));
        assertThat(row1).hasSize(3);
        assertThat(row1.get(0)).isEqualTo(child.get(2));
        assertThat(row1.get(1)).isEqualTo(child.get(3));
        assertThat(row1.get(2)).isEqualTo(child.get(4));
    }

    @Test
    void forEachTraversesInOrder() {
        IntVector child = IntVector.materialized(new int[] {1, 2, 3, 4}, Validity.allValid(4));
        ListVector vec = listVector(new int[] {0, 4}, child, 1);

        List<?> row = ListMaterializer.materializeAt(vec, 0, null);

        List<Object> traversed = new ArrayList<>();
        for (Object element : row) {
            traversed.add(element);
        }
        assertThat(traversed).containsExactly(1, 2, 3, 4);
    }

    @Test
    void mutatorsThrowUnsupportedOperation() {
        IntVector child = IntVector.materialized(new int[] {1, 2}, Validity.allValid(2));
        ListVector vec = listVector(new int[] {0, 2}, child, 1);

        @SuppressWarnings("unchecked")
        List<Object> row = (List<Object>) ListMaterializer.materializeAt(vec, 0, null);

        assertThatThrownBy(() -> row.add(99)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> row.set(0, 99)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> row.remove(0)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void nestedListOfListsReturnsLazyViewsRecursively() {
        IntVector innerChild = IntVector.materialized(new int[] {10, 20, 30, 40, 50}, Validity.allValid(5));
        ListVector inner = listVector(new int[] {0, 2, 2, 5}, innerChild, 3);
        ListVector outer = listVector(new int[] {0, 3}, inner, 1);

        List<?> row = ListMaterializer.materializeAt(outer, 0, null);

        assertThat(row).hasSize(3);
        assertThat(row).isInstanceOf(ListMaterializer.LazyListView.class);
        assertThat(row.get(0)).isInstanceOf(ListMaterializer.LazyListView.class);
        assertThat(nestedList(row, 0)).containsExactly(10, 20);
        assertThat(nestedList(row, 1)).isEmpty();
        assertThat(nestedList(row, 2)).containsExactly(30, 40, 50);
    }

    @Test
    void structElementsResolveCorrectValues() {
        Validity elemValidity = Validity.allValid(2);
        IntVector ageVec = IntVector.materialized(new int[] {7, 8}, elemValidity);
        StructVector child = new StructVector(Map.of(AGE, ageVec), elemValidity, 2);
        ListVector vec = listVector(new int[] {0, 2}, child, 1);

        List<?> row = ListMaterializer.materializeAt(vec, 0, listOfStructSchema());

        ParquetRecord first = (ParquetRecord) row.get(0);
        ParquetRecord second = (ParquetRecord) row.get(1);
        assertThat(first.get(AGE)).isEqualTo(7);
        assertThat(second.get(AGE)).isEqualTo(8);
    }

    @Test
    void nullStructElementYieldsNullAndValidNeighborsResolve() {
        // Three struct elements; the middle one's validity bit is cleared.
        BitSet elemValidBits = new BitSet(3);
        elemValidBits.set(0);
        elemValidBits.set(2);
        Validity elemValidity = Validity.of(elemValidBits, 3);
        IntVector ageVec = IntVector.materialized(new int[] {7, 0, 9}, elemValidity);
        StructVector child = new StructVector(Map.of(AGE, ageVec), elemValidity, 3);
        ListVector vec = listVector(new int[] {0, 3}, child, 1);

        List<?> row = ListMaterializer.materializeAt(vec, 0, listOfStructSchema());

        assertThat(((ParquetRecord) row.get(0)).get(AGE)).isEqualTo(7);
        assertThat(row.get(1)).isNull();
        assertThat(((ParquetRecord) row.get(2)).get(AGE)).isEqualTo(9);
    }

    @Test
    void constructionAndSizeDoNotTouchElements() {
        // A segment-backed child over a closed arena: element access throws, size metadata stays readable.
        MemorySegment closedSegment;
        try (Arena arena = Arena.ofConfined()) {
            closedSegment = arena.allocate(3L * Integer.BYTES);
        }
        IntVector child = IntVector.segmentBacked(closedSegment, Validity.allValid(3));
        ListVector vec = listVector(new int[] {0, 3}, child, 1);

        List<?> row = ListMaterializer.materializeAt(vec, 0, null);

        assertThat(row.size()).isEqualTo(3);
        assertThatThrownBy(() -> row.get(0)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void nullElementYieldsNullFromGet() {
        BitSet childValidBits = new BitSet(2);
        childValidBits.set(1);
        IntVector child = IntVector.materialized(new int[] {0, 5}, Validity.of(childValidBits, 2));
        ListVector vec = listVector(new int[] {0, 2}, child, 1);

        List<?> row = ListMaterializer.materializeAt(vec, 0, null);

        assertThat(row.get(0)).isNull();
        assertThat(row.get(1)).isEqualTo(5);
    }

    @Test
    void nullRowReturnsNullAndEmptyRowReturnsEmptyList() {
        IntVector child = IntVector.materialized(new int[] {7}, Validity.allValid(1));
        int[] offsets = {0, 1, 1};
        BitSet validBits = new BitSet(2);
        validBits.set(0);
        ListVector vec = new ListVector(offsets, child, Validity.of(validBits, 2), 2);

        assertThat(ListMaterializer.materializeAt(vec, 1, null)).isNull();

        ListVector allValidVec = new ListVector(offsets, child, Validity.allValid(2), 2);
        assertThat(ListMaterializer.materializeAt(allValidVec, 1, null)).isEmpty();
    }

    @SuppressWarnings("unchecked")
    private static List<Object> nestedList(List<?> outer, int index) {
        return (List<Object>) outer.get(index);
    }

    private static ListVector listVector(int[] offsets, ColumnVector child, int rows) {
        return new ListVector(offsets, child, Validity.allValid(rows), rows);
    }

    private static ParquetSchema listOfStructSchema() {
        SchemaNode.Primitive agePrimitive = new SchemaNode.Primitive(
                "age", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(agePrimitive), Optional.empty(), -1);
        return new ParquetSchema(root);
    }
}
