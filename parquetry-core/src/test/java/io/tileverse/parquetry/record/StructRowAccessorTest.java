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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.materializer.RowAccessor;
import io.tileverse.parquetry.schema.ColumnPath;

class StructRowAccessorTest {

    @Test
    void readsChildLeafByRelativePath() {
        Validity allValid = Validity.allValid(2);
        IntVector ages = IntVector.materialized(new int[] {30, 41}, allValid);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), ages), allValid, 2);

        RowAccessor row = new StructRowAccessor(person, 1, null);

        assertThat(row.get(ColumnPath.of("age")))
                .as("child leaf at relative path")
                .isEqualTo(41);
    }

    @Test
    void reportsNullChildLeafThroughValidity() {
        BitSet validBits = new BitSet();
        validBits.set(0); // row 0 valid, row 1 null
        Validity validity = Validity.of(validBits, 2);
        IntVector ages = IntVector.materialized(new int[] {30, 0}, validity);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), ages), validity, 2);

        RowAccessor row = new StructRowAccessor(person, 1, null);

        assertThat(row.get(ColumnPath.of("age"))).as("null child leaf").isNull();
    }

    @Test
    void valuesSnapshotContainsAllChildren() {
        Validity allValid = Validity.allValid(2);
        IntVector ages = IntVector.materialized(new int[] {30, 41}, allValid);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), ages), allValid, 2);

        RowAccessor row = new StructRowAccessor(person, 0, null);
        Map<ColumnPath, Object> values = row.values();

        assertThat(values).as("values snapshot").containsKey(ColumnPath.of("age"));
        assertThat(values.get(ColumnPath.of("age"))).as("value in snapshot").isEqualTo(30);
    }

    @Test
    void isGroupNullReturnsTrueWhenStructChildIsNull() {
        BitSet outerValidBits = new BitSet();
        outerValidBits.set(0, 2);
        Validity outerValid = Validity.of(outerValidBits, 2);
        BitSet innerValidBits = new BitSet();
        innerValidBits.set(0); // row 0 valid, row 1 null inner struct
        Validity innerValid = Validity.of(innerValidBits, 2);

        IntVector ages = IntVector.materialized(new int[] {30, 41}, outerValid);
        StructVector inner = new StructVector(Map.of(ColumnPath.of("x"), ages), innerValid, 2);
        StructVector outer = new StructVector(Map.of(ColumnPath.of("inner"), inner), outerValid, 2);

        RowAccessor row = new StructRowAccessor(outer, 1, null);

        assertThat(row.isGroupNull(ColumnPath.of("inner")))
                .as("null inner struct")
                .isTrue();
    }

    @Test
    void isGroupNullReturnsFalseForPrimitiveChild() {
        Validity allValid = Validity.allValid(2);
        IntVector ages = IntVector.materialized(new int[] {30, 41}, allValid);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), ages), allValid, 2);

        RowAccessor row = new StructRowAccessor(person, 0, null);

        assertThat(row.isGroupNull(ColumnPath.of("age")))
                .as("primitive child is not a group")
                .isFalse();
    }

    @Test
    void constructorRejectsOutOfBoundsRowIndex() {
        BitSet validBits = new BitSet();
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        IntVector ages = IntVector.materialized(new int[] {30, 41}, validity);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), ages), validity, 2);

        assertThatThrownBy(() -> new StructRowAccessor(person, 2, null))
                .as("rowIndex == size out of bounds")
                .isInstanceOf(IndexOutOfBoundsException.class);

        assertThatThrownBy(() -> new StructRowAccessor(person, -1, null))
                .as("negative rowIndex out of bounds")
                .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void valuesSnapshotIsUnmodifiable() {
        Validity allValid = Validity.allValid(2);
        IntVector ages = IntVector.materialized(new int[] {30, 41}, allValid);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), ages), allValid, 2);

        RowAccessor row = new StructRowAccessor(person, 0, null);
        Map<ColumnPath, Object> values = row.values();

        assertThatThrownBy(() -> values.put(ColumnPath.of("extra"), 99))
                .as("values snapshot must be unmodifiable")
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
