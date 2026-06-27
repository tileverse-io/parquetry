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
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

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
 * Verifies that a {@link StructVector} element inside a {@link ListVector} materializes to a {@link ParquetRecord}
 * sub-record with accessible primitive fields, rather than null.
 */
class ListOfStructMaterializeTest {

    private static final ColumnPath AGE = ColumnPath.of("age");

    /**
     * Schema: root -> list_col (LIST) -> element (struct) -> age (INT32). The list occupies one row with two struct
     * elements.
     */
    @Test
    void structElementsMaterializeAsParquetRecords() {
        // Two struct elements, ages 7 and 8.
        Validity elemValidity = allValid(2);
        IntVector ageVec = IntVector.materialized(new int[] {7, 8}, elemValidity);
        StructVector child = new StructVector(Map.of(AGE, ageVec), elemValidity, 2);

        // One list row spanning both struct elements: offsets [0, 2].
        int[] offsets = {0, 2};
        Validity listValidity = allValid(1);
        ListVector list = new ListVector(offsets, child, listValidity, 1);

        ParquetSchema schema = listOfStructSchema();
        List<?> result = ListMaterializer.materializeAt(list, 0, schema);

        assertThat(result).as("list has two elements").hasSize(2);

        Object elem0 = result.get(0);
        assertThat(elem0).as("element 0 is a ParquetRecord").isInstanceOf(ParquetRecord.class);
        ParquetRecord rec0 = (ParquetRecord) elem0;
        assertThat(rec0.get(AGE)).as("element 0 age").isEqualTo(7);

        Object elem1 = result.get(1);
        assertThat(elem1).as("element 1 is a ParquetRecord").isInstanceOf(ParquetRecord.class);
        ParquetRecord rec1 = (ParquetRecord) elem1;
        assertThat(rec1.get(AGE)).as("element 1 age").isEqualTo(8);
    }

    @Test
    void nullStructElementRemainsNull() {
        // Two elements: first valid (age 42), second null.
        BitSet elemValidBits = new BitSet(2);
        elemValidBits.set(0); // element 1 is null
        Validity elemValidity = Validity.of(elemValidBits, 2);
        IntVector ageVec = IntVector.materialized(new int[] {42, 0}, elemValidity);
        StructVector child = new StructVector(Map.of(AGE, ageVec), elemValidity, 2);

        int[] offsets = {0, 2};
        Validity listValidity = allValid(1);
        ListVector list = new ListVector(offsets, child, listValidity, 1);

        ParquetSchema schema = listOfStructSchema();
        List<?> result = ListMaterializer.materializeAt(list, 0, schema);

        assertThat(result).as("list size").hasSize(2);
        assertThat(result.get(0)).as("valid element is ParquetRecord").isInstanceOf(ParquetRecord.class);
        assertThat(result.get(1)).as("null element is null").isNull();
    }

    // Minimal schema for a list-of-struct with a single int "age" field.
    private static ParquetSchema listOfStructSchema() {
        SchemaNode.Primitive agePrimitive = new SchemaNode.Primitive(
                "age", Repetition.REQUIRED, PrimitiveKind.INT32, OptionalInt.empty(), Optional.empty(), 1);
        SchemaNode.Group root =
                new SchemaNode.Group("root", Repetition.REQUIRED, List.of(agePrimitive), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static Validity allValid(int n) {
        return Validity.allValid(n);
    }
}
