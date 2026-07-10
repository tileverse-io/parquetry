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
package io.tileverse.parquetry.record;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.BitSet;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.StructVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.schema.ColumnPath;

class RowApiNestedAccessTest {
    @Test
    void getStructIsSupportedOnTheUnifiedRecord() {
        BitSet validBits = new BitSet();
        validBits.set(0, 1);
        Validity valid = Validity.of(validBits, 1);
        IntVector age = IntVector.materialized(new int[] {52}, valid);
        StructVector person = new StructVector(Map.of(ColumnPath.of("age"), age), valid, 1);
        ParquetRecord unified = new DefaultParquetRecord(RowColumns.ofStruct(null, person), 0);
        assertThatCode(() -> unified.get(ColumnPath.of("age"))).doesNotThrowAnyException();
        assertThat(unified.get(ColumnPath.of("age"))).isEqualTo(52);
    }
}
