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
package io.tileverse.parquetry.batch;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;

class ConstantVectorsTest {

    @Test
    void fillsIntConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.IntVal(2024), 3);
        assertThat(vector).isInstanceOf(IntVector.class);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(((IntVector) vector).asArray()).containsExactly(2024, 2024, 2024);
    }

    @Test
    void fillsLongConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.LongVal(9000000000L), 2);
        assertThat(vector).isInstanceOf(LongVector.class);
        assertThat(((LongVector) vector).asArray()).containsExactly(9000000000L, 9000000000L);
    }

    @Test
    void fillsFloatConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.FloatVal(1.5f), 2);
        assertThat(vector).isInstanceOf(FloatVector.class);
        assertThat(vector.size()).isEqualTo(2);
        assertThat(((FloatVector) vector).asArray()).containsExactly(1.5f, 1.5f);
    }

    @Test
    void fillsDateAsEpochDayInt() {
        ColumnVector vector = ConstantVectors.of(new Value.DateVal(LocalDate.parse("2024-01-15")), 2);
        assertThat(vector).isInstanceOf(IntVector.class);
        int epochDay = (int) LocalDate.parse("2024-01-15").toEpochDay();
        assertThat(((IntVector) vector).asArray()).containsExactly(epochDay, epochDay);
    }

    @Test
    void fillsStringConstant() {
        ColumnVector vector = ConstantVectors.of(new Value.StringVal("buildings"), 2);
        assertThat(vector).isInstanceOf(BinaryVector.class);
        assertThat(vector.size()).isEqualTo(2);
    }

    @Test
    void nullValueFillsAllNull() {
        ColumnVector vector = ConstantVectors.ofNull(new Value.IntVal(0), 3);
        assertThat(vector.size()).isEqualTo(3);
        assertThat(vector.isNull(0)).isTrue();
        assertThat(vector.isNull(2)).isTrue();
    }
}
