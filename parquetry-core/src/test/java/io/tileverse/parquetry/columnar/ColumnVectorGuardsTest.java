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

import org.junit.jupiter.api.Test;

class ColumnVectorGuardsTest {

    @Test
    void isNullAndHasNullsDelegateToValidity() {
        BitSet bits = new BitSet(3);
        bits.set(0);
        bits.set(2);
        IntVector vec = IntVector.materialized(new int[] {7, 0, 9}, Validity.of(bits, 3));
        assertThat(vec.isNull(1)).as("row 1 is null").isTrue();
        assertThat(vec.isValid(0)).as("row 0 is valid").isTrue();
        assertThat(vec.hasNulls()).as("the vector has a null").isTrue();
        assertThat(vec.nullCount()).as("one null row").isEqualTo(1);
    }

    @Test
    void noNullVectorReportsNoNulls() {
        IntVector vec = IntVector.materialized(new int[] {1, 2}, Validity.allValid(2));
        assertThat(vec.hasNulls()).as("an all-valid vector has no nulls").isFalse();
        assertThat(vec.nullCount()).as("no null rows").isZero();
        assertThat(vec.isNull(0)).as("row 0 is not null").isFalse();
        assertThat(vec.isValid(1)).as("row 1 is valid in an all-valid vector").isTrue();
    }
}
