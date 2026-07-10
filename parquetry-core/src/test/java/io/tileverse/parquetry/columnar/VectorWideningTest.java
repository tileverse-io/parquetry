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
package io.tileverse.parquetry.columnar;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.schema.PrimitiveKind;

class VectorWideningTest {

    @Test
    void widensIntToLongPreservingNulls() {
        BitSet valid = new BitSet();
        valid.set(0);
        valid.set(2);
        Validity v = Validity.of(valid, 3);
        IntVector src = IntVector.materialized(new int[] {7, 0, 9}, v);

        ColumnVector out = VectorWidening.widen(src, PrimitiveKind.INT64);

        assertThat(out).isInstanceOf(LongVector.class);
        LongVector lv = (LongVector) out;
        assertThat(lv.getLong(0)).isEqualTo(7L);
        assertThat(lv.isNull(1)).isTrue();
        assertThat(lv.getLong(2)).isEqualTo(9L);
    }

    @Test
    void widensFloatToDouble() {
        BitSet valid = new BitSet();
        valid.set(0);
        valid.set(1);
        Validity v = Validity.of(valid, 2);
        FloatVector src = FloatVector.materialized(new float[] {1.5f, 2.5f}, v);

        ColumnVector out = VectorWidening.widen(src, PrimitiveKind.DOUBLE);

        assertThat(out).isInstanceOf(DoubleVector.class);
        DoubleVector dv = (DoubleVector) out;
        assertThat(dv.getDouble(0)).isEqualTo(1.5d);
        assertThat(dv.getDouble(1)).isEqualTo(2.5d);
    }

    @Test
    void rejectsUnsupportedWidening() {
        IntVector src = IntVector.materialized(new int[] {1}, Validity.allValid(1));

        assertThatThrownBy(() -> VectorWidening.widen(src, PrimitiveKind.FLOAT))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
