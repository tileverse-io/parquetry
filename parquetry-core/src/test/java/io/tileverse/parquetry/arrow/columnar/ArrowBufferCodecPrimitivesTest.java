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
package io.tileverse.parquetry.arrow.columnar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.Validity;

class ArrowBufferCodecPrimitivesTest {

    @Test
    void intVectorRoundTripsAllValid() {
        IntVector original = IntVector.materialized(new int[] {7, -3, 9}, Validity.allValid(3));

        IntVector restored = (IntVector) roundTrip(original, LeafType.INT32);

        assertThat(restored.asArray()).containsExactly(7, -3, 9);
        assertThat(restored.hasNulls()).isFalse();
    }

    @Test
    void intVectorRoundTripsWithNulls() {
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(2); // row 1 null
        IntVector original = IntVector.materialized(new int[] {7, 0, 9}, Validity.of(valid, 3));

        IntVector restored = (IntVector) roundTrip(original, LeafType.INT32);

        assertThat(restored.asArray()).containsExactly(7, 0, 9);
        assertThat(restored.isNull(1)).isTrue();
        assertThat(restored.isValid(0)).isTrue();
        assertThat(restored.isValid(2)).isTrue();
    }

    @Test
    void longVectorRoundTripsAllValid() {
        LongVector original = LongVector.materialized(new long[] {10L, -20L, Long.MAX_VALUE}, Validity.allValid(3));

        LongVector restored = (LongVector) roundTrip(original, LeafType.INT64);

        assertThat(restored.asArray()).containsExactly(10L, -20L, Long.MAX_VALUE);
        assertThat(restored.hasNulls()).isFalse();
    }

    @Test
    void longVectorRoundTripsWithNulls() {
        BitSet valid = new BitSet(3);
        valid.set(1); // rows 0 and 2 null
        LongVector original = LongVector.materialized(new long[] {0L, 42L, 0L}, Validity.of(valid, 3));

        LongVector restored = (LongVector) roundTrip(original, LeafType.INT64);

        assertThat(restored.asArray()).containsExactly(0L, 42L, 0L);
        assertThat(restored.isNull(0)).isTrue();
        assertThat(restored.isValid(1)).isTrue();
        assertThat(restored.isNull(2)).isTrue();
    }

    @Test
    void floatVectorRoundTripsAllValid() {
        FloatVector original = FloatVector.materialized(new float[] {1.5f, -2.25f, 3.75f}, Validity.allValid(3));

        FloatVector restored = (FloatVector) roundTrip(original, LeafType.FLOAT);

        assertThat(restored.asArray()).containsExactly(1.5f, -2.25f, 3.75f);
        assertThat(restored.hasNulls()).isFalse();
    }

    @Test
    void floatVectorRoundTripsWithNulls() {
        BitSet valid = new BitSet(3);
        valid.set(0);
        valid.set(1); // row 2 null
        FloatVector original = FloatVector.materialized(new float[] {1.5f, -2.25f, 0f}, Validity.of(valid, 3));

        FloatVector restored = (FloatVector) roundTrip(original, LeafType.FLOAT);

        assertThat(restored.asArray()).containsExactly(1.5f, -2.25f, 0f);
        assertThat(restored.isValid(0)).isTrue();
        assertThat(restored.isNull(2)).isTrue();
    }

    @Test
    void doubleVectorRoundTripsAllValid() {
        DoubleVector original = DoubleVector.materialized(new double[] {1.5d, -2.25d, 3.75d}, Validity.allValid(3));

        DoubleVector restored = (DoubleVector) roundTrip(original, LeafType.DOUBLE);

        assertThat(restored.asArray()).containsExactly(1.5d, -2.25d, 3.75d);
        assertThat(restored.hasNulls()).isFalse();
    }

    @Test
    void doubleVectorRoundTripsWithNulls() {
        BitSet valid = new BitSet(3);
        valid.set(2); // rows 0 and 1 null
        DoubleVector original = DoubleVector.materialized(new double[] {0d, 0d, 9.5d}, Validity.of(valid, 3));

        DoubleVector restored = (DoubleVector) roundTrip(original, LeafType.DOUBLE);

        assertThat(restored.asArray()).containsExactly(0d, 0d, 9.5d);
        assertThat(restored.isNull(0)).isTrue();
        assertThat(restored.isNull(1)).isTrue();
        assertThat(restored.isValid(2)).isTrue();
    }

    @Test
    void booleanVectorRoundTripsAllValid() {
        BooleanVector original =
                BooleanVector.materialized(new boolean[] {true, false, true, true}, Validity.allValid(4));

        BooleanVector restored = (BooleanVector) roundTrip(original, LeafType.BOOLEAN);

        assertThat(restored.asArray()).containsExactly(true, false, true, true);
        assertThat(restored.hasNulls()).isFalse();
    }

    @Test
    void booleanVectorRoundTripsWithNulls() {
        BitSet valid = new BitSet(4);
        valid.set(0);
        valid.set(3); // rows 1 and 2 null
        BooleanVector original =
                BooleanVector.materialized(new boolean[] {true, false, false, false}, Validity.of(valid, 4));

        BooleanVector restored = (BooleanVector) roundTrip(original, LeafType.BOOLEAN);

        assertThat(restored.asArray()).containsExactly(true, false, false, false);
        assertThat(restored.isValid(0)).isTrue();
        assertThat(restored.isNull(1)).isTrue();
        assertThat(restored.isNull(2)).isTrue();
        assertThat(restored.isValid(3)).isTrue();
    }

    private static ColumnVector roundTrip(ColumnVector vector, LeafType type) {
        EncodedNode node = ArrowBufferCodec.encode(vector);
        return ArrowBufferCodec.decode(node, type);
    }
}
