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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

class MapVectorTest {

    @Test
    void offsetsPairKeysAndValues() {
        // Row 0: {"a":1, "b":2}; row 1: {}.
        MemorySegment[] keyBytes = {
            MemorySegment.ofArray(new byte[] {'a'}), MemorySegment.ofArray(new byte[] {'b'}),
        };
        BinaryVector keys = BinaryVector.materialized(keyBytes, allValid(2));
        IntVector values = IntVector.materialized(new int[] {1, 2}, allValid(2));
        int[] offsets = {0, 2, 2};
        BitSet validity = new BitSet(2);
        validity.set(0, 2);

        MapVector vec = new MapVector(offsets, keys, values, validity, 2);

        assertThat(vec.size()).isEqualTo(2);
        assertThat(vec.rowOffsetStart(0)).isZero();
        assertThat(vec.rowOffsetEnd(0)).isEqualTo(2);
        assertThat(vec.rowOffsetStart(1)).isEqualTo(2);
        assertThat(vec.rowOffsetEnd(1)).isEqualTo(2);
        assertThat(((BinaryVector) vec.keys()).get(0)).isSameAs(keyBytes[0]);
    }

    @Test
    void nullRowVsEmptyMap() {
        // Row 0 has [a->1], row 1 is null map (no entries; validity false).
        BinaryVector keys =
                BinaryVector.materialized(new MemorySegment[] {MemorySegment.ofArray(new byte[] {'a'})}, allValid(1));
        IntVector values = IntVector.materialized(new int[] {1}, allValid(1));
        int[] offsets = {0, 1, 1};
        BitSet validity = new BitSet(2);
        validity.set(0); // row 1 invalid (null map)

        MapVector vec = new MapVector(offsets, keys, values, validity, 2);

        assertThat(vec.validity().get(0)).isTrue();
        assertThat(vec.validity().get(1)).isFalse();
    }

    @Test
    void constructorValidatesOffsetsLength() {
        BinaryVector keys =
                BinaryVector.materialized(new MemorySegment[] {MemorySegment.ofArray(new byte[] {'a'})}, allValid(1));
        IntVector values = IntVector.materialized(new int[] {1}, allValid(1));
        int[] wrongOffsets = {0, 1};
        BitSet validity = new BitSet(3);
        validity.set(0, 3);

        assertThatThrownBy(() -> new MapVector(wrongOffsets, keys, values, validity, 3))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("offsets length must be size + 1");
    }

    private static BitSet allValid(int n) {
        BitSet b = new BitSet(n);
        b.set(0, n);
        return b;
    }
}
