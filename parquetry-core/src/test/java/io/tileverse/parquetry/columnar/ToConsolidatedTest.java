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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

class ToConsolidatedTest {

    @Test
    void nonDictionaryVectorReturnsItself() {
        IntVector vector = IntVector.materialized(new int[] {1, 2}, Validity.allValid(2));
        assertThat(vector.toConsolidated()).isSameAs(vector);
    }

    @Test
    void dictionaryBinaryExpandsToConsolidatedHonoringNulls() {
        MemorySegment a = MemorySegment.ofArray("a".getBytes(StandardCharsets.UTF_8));
        MemorySegment bb = MemorySegment.ofArray("bb".getBytes(StandardCharsets.UTF_8));
        BinaryVector dict = BinaryVector.dictionary(
                new MemorySegment[] {a, bb},
                IntSequence.of(new int[] {0, 1, 0, 0}),
                Validity.of(BitSet.valueOf(new long[] {0b1011}), 4)); // row 2 null

        ColumnVector consolidated = dict.toConsolidated();

        assertThat(consolidated).isInstanceOf(BinaryVector.class);
        BinaryVector out = (BinaryVector) consolidated;
        assertThat(out.isDictionary()).isFalse();
        assertThat(out.size()).isEqualTo(4);
        assertThat(out.get(0).toArray(ValueLayout.JAVA_BYTE)).containsExactly('a');
        assertThat(out.get(1).toArray(ValueLayout.JAVA_BYTE)).containsExactly('b', 'b');
        assertThat((Object) out.get(2)).isNull();
        assertThat(out.get(3).toArray(ValueLayout.JAVA_BYTE)).containsExactly('a');
    }

    @Test
    void dictionaryFixedLenExpandsToConsolidated() {
        MemorySegment x = MemorySegment.ofArray(new byte[] {1, 2});
        MemorySegment y = MemorySegment.ofArray(new byte[] {3, 4});
        FixedLenBinaryVector dict = FixedLenBinaryVector.dictionary(
                new MemorySegment[] {x, y}, IntSequence.of(new int[] {1, 0}), 2, Validity.allValid(2));

        ColumnVector consolidated = dict.toConsolidated();

        assertThat(consolidated).isInstanceOf(FixedLenBinaryVector.class);
        FixedLenBinaryVector out = (FixedLenBinaryVector) consolidated;
        assertThat(out.isDictionary()).isFalse();
        assertThat(out.get(0).toArray(ValueLayout.JAVA_BYTE)).containsExactly(3, 4);
        assertThat(out.get(1).toArray(ValueLayout.JAVA_BYTE)).containsExactly(1, 2);
    }

    @Test
    void dictionaryInt96ExpandsToConsolidated() {
        byte[] first = new byte[12];
        byte[] second = new byte[12];
        first[0] = 7;
        second[0] = 9;
        Int96Vector dict = Int96Vector.dictionary(
                new MemorySegment[] {MemorySegment.ofArray(first), MemorySegment.ofArray(second)},
                IntSequence.of(new int[] {1, 0}),
                Validity.allValid(2));

        ColumnVector consolidated = dict.toConsolidated();

        assertThat(consolidated).isInstanceOf(Int96Vector.class);
        Int96Vector out = (Int96Vector) consolidated;
        assertThat(out.isDictionary()).isFalse();
        assertThat(out.get(0).get(ValueLayout.JAVA_BYTE, 0)).isEqualTo((byte) 9);
        assertThat(out.get(1).get(ValueLayout.JAVA_BYTE, 0)).isEqualTo((byte) 7);
    }
}
