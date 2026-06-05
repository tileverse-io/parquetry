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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.BitSet;

import org.junit.jupiter.api.Test;

class ColumnVectorHeapBytesTest {

    @Test
    void intVectorCountsFourBytesPerElement() {
        BitSet validBits = new BitSet(3);
        validBits.set(0, 3);
        Validity validity = Validity.of(validBits, 3);
        IntVector vector = IntVector.materialized(new int[] {1, 2, 3}, validity);
        assertThat(vector.approximateHeapBytes()).isGreaterThanOrEqualTo(12L);
    }

    @Test
    void longVectorCountsEightBytesPerElement() {
        BitSet validBits = new BitSet(2);
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        LongVector vector = LongVector.materialized(new long[] {1L, 2L}, validity);
        assertThat(vector.approximateHeapBytes()).isGreaterThanOrEqualTo(16L);
    }

    @Test
    void binaryVectorSumsSegmentSizes() {
        Arena arena = Arena.ofConfined();
        MemorySegment a = arena.allocate(10);
        MemorySegment b = arena.allocate(20);
        BitSet validBits = new BitSet(2);
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        BinaryVector vector = BinaryVector.materialized(new MemorySegment[] {a, b}, validity);
        assertThat(vector.approximateHeapBytes()).isGreaterThanOrEqualTo(30L);
        arena.close();
    }

    @Test
    void binaryVectorIgnoresNullSegments() {
        Validity validity = Validity.of(new BitSet(1), 1);
        BinaryVector vector = BinaryVector.materialized(new MemorySegment[] {null}, validity);
        assertThat(vector.approximateHeapBytes()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    void listVectorIncludesChildBytes() {
        BitSet childValidBits = new BitSet(3);
        childValidBits.set(0, 3);
        Validity childValidity = Validity.of(childValidBits, 3);
        IntVector child = IntVector.materialized(new int[] {1, 2, 3}, childValidity);
        BitSet validBits = new BitSet(2);
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        ListVector list = new ListVector(new int[] {0, 2, 3}, child, validity, 2);
        assertThat(list.approximateHeapBytes()).isGreaterThanOrEqualTo(child.approximateHeapBytes());
    }

    @Test
    void fixedLenBinaryVectorUsesByteWidth() {
        Arena arena = Arena.ofConfined();
        MemorySegment a = arena.allocate(4);
        MemorySegment b = arena.allocate(4);
        BitSet validBits = new BitSet(2);
        validBits.set(0, 2);
        Validity validity = Validity.of(validBits, 2);
        a.set(JAVA_BYTE, 0, (byte) 1);
        b.set(JAVA_BYTE, 0, (byte) 2);
        FixedLenBinaryVector vector = FixedLenBinaryVector.materialized(new MemorySegment[] {a, b}, 4, validity);
        assertThat(vector.approximateHeapBytes()).isGreaterThanOrEqualTo(8L);
        arena.close();
    }
}
