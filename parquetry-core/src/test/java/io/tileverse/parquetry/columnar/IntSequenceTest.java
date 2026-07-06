/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;

class IntSequenceTest {

    private static final ValueLayout.OfInt LE_I32 = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    @Test
    void heapBackingReadsValues() {
        IntSequence seq = IntSequence.of(new int[] {3, 1, 4, 1, 5});
        assertThat(seq.size()).isEqualTo(5);
        assertThat(seq.get(0)).isEqualTo(3);
        assertThat(seq.get(4)).isEqualTo(5);
    }

    @Test
    void segmentBackingReadsValues() {
        MemorySegment seg = MemorySegment.ofArray(new byte[4 * Integer.BYTES]);
        for (int i = 0; i < 4; i++) {
            seg.setAtIndex(LE_I32, i, (i + 1) * 10);
        }
        IntSequence seq = IntSequence.ofSegment(seg, 4);
        assertThat(seq.size()).isEqualTo(4);
        assertThat(seq.get(0)).isEqualTo(10);
        assertThat(seq.get(3)).isEqualTo(40);
    }

    @Test
    void getRejectsReadsPastSizeIntoAnOversizedSegment() {
        MemorySegment seg = MemorySegment.ofArray(new byte[6 * Integer.BYTES]);
        for (int i = 0; i < 6; i++) {
            seg.setAtIndex(LE_I32, i, i + 100);
        }
        IntSequence seq = IntSequence.ofSegment(seg, 4);
        assertThat(seq.get(3)).isEqualTo(103);
        assertThatThrownBy(() -> seq.get(4)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void getRejectsOutOfRangeIndexes() {
        IntSequence seq = IntSequence.of(new int[] {1, 2});
        assertThatThrownBy(() -> seq.get(2)).isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> seq.get(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void heapBytesCountsOnlyTheHeapBacking() {
        assertThat(IntSequence.of(new int[] {1, 2, 3}).heapBytes()).isEqualTo(3L * Integer.BYTES);
        assertThat(IntSequence.ofSegment(MemorySegment.ofArray(new byte[12]), 3).heapBytes())
                .isZero();
    }

    @Test
    void copyIntoExportsBothBackingsIdentically() {
        int[] values = {7, -1, 0, Integer.MAX_VALUE};
        MemorySegment seg = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        MemorySegment.copy(values, 0, seg, LE_I32, 0L, values.length);

        MemorySegment fromHeap = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        MemorySegment fromSegment = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        IntSequence.of(values).copyInto(fromHeap, 0L);
        IntSequence.ofSegment(seg, values.length).copyInto(fromSegment, 0L);

        assertThat(fromHeap.toArray(LE_I32)).containsExactly(values).isEqualTo(fromSegment.toArray(LE_I32));
    }

    @Test
    void windowedCopyIntoExportsTheRequestedRangeForBothBackings() {
        int[] values = {5, 6, 7, 8, 9, 10};
        MemorySegment seg = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        MemorySegment.copy(values, 0, seg, LE_I32, 0L, values.length);
        long dstOffset = 4L;
        int count = 3;

        for (IntSequence seq : new IntSequence[] {IntSequence.of(values), IntSequence.ofSegment(seg, values.length)}) {
            long trailing = 4L;
            MemorySegment dst = MemorySegment.ofArray(new byte[(int) (dstOffset + count * Integer.BYTES + trailing)]);
            dst.fill((byte) 0x5A);
            seq.copyInto(2, count, dst, dstOffset);

            byte[] beforeOffset = dst.asSlice(0L, dstOffset).toArray(ValueLayout.JAVA_BYTE);
            assertThat(beforeOffset).containsOnly((byte) 0x5A);
            int[] landed = dst.asSlice(dstOffset, (long) count * Integer.BYTES).toArray(LE_I32);
            assertThat(landed).containsExactly(7, 8, 9);
            byte[] afterWindow = dst.asSlice(dstOffset + count * Integer.BYTES).toArray(ValueLayout.JAVA_BYTE);
            assertThat(afterWindow).containsOnly((byte) 0x5A);
        }
    }

    @Test
    void windowedCopyIntoRejectsRangesOutsideTheSequence() {
        int[] values = {1, 2, 3, 4};
        MemorySegment seg = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        MemorySegment.copy(values, 0, seg, LE_I32, 0L, values.length);
        MemorySegment dst = MemorySegment.ofArray(new byte[16]);

        for (IntSequence seq : new IntSequence[] {IntSequence.of(values), IntSequence.ofSegment(seg, values.length)}) {
            assertThatThrownBy(() -> seq.copyInto(2, 3, dst, 0L)).isInstanceOf(IndexOutOfBoundsException.class);
            assertThatThrownBy(() -> seq.copyInto(-1, 2, dst, 0L)).isInstanceOf(IndexOutOfBoundsException.class);
        }
    }

    @Test
    void copyIntoWritesAtTheGivenByteOffsetForBothBackings() {
        int[] values = {11, 22, 33};
        MemorySegment seg = MemorySegment.ofArray(new byte[values.length * Integer.BYTES]);
        MemorySegment.copy(values, 0, seg, LE_I32, 0L, values.length);
        long dstOffset = 8L;

        for (IntSequence seq : new IntSequence[] {IntSequence.of(values), IntSequence.ofSegment(seg, values.length)}) {
            MemorySegment dst = MemorySegment.ofArray(new byte[(int) dstOffset + values.length * Integer.BYTES]);
            dst.fill((byte) 0x5A);
            seq.copyInto(dst, dstOffset);

            byte[] beforeOffset = dst.asSlice(0L, dstOffset).toArray(ValueLayout.JAVA_BYTE);
            assertThat(beforeOffset).containsOnly((byte) 0x5A);
            int[] landed = dst.asSlice(dstOffset).toArray(LE_I32);
            assertThat(landed).containsExactly(values);
        }
    }
}
