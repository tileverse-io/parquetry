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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.testsupport.VectorArrays;

class BooleanVectorTest {

    @Test
    void segmentBackedReadsBitsWithoutHeapValues() {
        // rows: 0=true,1=false,2=true,3=true,4=false -> bits 0,2,3 set -> 0b0000_1101
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment bitmap = arena.allocate(1);
            bitmap.set(ValueLayout.JAVA_BYTE, 0L, (byte) 0b0000_1101);
            BooleanVector vector = BooleanVector.segmentBacked(bitmap, 5, Validity.allValid(5));
            assertThat(vector.size()).as("size").isEqualTo(5);
            assertThat(vector.getBoolean(0)).isTrue();
            assertThat(vector.getBoolean(1)).isFalse();
            assertThat(vector.getBoolean(2)).isTrue();
            assertThat(vector.getBoolean(3)).isTrue();
            assertThat(vector.getBoolean(4)).isFalse();
            assertThat(VectorArrays.toArray(vector)).containsExactly(true, false, true, true, false);
            assertThat(vector.approximateHeapBytes())
                    .as("off-heap bitmap is not counted as heap")
                    .isEqualTo(vector.validity().heapBytes());
        }
    }

    @Test
    void materializedHeapModeStillWorks() {
        BooleanVector vector = BooleanVector.materialized(new boolean[] {true, false}, Validity.allValid(2));
        assertThat(vector.getBoolean(0)).isTrue();
        assertThat(VectorArrays.toArray(vector)).containsExactly(true, false);
    }

    @Nested
    class CopyInto {

        @Test
        void packsHeapBackedRowsLsbFirst() {
            boolean[] vals = {true, false, true, true, false, false, false, false, true}; // 9 rows
            BooleanVector vec = BooleanVector.materialized(vals, Validity.allValid(9));

            MemorySegment target = MemorySegment.ofArray(new byte[2]);
            vec.copyInto(target, 0L, 0, 9);

            // byte 0 bits 0,2,3 set = 0b0000_1101 = 0x0D; byte 1 bit 0 set (row 8) = 0x01
            assertThat(target.get(ValueLayout.JAVA_BYTE, 0L)).isEqualTo((byte) 0x0D);
            assertThat(target.get(ValueLayout.JAVA_BYTE, 1L)).isEqualTo((byte) 0x01);
        }

        @Test
        void zeroesDirtyTargetBitsBeforePacking() {
            boolean[] vals = {false, true};
            BooleanVector vec = BooleanVector.materialized(vals, Validity.allValid(2));

            MemorySegment target = MemorySegment.ofArray(new byte[] {(byte) 0xFF});
            vec.copyInto(target, 0L, 0, 2);

            // row0=false, row1=true -> 0b10 = 0x02; the pre-existing 0xFF must be cleared
            assertThat(target.get(ValueLayout.JAVA_BYTE, 0L)).isEqualTo((byte) 0x02);
        }

        @Test
        void packsSegmentBackedRows() {
            MemorySegment bitmap = MemorySegment.ofArray(new byte[] {(byte) 0b0000_0101}); // rows 0 and 2 true
            BooleanVector vec = BooleanVector.segmentBacked(bitmap, 3, Validity.allValid(3));

            MemorySegment target = MemorySegment.ofArray(new byte[1]);
            vec.copyInto(target, 0L, 0, 3);

            assertThat(target.get(ValueLayout.JAVA_BYTE, 0L)).isEqualTo((byte) 0b0000_0101);
        }
    }
}
