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

import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class LongVectorTest {

    @Test
    void segmentBackedReadsValuesWithoutHeapValues() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment values = arena.allocate(3L * Long.BYTES);
            values.setAtIndex(INT64, 0, 10L);
            values.setAtIndex(INT64, 1, 20L);
            values.setAtIndex(INT64, 2, 30L);
            LongVector vector = LongVector.segmentBacked(values, Validity.allValid(3));
            assertThat(vector.size()).as("size").isEqualTo(3);
            assertThat(vector.getLong(0)).isEqualTo(10L);
            assertThat(vector.getLong(2)).isEqualTo(30L);
            assertThat(vector.<Long>get(1)).isEqualTo(20L);
            assertThat(vector.approximateHeapBytes())
                    .as("off-heap values are not counted as heap")
                    .isEqualTo(vector.validity().heapBytes());
        }
    }

    @Test
    void materializedHeapModeStillWorks() {
        LongVector vector = LongVector.materialized(new long[] {4L, 5L}, Validity.allValid(2));
        assertThat(vector.getLong(1)).isEqualTo(5L);
        assertThat(vector.asArray()).containsExactly(4L, 5L);
    }

    @Nested
    class CopyInto {

        @Test
        void copiesHeapBackedValuesAtTargetOffset() {
            LongVector vec = LongVector.materialized(new long[] {10L, 20L, 30L, 40L}, Validity.allValid(4));

            MemorySegment target = MemorySegment.ofArray(new byte[6 * Long.BYTES]);
            vec.copyInto(target, (long) Long.BYTES, 1, 2);

            assertThat(target.getAtIndex(INT64, 1)).isEqualTo(20L);
            assertThat(target.getAtIndex(INT64, 2)).isEqualTo(30L);
        }

        @Test
        void copiesSegmentBackedValues() {
            MemorySegment src = MemorySegment.ofArray(new byte[3 * Long.BYTES]);
            MemorySegment.copy(new long[] {7L, 8L, 9L}, 0, src, INT64, 0L, 3);
            LongVector vec = LongVector.segmentBacked(src, Validity.allValid(3));

            MemorySegment target = MemorySegment.ofArray(new byte[3 * Long.BYTES]);
            vec.copyInto(target, 0L, 0, 3);

            assertThat(target.getAtIndex(INT64, 0)).isEqualTo(7L);
            assertThat(target.getAtIndex(INT64, 2)).isEqualTo(9L);
        }
    }
}
