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

import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class FloatVectorTest {

    @Test
    void segmentBackedReadsValuesWithoutHeapValues() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment values = arena.allocate(3L * Float.BYTES);
            values.setAtIndex(FLOAT, 0, 1.5f);
            values.setAtIndex(FLOAT, 1, 2.5f);
            values.setAtIndex(FLOAT, 2, 3.5f);
            FloatVector vector = FloatVector.segmentBacked(values, Validity.allValid(3));
            assertThat(vector.size()).as("size").isEqualTo(3);
            assertThat(vector.getFloat(0)).isEqualTo(1.5f);
            assertThat(vector.getFloat(2)).isEqualTo(3.5f);
            assertThat(vector.<Float>get(1)).isEqualTo(2.5f);
            assertThat(vector.approximateHeapBytes())
                    .as("off-heap values are not counted as heap")
                    .isEqualTo(vector.validity().heapBytes());
        }
    }

    @Test
    void materializedHeapModeStillWorks() {
        FloatVector vector = FloatVector.materialized(new float[] {4f, 5f}, Validity.allValid(2));
        assertThat(vector.getFloat(1)).isEqualTo(5f);
        assertThat(vector.asArray()).containsExactly(4f, 5f);
    }

    @Nested
    class CopyInto {

        @Test
        void copiesHeapBackedValuesAtTargetOffset() {
            FloatVector vec = FloatVector.materialized(new float[] {10f, 20f, 30f, 40f}, Validity.allValid(4));

            MemorySegment target = MemorySegment.ofArray(new byte[6 * Float.BYTES]);
            vec.copyInto(target, (long) Float.BYTES, 1, 2);

            assertThat(target.getAtIndex(FLOAT, 1)).isEqualTo(20f);
            assertThat(target.getAtIndex(FLOAT, 2)).isEqualTo(30f);
        }

        @Test
        void copiesSegmentBackedValues() {
            MemorySegment src = MemorySegment.ofArray(new byte[3 * Float.BYTES]);
            MemorySegment.copy(new float[] {7f, 8f, 9f}, 0, src, FLOAT, 0L, 3);
            FloatVector vec = FloatVector.segmentBacked(src, Validity.allValid(3));

            MemorySegment target = MemorySegment.ofArray(new byte[3 * Float.BYTES]);
            vec.copyInto(target, 0L, 0, 3);

            assertThat(target.getAtIndex(FLOAT, 0)).isEqualTo(7f);
            assertThat(target.getAtIndex(FLOAT, 2)).isEqualTo(9f);
        }
    }
}
