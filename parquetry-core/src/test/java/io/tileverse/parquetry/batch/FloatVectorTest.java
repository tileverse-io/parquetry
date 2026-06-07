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
}
