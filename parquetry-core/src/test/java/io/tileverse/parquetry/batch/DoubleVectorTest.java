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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

class DoubleVectorTest {

    @Test
    void segmentBackedReadsValuesWithoutHeapValues() {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment values = arena.allocate(3 * Double.BYTES);
            values.setAtIndex(DOUBLE, 0, 1.5);
            values.setAtIndex(DOUBLE, 1, 2.5);
            values.setAtIndex(DOUBLE, 2, 3.5);
            DoubleVector vector = DoubleVector.segmentBacked(values, Validity.allValid(3));
            assertThat(vector.size()).as("size").isEqualTo(3);
            assertThat(vector.getDouble(0)).isEqualTo(1.5);
            assertThat(vector.getDouble(2)).isEqualTo(3.5);
            assertThat(vector.<Double>get(1)).isEqualTo(2.5);
            assertThat(vector.approximateHeapBytes())
                    .as("off-heap values are not counted as heap")
                    .isEqualTo(vector.validity().heapBytes());
        }
    }

    @Test
    void materializedHeapModeStillWorks() {
        DoubleVector vector = DoubleVector.materialized(new double[] {4.0, 5.0}, Validity.allValid(2));
        assertThat(vector.getDouble(1)).isEqualTo(5.0);
        assertThat(vector.asArray()).containsExactly(4.0, 5.0);
    }
}
