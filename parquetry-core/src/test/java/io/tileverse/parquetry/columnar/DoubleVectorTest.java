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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.testsupport.VectorArrays;

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
        assertThat(VectorArrays.toArray(vector)).containsExactly(4.0, 5.0);
    }

    @Nested
    class CopyInto {

        @Test
        void copiesHeapBackedValuesAtTargetOffset() {
            DoubleVector vec = DoubleVector.materialized(new double[] {10d, 20d, 30d, 40d}, Validity.allValid(4));

            MemorySegment target = MemorySegment.ofArray(new byte[6 * Double.BYTES]);
            vec.copyInto(target, (long) Double.BYTES, 1, 2);

            assertThat(target.getAtIndex(DOUBLE, 1)).isEqualTo(20d);
            assertThat(target.getAtIndex(DOUBLE, 2)).isEqualTo(30d);
        }

        @Test
        void copiesSegmentBackedValues() {
            MemorySegment src = MemorySegment.ofArray(new byte[3 * Double.BYTES]);
            MemorySegment.copy(new double[] {7d, 8d, 9d}, 0, src, DOUBLE, 0L, 3);
            DoubleVector vec = DoubleVector.segmentBacked(src, Validity.allValid(3));

            MemorySegment target = MemorySegment.ofArray(new byte[3 * Double.BYTES]);
            vec.copyInto(target, 0L, 0, 3);

            assertThat(target.getAtIndex(DOUBLE, 0)).isEqualTo(7d);
            assertThat(target.getAtIndex(DOUBLE, 2)).isEqualTo(9d);
        }
    }
}
