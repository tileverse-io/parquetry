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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.junit.jupiter.api.Test;

class HalfFloatsTest {

    private static final int PATTERNS = 1 << Short.SIZE;

    @Test
    void theOrderKeyOfEachPatternGivesItsBitsBack() {
        for (int pattern = 0; pattern < PATTERNS; pattern++) {
            short bits = (short) pattern;

            assertThat(HalfFloats.bits(HalfFloats.key(bits))).isEqualTo(bits);
        }
    }

    @Test
    void orderKeysGrowWithTheValueAndPutNegativeZeroBeforePositiveZero() {
        List<Short> numbers = new ArrayList<>();
        for (int pattern = 0; pattern < PATTERNS; pattern++) {
            short bits = (short) pattern;
            if (!Float.isNaN(Float.float16ToFloat(bits))) {
                numbers.add(bits);
            }
        }
        // Float.compare orders by value and puts -0.0 before +0.0.
        numbers.sort(Comparator.comparing(Float::float16ToFloat, Float::compare));

        for (int i = 1; i < numbers.size(); i++) {
            short lesser = numbers.get(i - 1);
            short greater = numbers.get(i);

            assertThat(HalfFloats.key(lesser))
                    .as("%s before %s", Float.float16ToFloat(lesser), Float.float16ToFloat(greater))
                    .isLessThan(HalfFloats.key(greater));
        }
    }

    @Test
    void aCellIsNaNWhenItsHalfFloatIs() {
        for (int pattern = 0; pattern < PATTERNS; pattern++) {
            short bits = (short) pattern;
            MemorySegment cell = MemorySegment.ofArray(HalfFloats.encode(bits));

            assertThat(HalfFloats.isNaN(cell))
                    .as("pattern 0x%04x", pattern)
                    .isEqualTo(Float.isNaN(Float.float16ToFloat(bits)));
        }
    }

    @Test
    void aCellStoresItsHalfFloatLittleEndian() {
        byte[] cell = HalfFloats.encode((short) 0xC000);

        assertThat(cell).containsExactly(0x00, 0xC0);
        assertThat(HalfFloats.key(MemorySegment.ofArray(cell))).isEqualTo(HalfFloats.key((short) 0xC000));
    }
}
