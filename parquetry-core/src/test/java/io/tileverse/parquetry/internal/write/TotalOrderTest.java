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

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * The keys of {@link TotalOrder} order the bit patterns of FLOAT and DOUBLE values as IEEE 754 total order does:
 * negative NaNs from the largest payload down, negative numbers, both zeros with the negative one first, positive
 * numbers, positive NaNs from the smallest payload up. Each key gives its bit pattern back.
 */
class TotalOrderTest {

    private static final List<Integer> FLOAT_PATTERNS_IN_TOTAL_ORDER = List.of(
            0xFFFFFFFF, // negative NaN, largest payload
            0xFFC00001, // negative quiet NaN with a payload
            0xFFC00000, // negative quiet NaN
            0xFF800001, // negative signaling NaN, smallest payload
            0xFF800000, // negative infinity
            Float.floatToRawIntBits(-Float.MAX_VALUE),
            Float.floatToRawIntBits(-1.0f),
            Float.floatToRawIntBits(-Float.MIN_VALUE),
            Float.floatToRawIntBits(-0.0f),
            Float.floatToRawIntBits(0.0f),
            Float.floatToRawIntBits(Float.MIN_VALUE),
            Float.floatToRawIntBits(1.0f),
            Float.floatToRawIntBits(Float.MAX_VALUE),
            0x7F800000, // positive infinity
            0x7F800001, // positive signaling NaN, smallest payload
            0x7FC00000, // positive quiet NaN
            0x7FC00001, // positive quiet NaN with a payload
            0x7FFFFFFF); // positive NaN, largest payload

    private static final List<Long> DOUBLE_PATTERNS_IN_TOTAL_ORDER = List.of(
            0xFFFFFFFFFFFFFFFFL, // negative NaN, largest payload
            0xFFF8000000000001L, // negative quiet NaN with a payload
            0xFFF8000000000000L, // negative quiet NaN
            0xFFF0000000000001L, // negative signaling NaN, smallest payload
            0xFFF0000000000000L, // negative infinity
            Double.doubleToRawLongBits(-Double.MAX_VALUE),
            Double.doubleToRawLongBits(-1.0),
            Double.doubleToRawLongBits(-Double.MIN_VALUE),
            Double.doubleToRawLongBits(-0.0),
            Double.doubleToRawLongBits(0.0),
            Double.doubleToRawLongBits(Double.MIN_VALUE),
            Double.doubleToRawLongBits(1.0),
            Double.doubleToRawLongBits(Double.MAX_VALUE),
            0x7FF0000000000000L, // positive infinity
            0x7FF0000000000001L, // positive signaling NaN, smallest payload
            0x7FF8000000000000L, // positive quiet NaN
            0x7FF8000000000001L, // positive quiet NaN with a payload
            0x7FFFFFFFFFFFFFFFL); // positive NaN, largest payload

    @Test
    void floatKeysAscendInTotalOrder() {
        for (int i = 1; i < FLOAT_PATTERNS_IN_TOTAL_ORDER.size(); i++) {
            int lesser = FLOAT_PATTERNS_IN_TOTAL_ORDER.get(i - 1);
            int greater = FLOAT_PATTERNS_IN_TOTAL_ORDER.get(i);

            assertThat(TotalOrder.key(lesser))
                    .as("0x%08x before 0x%08x", lesser, greater)
                    .isLessThan(TotalOrder.key(greater));
        }
    }

    @Test
    void doubleKeysAscendInTotalOrder() {
        for (int i = 1; i < DOUBLE_PATTERNS_IN_TOTAL_ORDER.size(); i++) {
            long lesser = DOUBLE_PATTERNS_IN_TOTAL_ORDER.get(i - 1);
            long greater = DOUBLE_PATTERNS_IN_TOTAL_ORDER.get(i);

            assertThat(TotalOrder.key(lesser))
                    .as("0x%016x before 0x%016x", lesser, greater)
                    .isLessThan(TotalOrder.key(greater));
        }
    }

    @Test
    void aFloatKeyGivesItsBitPatternBack() {
        for (int pattern : FLOAT_PATTERNS_IN_TOTAL_ORDER) {
            assertThat(TotalOrder.bits(TotalOrder.key(pattern))).isEqualTo(pattern);
        }
    }

    @Test
    void aDoubleKeyGivesItsBitPatternBack() {
        for (long pattern : DOUBLE_PATTERNS_IN_TOTAL_ORDER) {
            assertThat(TotalOrder.bits(TotalOrder.key(pattern))).isEqualTo(pattern);
        }
    }
}
