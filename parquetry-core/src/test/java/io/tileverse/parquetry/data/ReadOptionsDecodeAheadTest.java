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
package io.tileverse.parquetry.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Boundary coverage for the cores/heap-adaptive decode-ahead default math. */
class ReadOptionsDecodeAheadTest {

    private static final long MIB = 1L << 20;

    static Stream<Arguments> cases() {
        return Stream.of(
                // cores, decodeBudgetCapacity, expected, why
                Arguments.of(1, 256 * MIB, 2, "low cores held to the floor of 2"),
                Arguments.of(2, 2048 * MIB, 2, "cores at the floor"),
                Arguments.of(4, 256 * MIB, 4, "4-core / 1g pod: core-capped under heapTerm 8"),
                Arguments.of(16, 256 * MIB, 8, "16-core / 1g pod: heap-capped at heapTerm 8"),
                Arguments.of(8, 256 * MIB, 8, "cores exactly at heapTerm"),
                Arguments.of(16, 2048 * MIB, 16, "16-core / 8g pod: core-capped under heapTerm 64"),
                Arguments.of(16, 1L, 2, "tiny budget: heapTerm held to the floor of 2"));
    }

    @ParameterizedTest(name = "[{index}] cores={0} budget={1}B -> {2} ({3})")
    @MethodSource("cases")
    @DisplayName("decode-ahead default clamps processors into [2, decodeBudget/32MiB]")
    void decodeAheadDefaultClampsCoresAgainstHeapTerm(int cores, long decodeBudgetCapacity, int expected, String why) {
        assertThat(ReadOptions.decodeAheadDefault(cores, decodeBudgetCapacity))
                .as(why)
                .isEqualTo(expected);
    }
}
