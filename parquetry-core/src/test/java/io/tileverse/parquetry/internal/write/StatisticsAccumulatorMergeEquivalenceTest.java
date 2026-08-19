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
import java.lang.foreign.ValueLayout;
import java.util.Random;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Proves the merge-at-page-boundary design exact: a chunk accumulator fed only by merge(pageAccumulator) at randomized
 * page boundaries produces byte-identical statistics to a reference accumulator fed every cell directly. Covers NaN
 * mixes, null runs, only-null pages, and an empty tail window.
 */
class StatisticsAccumulatorMergeEquivalenceTest {

    private static final int CELLS = 10_000;

    static Stream<Arguments> scenarios() {
        Stream.Builder<Arguments> out = Stream.builder();
        for (PrimitiveKind kind : PrimitiveKind.values()) {
            for (long seed : new long[] {1L, 42L, 20260704L}) {
                out.add(Arguments.of(kind, seed));
            }
        }
        return out.build();
    }

    @ParameterizedTest(name = "{0} seed {1}")
    @MethodSource("scenarios")
    void mergedPageWindowsMatchDirectAccumulation(PrimitiveKind kind, long seed) {
        StatisticsAccumulator direct = StatisticsAccumulator.forKind(kind, null);
        StatisticsAccumulator chunk = StatisticsAccumulator.forKind(kind, null);
        StatisticsAccumulator page = StatisticsAccumulator.forKind(kind, null);

        Random random = new Random(seed);
        int cellsUntilFlush = 1 + random.nextInt(500);
        for (int i = 0; i < CELLS; i++) {
            feedOneCell(kind, random, direct, page);
            if (--cellsUntilFlush == 0) {
                chunk.merge(page);
                page.reset();
                cellsUntilFlush = 1 + random.nextInt(500);
            }
        }
        // Tail window, possibly empty - finishChunk() in the writer flushes it the same way.
        chunk.merge(page);
        page.reset();

        assertStatisticsEqual(direct.finishChunk(), chunk.finishChunk());
    }

    private static void feedOneCell(
            PrimitiveKind kind, Random random, StatisticsAccumulator direct, StatisticsAccumulator page) {
        // 15% nulls; runs of nulls emerge naturally from consecutive draws.
        if (random.nextInt(100) < 15) {
            direct.updateNull();
            page.updateNull();
            return;
        }
        switch (kind) {
            case BOOLEAN -> {
                boolean v = random.nextBoolean();
                direct.updateBoolean(v);
                page.updateBoolean(v);
            }
            case INT32 -> {
                int v = random.nextInt();
                direct.updateInt(v);
                page.updateInt(v);
            }
            case INT64 -> {
                long v = random.nextLong();
                direct.updateLong(v);
                page.updateLong(v);
            }
            case FLOAT -> {
                // 5% NaN: excluded from min/max, still counted non-null.
                float v = random.nextInt(100) < 5 ? Float.NaN : Float.intBitsToFloat(random.nextInt());
                direct.updateFloat(v);
                page.updateFloat(v);
            }
            case DOUBLE -> {
                double v = random.nextInt(100) < 5 ? Double.NaN : Double.longBitsToDouble(random.nextLong());
                direct.updateDouble(v);
                page.updateDouble(v);
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                byte[] v = new byte[kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY ? 8 : random.nextInt(24)];
                random.nextBytes(v);
                MemorySegment segment = MemorySegment.ofArray(v);
                direct.updateBinary(segment);
                page.updateBinary(segment);
            }
            case INT96 -> {
                direct.updateNonNull();
                page.updateNonNull();
            }
        }
    }

    private static void assertStatisticsEqual(Statistics expected, Statistics actual) {
        assertThat(actual.nullCount()).isEqualTo(expected.nullCount());
        assertThat(actual.isMinValueExact()).isEqualTo(expected.isMinValueExact());
        assertThat(actual.isMaxValueExact()).isEqualTo(expected.isMaxValueExact());
        assertSegmentEqual(expected.minValue(), actual.minValue());
        assertSegmentEqual(expected.maxValue(), actual.maxValue());
        assertSegmentEqual(expected.min(), actual.min());
        assertSegmentEqual(expected.max(), actual.max());
    }

    private static void assertSegmentEqual(MemorySegment expected, MemorySegment actual) {
        if (expected == MemorySegment.NULL || actual == MemorySegment.NULL) {
            assertThat(actual).isSameAs(expected);
            return;
        }
        assertThat(actual.toArray(ValueLayout.JAVA_BYTE)).isEqualTo(expected.toArray(ValueLayout.JAVA_BYTE));
    }
}
