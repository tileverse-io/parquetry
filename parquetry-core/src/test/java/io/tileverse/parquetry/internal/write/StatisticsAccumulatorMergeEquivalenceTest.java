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

import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * Proves the merge-at-page-boundary design exact: a chunk accumulator fed only by merge(pageAccumulator) at randomized
 * page boundaries produces byte-identical statistics to a reference accumulator fed every cell directly. Covers NaN
 * mixes, null runs, only-null pages, an empty tail window, and the orders given to a column by its logical type.
 */
class StatisticsAccumulatorMergeEquivalenceTest {

    private static final int CELLS = 10_000;
    private static final int FIXED_CELL_BYTES = 8;
    private static final int MAX_VARIABLE_CELL_BYTES = 24;

    private static final long[] SEEDS = {1L, 42L, 20260704L};

    static Stream<Arguments> scenarios() {
        Stream.Builder<Arguments> out = Stream.builder();
        for (PrimitiveKind kind : PrimitiveKind.values()) {
            addScenarios(out, kind, null);
        }
        addScenarios(out, PrimitiveKind.INT32, new LogicalType.IntType((byte) 32, false));
        addScenarios(out, PrimitiveKind.INT64, new LogicalType.IntType((byte) 64, false));
        addScenarios(out, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Decimal(2, 18));
        addScenarios(out, PrimitiveKind.BYTE_ARRAY, new LogicalType.Decimal(2, 18));
        addScenarios(out, PrimitiveKind.FIXED_LEN_BYTE_ARRAY, new LogicalType.Float16Type());
        return out.build();
    }

    private static void addScenarios(Stream.Builder<Arguments> out, PrimitiveKind kind, LogicalType logicalType) {
        for (long seed : SEEDS) {
            out.add(Arguments.of(kind, logicalType, seed));
        }
    }

    @ParameterizedTest(name = "{0} annotated {1} seed {2}")
    @MethodSource("scenarios")
    void mergedPageWindowsMatchDirectAccumulation(PrimitiveKind kind, LogicalType logicalType, long seed) {
        StatisticsAccumulator direct = WriteFixtures.accumulator(kind, logicalType);
        StatisticsAccumulator chunk = WriteFixtures.accumulator(kind, logicalType);
        StatisticsAccumulator page = WriteFixtures.accumulator(kind, logicalType);

        Random random = new Random(seed);
        int cellsUntilFlush = 1 + random.nextInt(500);
        for (int i = 0; i < CELLS; i++) {
            feedOneCell(kind, logicalType, random, direct, page);
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
            PrimitiveKind kind,
            LogicalType logicalType,
            Random random,
            StatisticsAccumulator direct,
            StatisticsAccumulator page) {
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
                // 5% NaN of either sign and any payload: counted, and left out of the bounds.
                float v = random.nextInt(100) < 5
                        ? WriteFixtures.randomFloatNaN(random)
                        : Float.intBitsToFloat(random.nextInt());
                direct.updateFloat(v);
                page.updateFloat(v);
            }
            case DOUBLE -> {
                double v = random.nextInt(100) < 5
                        ? WriteFixtures.randomDoubleNaN(random)
                        : Double.longBitsToDouble(random.nextLong());
                direct.updateDouble(v);
                page.updateDouble(v);
            }
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY -> {
                byte[] v = new byte[binaryCellBytes(kind, logicalType, random)];
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

    /** The byte length of the next binary cell: two for a half float, eight for other fixed cells, else random. */
    private static int binaryCellBytes(PrimitiveKind kind, LogicalType logicalType, Random random) {
        if (logicalType instanceof LogicalType.Float16Type) {
            return HalfFloats.BYTES;
        }
        return kind == PrimitiveKind.FIXED_LEN_BYTE_ARRAY ? FIXED_CELL_BYTES : random.nextInt(MAX_VARIABLE_CELL_BYTES);
    }

    private static void assertStatisticsEqual(Statistics expected, Statistics actual) {
        assertThat(actual.nullCount()).isEqualTo(expected.nullCount());
        assertThat(actual.nanCount()).isEqualTo(expected.nanCount());
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
