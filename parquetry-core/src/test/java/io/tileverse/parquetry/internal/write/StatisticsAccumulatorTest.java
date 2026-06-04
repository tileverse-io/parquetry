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
package io.tileverse.parquetry.internal.write;

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

class StatisticsAccumulatorTest {

    @Test
    void emptyChunkSnapshotHasNoMinMaxAndZeroNullCount() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);

        Statistics stats = acc.finishChunk();

        assertThat(stats.minValue()).as("minValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("maxValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).as("nullCount").hasValue(0L);
        assertThat(stats.distinctCount()).as("distinctCount").isEmpty();
    }

    @Test
    void emptyPageSnapshotIsNotNullPage() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);

        PageStatistics page = acc.finishPage();

        assertThat(page.min()).as("min").isEqualTo(MemorySegment.NULL);
        assertThat(page.max()).as("max").isEqualTo(MemorySegment.NULL);
        assertThat(page.nullCount()).as("nullCount").isZero();
        assertThat(page.isNullPage()).as("isNullPage").isFalse();
    }

    @Test
    void int32MinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);

        acc.update(5, false);
        acc.update(null, true);
        acc.update(-3, false);
        acc.update(42, false);
        acc.update(null, true);
        acc.update(7, false);

        Statistics stats = acc.finishChunk();
        assertThat(decodeInt32(stats.minValue())).as("min").isEqualTo(-3);
        assertThat(decodeInt32(stats.maxValue())).as("max").isEqualTo(42);
        assertThat(stats.nullCount()).hasValue(2L);
        assertThat(stats.isMinValueExact()).isTrue();
        assertThat(stats.isMaxValueExact()).isTrue();
    }

    @Test
    void int64MinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT64, null);

        long[] inputs = {1_000_000L, -50L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, 17L};
        for (long v : inputs) {
            acc.update(v, false);
        }

        Statistics stats = acc.finishChunk();
        assertThat(decodeInt64(stats.minValue())).isEqualTo(Long.MIN_VALUE);
        assertThat(decodeInt64(stats.maxValue())).isEqualTo(Long.MAX_VALUE);
        assertThat(stats.nullCount()).hasValue(0L);
    }

    @Test
    void floatMinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.FLOAT, null);

        acc.update(1.5f, false);
        acc.update(-2.25f, false);
        acc.update(null, true);
        acc.update(3.0f, false);

        Statistics stats = acc.finishChunk();
        assertThat(decodeFloat(stats.minValue())).isEqualTo(-2.25f);
        assertThat(decodeFloat(stats.maxValue())).isEqualTo(3.0f);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void floatNaNIsExcludedFromMinMaxButCountedAsNonNull() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.FLOAT, null);

        acc.update(Float.NaN, false);
        acc.update(Float.NaN, false);

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(0L);
    }

    @Test
    void doubleMinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.DOUBLE, null);

        double[] inputs = {3.14, -1.5, 2.718, 0.0, 100.5};
        for (double v : inputs) {
            acc.update(v, false);
        }
        acc.update(null, true);

        Statistics stats = acc.finishChunk();
        assertThat(decodeDouble(stats.minValue())).isEqualTo(-1.5);
        assertThat(decodeDouble(stats.maxValue())).isEqualTo(100.5);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void booleanMinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BOOLEAN, null);

        acc.update(true, false);
        acc.update(true, false);
        acc.update(false, false);
        acc.update(null, true);

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue().toArray(JAVA_BYTE)).containsExactly(0);
        assertThat(stats.maxValue().toArray(JAVA_BYTE)).containsExactly(1);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void binaryMinMaxIsUnsignedLexicographic() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);

        acc.update(asSegment("banana".getBytes(StandardCharsets.UTF_8)), false);
        acc.update(asSegment("apple".getBytes(StandardCharsets.UTF_8)), false);
        acc.update(asSegment("cherry".getBytes(StandardCharsets.UTF_8)), false);

        Statistics stats = acc.finishChunk();
        assertThat(new String(stats.minValue().toArray(JAVA_BYTE), StandardCharsets.UTF_8))
                .isEqualTo("apple");
        assertThat(new String(stats.maxValue().toArray(JAVA_BYTE), StandardCharsets.UTF_8))
                .isEqualTo("cherry");
    }

    @Test
    void binaryUpdateCopiesCallerBytesDefensively() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        byte[] mutable = {1, 2, 3};

        acc.update(asSegment(mutable), false);
        mutable[0] = 99;

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue().toArray(JAVA_BYTE)).containsExactly(1, 2, 3);
        assertThat(stats.maxValue().toArray(JAVA_BYTE)).containsExactly(1, 2, 3);
    }

    @Test
    void geometryColumnTracksNullCountOnly() {
        StatisticsAccumulator acc =
                StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geometry(Optional.empty()));

        acc.update(asSegment(new byte[] {0x01, 0x02, 0x03}), false);
        acc.update(null, true);
        acc.update(asSegment(new byte[] {0x04}), false);

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).as("minValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("maxValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void int96ProducesNoMinMax() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT96, null);

        acc.update(asSegment(new byte[12]), false);
        acc.update(null, true);

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void mergeProducesSameSnapshotAsDirectAccumulation() {
        StatisticsAccumulator combined = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        combined.update(10, false);
        combined.update(2, false);
        combined.update(null, true);
        combined.update(57, false);

        StatisticsAccumulator left = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        left.update(10, false);
        left.update(2, false);
        StatisticsAccumulator right = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        right.update(null, true);
        right.update(57, false);

        left.merge(right);

        assertSameStats(left.finishChunk(), combined.finishChunk());
    }

    @Test
    void mergeAbsorbsEmptyAccumulator() {
        StatisticsAccumulator left = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        left.update(1, false);
        left.update(2, false);
        StatisticsAccumulator right = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);

        left.merge(right);

        Statistics stats = left.finishChunk();
        assertThat(decodeInt32(stats.minValue())).isEqualTo(1);
        assertThat(decodeInt32(stats.maxValue())).isEqualTo(2);
        assertThat(stats.nullCount()).hasValue(0L);
    }

    @Test
    void mergeIntoEmptyAccumulatorCopiesSource() {
        StatisticsAccumulator left = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        StatisticsAccumulator right = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        right.update(7, false);
        right.update(-1, false);

        left.merge(right);

        Statistics stats = left.finishChunk();
        assertThat(decodeInt32(stats.minValue())).isEqualTo(-1);
        assertThat(decodeInt32(stats.maxValue())).isEqualTo(7);
    }

    @Test
    void mergeRejectsMismatchedKinds() {
        StatisticsAccumulator intAcc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        StatisticsAccumulator longAcc = StatisticsAccumulator.forKind(PrimitiveKind.INT64, null);

        assertThatThrownBy(() -> intAcc.merge(longAcc))
                .isInstanceOf(ParquetWriteException.class)
                .hasMessageContaining("different kinds");
    }

    @Test
    void resetReturnsToInitialState() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        acc.update(42, false);
        acc.update(null, true);

        acc.reset();

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(0L);
    }

    @Test
    void finishPageDoesNotResetAccumulationSoFinishChunkCarriesFullWindow() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        acc.update(5, false);
        acc.update(10, false);

        PageStatistics page1 = acc.finishPage();
        assertThat(decodeInt32(page1.min())).isEqualTo(5);
        assertThat(decodeInt32(page1.max())).isEqualTo(10);
        assertThat(page1.isNullPage()).isFalse();

        acc.update(-3, false);
        acc.update(20, false);

        Statistics chunk = acc.finishChunk();
        assertThat(decodeInt32(chunk.minValue())).isEqualTo(-3);
        assertThat(decodeInt32(chunk.maxValue())).isEqualTo(20);
        assertThat(chunk.nullCount()).hasValue(0L);
    }

    @Test
    void allNullPageIsFlaggedAsNullPage() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        acc.update(null, true);
        acc.update(null, true);
        acc.update(null, true);

        PageStatistics page = acc.finishPage();

        assertThat(page.isNullPage()).isTrue();
        assertThat(page.nullCount()).isEqualTo(3L);
        assertThat(page.min()).isEqualTo(MemorySegment.NULL);
        assertThat(page.max()).isEqualTo(MemorySegment.NULL);
    }

    private static void assertSameStats(Statistics a, Statistics b) {
        assertThat(a.nullCount()).isEqualTo(b.nullCount());
        assertThat(a.distinctCount()).isEqualTo(b.distinctCount());
        assertThat(a.minValue().toArray(JAVA_BYTE)).isEqualTo(b.minValue().toArray(JAVA_BYTE));
        assertThat(a.maxValue().toArray(JAVA_BYTE)).isEqualTo(b.maxValue().toArray(JAVA_BYTE));
        assertThat(a.isMinValueExact()).isEqualTo(b.isMinValueExact());
        assertThat(a.isMaxValueExact()).isEqualTo(b.isMaxValueExact());
    }

    private static MemorySegment asSegment(byte[] bytes) {
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static int decodeInt32(MemorySegment seg) {
        return seg.get(INT32, 0L);
    }

    private static long decodeInt64(MemorySegment seg) {
        return seg.get(INT64, 0L);
    }

    private static float decodeFloat(MemorySegment seg) {
        return seg.get(FLOAT, 0L);
    }

    private static double decodeDouble(MemorySegment seg) {
        return seg.get(DOUBLE, 0L);
    }
}
