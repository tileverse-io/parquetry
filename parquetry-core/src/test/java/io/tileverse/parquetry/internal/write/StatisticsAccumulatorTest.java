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

        acc.updateInt(5);
        acc.updateNull();
        acc.updateInt(-3);
        acc.updateInt(42);
        acc.updateNull();
        acc.updateInt(7);

        Statistics stats = acc.finishChunk();
        assertThat(decodeInt32(stats.minValue())).as("min").isEqualTo(-3);
        assertThat(decodeInt32(stats.maxValue())).as("max").isEqualTo(42);
        assertThat(stats.nullCount()).hasValue(2L);
        assertThat(stats.isMinValueExact()).isTrue();
        assertThat(stats.isMaxValueExact()).isTrue();
    }

    @Test
    void legacyMinMaxMirrorTheModernValuesForCompatibility() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        acc.updateInt(5);
        acc.updateInt(-3);
        acc.updateInt(42);

        Statistics stats = acc.finishChunk();
        assertThat(decodeInt32(stats.min())).as("legacy min").isEqualTo(-3);
        assertThat(decodeInt32(stats.max())).as("legacy max").isEqualTo(42);
        assertThat(stats.min().toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                .as("legacy min mirrors min_value")
                .isEqualTo(stats.minValue().toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
        assertThat(stats.max().toArray(java.lang.foreign.ValueLayout.JAVA_BYTE))
                .as("legacy max mirrors max_value")
                .isEqualTo(stats.maxValue().toArray(java.lang.foreign.ValueLayout.JAVA_BYTE));
    }

    @Test
    void binaryOmitsLegacyMinMax() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        acc.updateBinary(asSegment(new byte[] {0x7E}));
        acc.updateBinary(asSegment(new byte[] {(byte) 0x80}));

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).as("modern minValue present").isNotEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("modern maxValue present").isNotEqualTo(MemorySegment.NULL);
        assertThat(stats.min()).as("legacy min omitted").isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).as("legacy max omitted").isEqualTo(MemorySegment.NULL);
    }

    @Test
    void fixedLenBinaryOmitsLegacyMinMax() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.FIXED_LEN_BYTE_ARRAY, null);
        acc.updateBinary(asSegment(new byte[] {0x7E}));
        acc.updateBinary(asSegment(new byte[] {(byte) 0x80}));

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).as("modern minValue present").isNotEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("modern maxValue present").isNotEqualTo(MemorySegment.NULL);
        assertThat(stats.min()).as("legacy min omitted").isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).as("legacy max omitted").isEqualTo(MemorySegment.NULL);
    }

    @Test
    void unsignedIntOmitsLegacyMinMax() {
        StatisticsAccumulator acc =
                StatisticsAccumulator.forKind(PrimitiveKind.INT32, new LogicalType.IntType((byte) 32, false));
        acc.updateInt(5);
        acc.updateInt(-3);
        acc.updateInt(42);

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).as("modern minValue present").isNotEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("modern maxValue present").isNotEqualTo(MemorySegment.NULL);
        assertThat(stats.min()).as("legacy min omitted").isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).as("legacy max omitted").isEqualTo(MemorySegment.NULL);
    }

    @Test
    void emptyChunkOmitsLegacyMinMax() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);

        Statistics stats = acc.finishChunk();
        assertThat(stats.min()).as("legacy min absent").isEqualTo(MemorySegment.NULL);
        assertThat(stats.max()).as("legacy max absent").isEqualTo(MemorySegment.NULL);
    }

    @Test
    void int64MinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT64, null);

        long[] inputs = {1_000_000L, -50L, 0L, Long.MAX_VALUE, Long.MIN_VALUE, 17L};
        for (long v : inputs) {
            acc.updateLong(v);
        }

        Statistics stats = acc.finishChunk();
        assertThat(decodeInt64(stats.minValue())).isEqualTo(Long.MIN_VALUE);
        assertThat(decodeInt64(stats.maxValue())).isEqualTo(Long.MAX_VALUE);
        assertThat(stats.nullCount()).hasValue(0L);
    }

    @Test
    void floatMinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.FLOAT, null);

        acc.updateFloat(1.5f);
        acc.updateFloat(-2.25f);
        acc.updateNull();
        acc.updateFloat(3.0f);

        Statistics stats = acc.finishChunk();
        assertThat(decodeFloat(stats.minValue())).isEqualTo(-2.25f);
        assertThat(decodeFloat(stats.maxValue())).isEqualTo(3.0f);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void floatNaNIsExcludedFromMinMaxButCountedAsNonNull() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.FLOAT, null);

        acc.updateFloat(Float.NaN);
        acc.updateFloat(Float.NaN);

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
            acc.updateDouble(v);
        }
        acc.updateNull();

        Statistics stats = acc.finishChunk();
        assertThat(decodeDouble(stats.minValue())).isEqualTo(-1.5);
        assertThat(decodeDouble(stats.maxValue())).isEqualTo(100.5);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void booleanMinMaxAndNullCountAccumulate() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BOOLEAN, null);

        acc.updateBoolean(true);
        acc.updateBoolean(true);
        acc.updateBoolean(false);
        acc.updateNull();

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue().toArray(JAVA_BYTE)).containsExactly(0);
        assertThat(stats.maxValue().toArray(JAVA_BYTE)).containsExactly(1);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void binaryMinMaxIsUnsignedLexicographic() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);

        acc.updateBinary(asSegment("banana".getBytes(StandardCharsets.UTF_8)));
        acc.updateBinary(asSegment("apple".getBytes(StandardCharsets.UTF_8)));
        acc.updateBinary(asSegment("cherry".getBytes(StandardCharsets.UTF_8)));

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

        acc.updateBinary(asSegment(mutable));
        mutable[0] = 99;

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue().toArray(JAVA_BYTE)).containsExactly(1, 2, 3);
        assertThat(stats.maxValue().toArray(JAVA_BYTE)).containsExactly(1, 2, 3);
    }

    @Test
    void updateBinaryRecordsNonNullObservationAndMinMax() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        acc.updateBinary(segmentOf("banana"));
        acc.updateBinary(segmentOf("apple"));
        acc.updateBinary(segmentOf("cherry"));
        Statistics stats = acc.finishChunk();
        assertThat(stats.nullCount()).hasValue(0L);
        assertThat(utf8(stats.minValue())).isEqualTo("apple");
        assertThat(utf8(stats.maxValue())).isEqualTo("cherry");
    }

    @Test
    void updateBinaryOrdersUnsignedAndShorterPrefixFirst() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        // 0x80 must order above 0x7f (unsigned), and "ab" above "a" (prefix tiebreak).
        acc.updateBinary(MemorySegment.ofArray(new byte[] {(byte) 0x80}));
        acc.updateBinary(MemorySegment.ofArray(new byte[] {(byte) 0x7f}));
        acc.updateBinary(segmentOf("ab"));
        acc.updateBinary(segmentOf("a"));
        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue().toArray(JAVA_BYTE)).containsExactly(0x61); // "a"
        assertThat(stats.maxValue().toArray(JAVA_BYTE)).containsExactly(0x80);
    }

    @Test
    void updateBinaryCopiesOnImprovementNotAliasingCallerMemory() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        byte[] mutable = "mmm".getBytes(StandardCharsets.UTF_8);
        acc.updateBinary(MemorySegment.ofArray(mutable));
        mutable[0] = 'z';
        Statistics stats = acc.finishChunk();
        assertThat(utf8(stats.minValue())).isEqualTo("mmm");
        assertThat(utf8(stats.maxValue())).isEqualTo("mmm");
    }

    @Test
    void typedUpdateRejectsWrongKind() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        assertThatThrownBy(() -> acc.updateLong(1L)).isInstanceOf(ParquetWriteException.class);
    }

    @Test
    void updateNonNullRejectsMinMaxTrackingKinds() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        assertThatThrownBy(acc::updateNonNull).isInstanceOf(ParquetWriteException.class);
    }

    @Test
    void mergePicksBinaryBoundsAcrossAccumulators() {
        StatisticsAccumulator left = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        StatisticsAccumulator right = StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, null);
        left.updateBinary(segmentOf("m"));
        right.updateBinary(segmentOf("a"));
        right.updateBinary(segmentOf("z"));
        left.merge(right);
        Statistics stats = left.finishChunk();
        assertThat(utf8(stats.minValue())).isEqualTo("a");
        assertThat(utf8(stats.maxValue())).isEqualTo("z");
    }

    @Test
    void geometryColumnTracksNullCountOnly() {
        StatisticsAccumulator acc =
                StatisticsAccumulator.forKind(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geometry(Optional.empty()));

        acc.updateBinary(asSegment(new byte[] {0x01, 0x02, 0x03}));
        acc.updateNull();
        acc.updateBinary(asSegment(new byte[] {0x04}));

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).as("minValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).as("maxValue").isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void int96ProducesNoMinMax() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT96, null);

        acc.updateNonNull();
        acc.updateNull();

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(1L);
    }

    @Test
    void mergeProducesSameSnapshotAsDirectAccumulation() {
        StatisticsAccumulator combined = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        combined.updateInt(10);
        combined.updateInt(2);
        combined.updateNull();
        combined.updateInt(57);

        StatisticsAccumulator left = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        left.updateInt(10);
        left.updateInt(2);
        StatisticsAccumulator right = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        right.updateNull();
        right.updateInt(57);

        left.merge(right);

        assertSameStats(left.finishChunk(), combined.finishChunk());
    }

    @Test
    void mergeAbsorbsEmptyAccumulator() {
        StatisticsAccumulator left = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        left.updateInt(1);
        left.updateInt(2);
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
        right.updateInt(7);
        right.updateInt(-1);

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
        acc.updateInt(42);
        acc.updateNull();

        acc.reset();

        Statistics stats = acc.finishChunk();
        assertThat(stats.minValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.maxValue()).isEqualTo(MemorySegment.NULL);
        assertThat(stats.nullCount()).hasValue(0L);
    }

    @Test
    void finishPageDoesNotResetAccumulationSoFinishChunkCarriesFullWindow() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        acc.updateInt(5);
        acc.updateInt(10);

        PageStatistics page1 = acc.finishPage();
        assertThat(decodeInt32(page1.min())).isEqualTo(5);
        assertThat(decodeInt32(page1.max())).isEqualTo(10);
        assertThat(page1.isNullPage()).isFalse();

        acc.updateInt(-3);
        acc.updateInt(20);

        Statistics chunk = acc.finishChunk();
        assertThat(decodeInt32(chunk.minValue())).isEqualTo(-3);
        assertThat(decodeInt32(chunk.maxValue())).isEqualTo(20);
        assertThat(chunk.nullCount()).hasValue(0L);
    }

    @Test
    void allNullPageIsFlaggedAsNullPage() {
        StatisticsAccumulator acc = StatisticsAccumulator.forKind(PrimitiveKind.INT32, null);
        acc.updateNull();
        acc.updateNull();
        acc.updateNull();

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

    private static MemorySegment segmentOf(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8));
    }

    private static String utf8(MemorySegment segment) {
        return new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8);
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
