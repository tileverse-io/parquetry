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

import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.data.WriteOptions.FloatColumnOrder;
import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

class ColumnIndexBuilderTest {

    private static RecursiveComparisonConfiguration memorySegmentByContent() {
        RecursiveComparisonConfiguration cfg = new RecursiveComparisonConfiguration();
        cfg.registerEqualsForType(
                (MemorySegment a, MemorySegment b) -> {
                    if (a == b) {
                        return true;
                    }
                    if (a == null || b == null) {
                        return false;
                    }
                    if (a.byteSize() != b.byteSize()) {
                        return false;
                    }
                    byte[] ab = a.toArray(JAVA_BYTE);
                    byte[] bb = b.toArray(JAVA_BYTE);
                    for (int i = 0; i < ab.length; i++) {
                        if (ab[i] != bb[i]) {
                            return false;
                        }
                    }
                    return true;
                },
                MemorySegment.class);
        return cfg;
    }

    @Test
    void int32RoundTripPreservesNullPagesAndBoundsAndNullCounts() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(10), 0L, false));
        builder.appendPage(page(int32(11), int32(20), 1L, false));
        builder.appendPage(page(int32(21), int32(30), 0L, false));

        ColumnIndex original = builder.finishChunk().orElseThrow();
        ColumnIndex parsed = roundTrip(original);

        assertThat(parsed).usingRecursiveComparison(memorySegmentByContent()).isEqualTo(original);
        assertThat(parsed.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
        assertThat(parsed.nullPages()).containsExactly(false, false, false);
        assertThat(parsed.nullCounts()).contains(List.of(0L, 1L, 0L));
    }

    @Test
    void allNullPagesEmitMemorySegmentNullForMinMax() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT64);
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 5L, true));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 7L, true));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 3L, true));

        ColumnIndex original = builder.finishChunk().orElseThrow();

        assertThat(original.nullPages()).containsExactly(true, true, true);
        assertThat(original.minValues())
                .allSatisfy(seg -> assertThat(seg.byteSize()).isZero());
        assertThat(original.maxValues())
                .allSatisfy(seg -> assertThat(seg.byteSize()).isZero());
        assertThat(original.boundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
        assertThat(original.nullCounts()).contains(List.of(5L, 7L, 3L));
    }

    @Test
    void mixedNullAndValuedPagesRoundTripCorrectly() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(0), int32(5), 1L, false));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 4L, true));
        builder.appendPage(page(int32(6), int32(9), 0L, false));

        ColumnIndex parsed = roundTrip(builder.finishChunk().orElseThrow());

        assertThat(parsed.nullPages()).containsExactly(false, true, false);
        assertThat(parsed.boundaryOrder())
                .as("an all-null page between two ordered non-null pages still leaves the non-null sequence ascending")
                .isEqualTo(BoundaryOrder.ASCENDING);
        assertThat(parsed.nullCounts()).contains(List.of(1L, 4L, 0L));
    }

    @Test
    void ascendingInt32PagesYieldAscendingBoundaryOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(2), 0L, false));
        builder.appendPage(page(int32(3), int32(4), 0L, false));
        builder.appendPage(page(int32(5), int32(6), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void descendingInt64PagesYieldDescendingBoundaryOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT64);
        builder.appendPage(page(int64(100L), int64(200L), 0L, false));
        builder.appendPage(page(int64(50L), int64(99L), 0L, false));
        builder.appendPage(page(int64(0L), int64(49L), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
    }

    @Test
    void crossingMinMaxYieldsUnorderedBoundaryOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(10), 0L, false));
        builder.appendPage(page(int32(5), int32(7), 0L, false));
        builder.appendPage(page(int32(20), int32(30), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
    }

    @Test
    void singleNonNullPageDefaultsToUnordered() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(42), int32(42), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder())
                .as("a single page can't establish an order; UNORDERED is the safe value")
                .isEqualTo(BoundaryOrder.UNORDERED);
    }

    @Test
    void byteArrayPagesUseUnsignedLexOrdering() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.UNSIGNED_BYTES);
        builder.appendPage(page(bytes("apple"), bytes("banana"), 0L, false));
        builder.appendPage(page(bytes("cherry"), bytes("date"), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void chunkOfAColumnWithoutADefinedOrderHasNoColumnIndex() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.UNDEFINED);
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 0L, false));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 2L, false));

        assertThat(builder.finishChunk()).isEmpty();
    }

    @Test
    void chunkOfOnlyNullPagesHasNoColumnIndexWithoutADefinedOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.UNDEFINED);
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 4L, true));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 6L, true));

        assertThat(builder.finishChunk()).isEmpty();
    }

    @Test
    void chunkOfOnlyNullPagesKeepsItsColumnIndexInADefinedOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 4L, true));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 6L, true));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.nullPages()).containsExactly(true, true);
        assertThat(index.nullCounts()).contains(List.of(4L, 6L));
        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
    }

    @Test
    void finishChunkClearsBuilderForReuse() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(2), 0L, false));
        builder.appendPage(page(int32(3), int32(4), 0L, false));
        ColumnIndex first = builder.finishChunk().orElseThrow();

        builder.appendPage(page(int32(100), int32(200), 5L, false));
        ColumnIndex second = builder.finishChunk().orElseThrow();

        assertThat(first.nullPages()).hasSize(2);
        assertThat(second.nullPages()).hasSize(1);
        assertThat(second.nullCounts()).contains(List.of(5L));
    }

    @Test
    void resetClearsAccumulatedPages() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(2), 0L, false));
        builder.reset();
        builder.appendPage(page(int32(10), int32(20), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.nullPages()).hasSize(1);
        assertThat(index.nullCounts()).contains(List.of(0L));
    }

    @Test
    void chunkWithAPageOfOnlyNaNHasNoColumnIndex() {
        StatisticsAccumulator nanPage = WriteFixtures.accumulator(PrimitiveKind.FLOAT, null);
        nanPage.updateFloat(Float.NaN);
        nanPage.updateFloat(Float.NaN);
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.FLOAT);
        builder.appendPage(floatPage(1.0f, 2.0f));
        builder.appendPage(nanPage.finishPage());

        assertThat(builder.finishChunk()).isEmpty();
    }

    @Test
    void chunkWithAPageOfOnlyNaNKeepsItsColumnIndexInTotalOrder() {
        SchemaNode.Primitive floats = WriteFixtures.leaf(PrimitiveKind.FLOAT, null);
        StatisticsAccumulator nanPage = WriteFixtures.accumulator(floats, FloatColumnOrder.IEEE_754_TOTAL_ORDER);
        nanPage.updateFloat(Float.NaN);
        nanPage.updateFloat(Float.NaN);
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.FLOAT_TOTAL_ORDER);
        builder.appendPage(floatPage(1.0f, 2.0f));
        builder.appendPage(nanPage.finishPage());

        ColumnIndex index = builder.finishChunk().orElseThrow();
        MemorySegment nanPageMax = index.maxValues().get(1);

        assertThat(index.nullPages()).containsExactly(false, false);
        assertThat(index.nanCounts()).contains(List.of(0L, 2L));
        assertThat(WriteFixtures.floatBits(nanPageMax)).isEqualTo(Float.floatToRawIntBits(Float.NaN));
        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void floatColumnIndexRecordsTheNaNCountOfEachPage() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.FLOAT);
        builder.appendPage(floatPage(1.0f, 2.0f));
        builder.appendPage(floatPageWithNaNs(3.0f, 4.0f, 5L));

        ColumnIndex parsed = roundTrip(builder.finishChunk().orElseThrow());

        assertThat(parsed.nanCounts()).contains(List.of(0L, 5L));
    }

    @Test
    void integerColumnIndexRecordsNoNaNCounts() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(2), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.nanCounts()).isEmpty();
    }

    @Test
    void floatPagesMeetingAtZeroAscend() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.FLOAT);
        builder.appendPage(floatPage(-1.0f, 0.0f));
        builder.appendPage(floatPage(-0.0f, 1.0f));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void chunkWithMixedNaNPagesKeepsItsColumnIndex() {
        StatisticsAccumulator mixedPage = WriteFixtures.accumulator(PrimitiveKind.FLOAT, null);
        mixedPage.updateFloat(Float.NaN);
        mixedPage.updateFloat(3.0f);
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.FLOAT);
        builder.appendPage(mixedPage.finishPage());

        assertThat(builder.finishChunk()).isPresent();
    }

    @Test
    void chunkWithANonNullPageWithoutBoundsHasNoColumnIndex() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(int32(1), int32(2), 0L, false));
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 0L, false));

        assertThat(builder.finishChunk()).isEmpty();
    }

    @Test
    void builderServesTheNextChunkAfterOneWithoutColumnIndex() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_INT32);
        builder.appendPage(page(MemorySegment.NULL, MemorySegment.NULL, 0L, false));
        builder.finishChunk();
        builder.appendPage(page(int32(1), int32(2), 0L, false));

        assertThat(builder.finishChunk()).isPresent();
    }

    @Test
    void floatPageWithoutANaNCountIsRejected() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.FLOAT);
        PageStatistics uncounted = page(float32(1.0f), float32(2.0f), 0L, false);

        assertThatThrownBy(() -> builder.appendPage(uncounted)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void unsignedPageBoundsAscendAcrossTheSignedRange() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.UNSIGNED_INT32);
        builder.appendPage(page(int32(1), int32(100), 0L, false));
        builder.appendPage(page(uint32(2_200_000_000L), uint32(3_000_000_000L), 0L, false));
        builder.appendPage(page(uint32(3_500_000_000L), uint32(4_000_000_000L), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void decimalPageBoundsAscendFromNegativeToPositive() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.SIGNED_BYTES);
        builder.appendPage(page(decimal(-9), decimal(-5), 0L, false));
        builder.appendPage(page(decimal(-1), decimal(3), 0L, false));
        builder.appendPage(page(decimal(4), decimal(8), 0L, false));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void halfFloatPageBoundsAscendByValue() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(BoundsOrder.HALF_FLOAT);
        builder.appendPage(halfFloatPage(-2.0f, -1.0f));
        builder.appendPage(halfFloatPage(-0.5f, 0.5f));
        builder.appendPage(halfFloatPage(1.0f, 3.0f));

        ColumnIndex index = builder.finishChunk().orElseThrow();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    /** The statistics of a page of a FLOAT column holding no NaN cell and no null. */
    private static PageStatistics floatPage(float min, float max) {
        return floatPageWithNaNs(min, max, 0L);
    }

    /** The statistics of a page of a FLOAT column holding {@code nanCount} NaN cells and no null. */
    private static PageStatistics floatPageWithNaNs(float min, float max, long nanCount) {
        return new PageStatistics(float32(min), float32(max), 0L, false, OptionalLong.of(nanCount), true, true);
    }

    /** The statistics of a page of a FLOAT16 column holding no NaN cell and no null. */
    private static PageStatistics halfFloatPage(float min, float max) {
        MemorySegment minCell = WriteFixtures.halfFloat(min);
        MemorySegment maxCell = WriteFixtures.halfFloat(max);
        return new PageStatistics(minCell, maxCell, 0L, false, OptionalLong.of(0L), true, true);
    }

    /** The statistics of a page of a column without NaN counts. */
    private static PageStatistics page(MemorySegment min, MemorySegment max, long nullCount, boolean isNullPage) {
        boolean bounded = min != MemorySegment.NULL && max != MemorySegment.NULL;
        return new PageStatistics(min, max, nullCount, isNullPage, OptionalLong.empty(), bounded, bounded);
    }

    private static ColumnIndex roundTrip(ColumnIndex original) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeColumnIndex(out, original);
        return ParquetFormatDeserializer.readColumnIndex(new ByteArrayInputStream(out.toByteArray()));
    }

    private static MemorySegment float32(float value) {
        byte[] buf = new byte[4];
        MemorySegment.ofArray(buf).set(FLOAT, 0, value);
        return MemorySegment.ofArray(buf).asReadOnly();
    }

    private static MemorySegment int32(int value) {
        byte[] buf = new byte[4];
        MemorySegment.ofArray(buf).set(INT32, 0, value);
        return MemorySegment.ofArray(buf).asReadOnly();
    }

    private static MemorySegment uint32(long value) {
        return int32((int) value);
    }

    /** A decimal's unscaled value as four big-endian two's complement bytes. */
    private static MemorySegment decimal(int unscaled) {
        byte[] bytes = WriteFixtures.signedBytes(BigInteger.valueOf(unscaled), Integer.BYTES);
        return MemorySegment.ofArray(bytes).asReadOnly();
    }

    private static MemorySegment int64(long value) {
        byte[] buf = new byte[8];
        MemorySegment.ofArray(buf).set(INT64, 0, value);
        return MemorySegment.ofArray(buf).asReadOnly();
    }

    private static MemorySegment bytes(String s) {
        return MemorySegment.ofArray(s.getBytes(StandardCharsets.UTF_8)).asReadOnly();
    }
}
