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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.codec.ParquetFormatDeserializer;
import io.tileverse.parquetry.internal.write.page.PageStatistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

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
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(1), int32(10), 0L, false));
        builder.appendPage(new PageStatistics(int32(11), int32(20), 1L, false));
        builder.appendPage(new PageStatistics(int32(21), int32(30), 0L, false));

        ColumnIndex original = builder.finishChunk();
        ColumnIndex parsed = roundTrip(original);

        assertThat(parsed).usingRecursiveComparison(memorySegmentByContent()).isEqualTo(original);
        assertThat(parsed.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
        assertThat(parsed.nullPages()).containsExactly(false, false, false);
        assertThat(parsed.nullCounts()).contains(List.of(0L, 1L, 0L));
    }

    @Test
    void allNullPagesEmitMemorySegmentNullForMinMax() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT64, null);
        builder.appendPage(new PageStatistics(MemorySegment.NULL, MemorySegment.NULL, 5L, true));
        builder.appendPage(new PageStatistics(MemorySegment.NULL, MemorySegment.NULL, 7L, true));
        builder.appendPage(new PageStatistics(MemorySegment.NULL, MemorySegment.NULL, 3L, true));

        ColumnIndex original = builder.finishChunk();

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
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(0), int32(5), 1L, false));
        builder.appendPage(new PageStatistics(MemorySegment.NULL, MemorySegment.NULL, 4L, true));
        builder.appendPage(new PageStatistics(int32(6), int32(9), 0L, false));

        ColumnIndex parsed = roundTrip(builder.finishChunk());

        assertThat(parsed.nullPages()).containsExactly(false, true, false);
        assertThat(parsed.boundaryOrder())
                .as("an all-null page between two ordered non-null pages still leaves the non-null sequence ascending")
                .isEqualTo(BoundaryOrder.ASCENDING);
        assertThat(parsed.nullCounts()).contains(List.of(1L, 4L, 0L));
    }

    @Test
    void ascendingInt32PagesYieldAscendingBoundaryOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(1), int32(2), 0L, false));
        builder.appendPage(new PageStatistics(int32(3), int32(4), 0L, false));
        builder.appendPage(new PageStatistics(int32(5), int32(6), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void descendingInt64PagesYieldDescendingBoundaryOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT64, null);
        builder.appendPage(new PageStatistics(int64(100L), int64(200L), 0L, false));
        builder.appendPage(new PageStatistics(int64(50L), int64(99L), 0L, false));
        builder.appendPage(new PageStatistics(int64(0L), int64(49L), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.DESCENDING);
    }

    @Test
    void crossingMinMaxYieldsUnorderedBoundaryOrder() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(1), int32(10), 0L, false));
        builder.appendPage(new PageStatistics(int32(5), int32(7), 0L, false));
        builder.appendPage(new PageStatistics(int32(20), int32(30), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.UNORDERED);
    }

    @Test
    void singleNonNullPageDefaultsToUnordered() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(42), int32(42), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.boundaryOrder())
                .as("a single page can't establish an order; UNORDERED is the safe value")
                .isEqualTo(BoundaryOrder.UNORDERED);
    }

    @Test
    void byteArrayPagesUseUnsignedLexOrdering() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.BYTE_ARRAY, null);
        builder.appendPage(new PageStatistics(bytes("apple"), bytes("banana"), 0L, false));
        builder.appendPage(new PageStatistics(bytes("cherry"), bytes("date"), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.boundaryOrder()).isEqualTo(BoundaryOrder.ASCENDING);
    }

    @Test
    void geometryColumnsAlwaysReportUnorderedBoundaryOrder() {
        ColumnIndexBuilder builder =
                new ColumnIndexBuilder(PrimitiveKind.BYTE_ARRAY, new LogicalType.Geometry(Optional.empty()));
        builder.appendPage(new PageStatistics(bytes("\1\2"), bytes("\3\4"), 0L, false));
        builder.appendPage(new PageStatistics(bytes("\5\6"), bytes("\7\10"), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.boundaryOrder())
                .as("WKB bytes carry no meaningful order; geometry columns must stay UNORDERED")
                .isEqualTo(BoundaryOrder.UNORDERED);
    }

    @Test
    void finishChunkClearsBuilderForReuse() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(1), int32(2), 0L, false));
        builder.appendPage(new PageStatistics(int32(3), int32(4), 0L, false));
        ColumnIndex first = builder.finishChunk();

        builder.appendPage(new PageStatistics(int32(100), int32(200), 5L, false));
        ColumnIndex second = builder.finishChunk();

        assertThat(first.nullPages()).hasSize(2);
        assertThat(second.nullPages()).hasSize(1);
        assertThat(second.nullCounts()).contains(List.of(5L));
    }

    @Test
    void resetClearsAccumulatedPages() {
        ColumnIndexBuilder builder = new ColumnIndexBuilder(PrimitiveKind.INT32, null);
        builder.appendPage(new PageStatistics(int32(1), int32(2), 0L, false));
        builder.reset();
        builder.appendPage(new PageStatistics(int32(10), int32(20), 0L, false));

        ColumnIndex index = builder.finishChunk();

        assertThat(index.nullPages()).hasSize(1);
        assertThat(index.nullCounts()).contains(List.of(0L));
    }

    private static ColumnIndex roundTrip(ColumnIndex original) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ParquetFormat.writeColumnIndex(out, original);
        return ParquetFormatDeserializer.readColumnIndex(new ByteArrayInputStream(out.toByteArray()));
    }

    private static MemorySegment int32(int value) {
        byte[] buf = new byte[4];
        MemorySegment.ofArray(buf).set(INT32, 0, value);
        return MemorySegment.ofArray(buf).asReadOnly();
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
