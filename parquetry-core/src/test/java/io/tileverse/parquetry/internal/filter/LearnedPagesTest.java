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
package io.tileverse.parquetry.internal.filter;

import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.IntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

class LearnedPagesTest {

    @Test
    void accessorsReadBackEachPage() {
        LearnedPages.Builder builder = int64Builder(3);
        builder.add(0, 120, 0, 1000, Encoding.PLAIN, int64Statistics(OptionalLong.of(0), 10, 99));
        builder.add(120, 140, 1000, 1000, Encoding.RLE_DICTIONARY, int64Statistics(OptionalLong.of(5), 100, 199));
        builder.add(260, 90, 2000, 500, Encoding.PLAIN, int64Statistics(OptionalLong.empty(), 200, 299));

        LearnedPages pages = builder.build();

        assertThat(pages.pageCount()).isEqualTo(3);
        assertThat(eachPage(pages, pages::offset)).containsExactly(0L, 120L, 260L);
        assertThat(eachPage(pages, pages::size)).containsExactly(120, 140, 90);
        assertThat(eachPage(pages, pages::firstRow)).containsExactly(0L, 1000L, 2000L);
        assertThat(eachPage(pages, pages::nullCount))
                .containsExactly(OptionalLong.of(0), OptionalLong.of(5), OptionalLong.empty());
        assertThat(eachPage(pages, pages::encoding))
                .containsExactly(Encoding.PLAIN, Encoding.RLE_DICTIONARY, Encoding.PLAIN);
        assertThat(pages.hasBounds()).isTrue();
        assertThat(eachPage(pages, page -> longOf(pages.min(page)))).containsExactly(10L, 100L, 200L);
        assertThat(eachPage(pages, page -> longOf(pages.max(page)))).containsExactly(99L, 199L, 299L);
    }

    @Test
    void aPageOfOnlyNullsIsMarked() {
        LearnedPages.Builder builder = int64Builder(2);
        builder.add(0, 50, 0, 100, Encoding.PLAIN, nullsOnly(100));
        builder.add(50, 80, 100, 100, Encoding.PLAIN, int64Statistics(OptionalLong.of(99), 1, 9));

        LearnedPages pages = builder.build();

        assertThat(pages.nullPage(0)).isTrue();
        assertThat(pages.nullPage(1)).as("a page holding one value").isFalse();
    }

    // A recorded bound proves a value. Marking the page would let `x = 5` skip rows returned by a full read.
    @Test
    void boundedPageIsNotANullPage() {
        LearnedPages.Builder builder = int64Builder(1);
        builder.add(0, 120, 0, 1000, Encoding.PLAIN, int64Statistics(OptionalLong.of(1000), 5, 5));

        LearnedPages pages = builder.build();

        assertThat(pages.nullPage(0)).isFalse();
        assertThat(pages.hasBounds()).isTrue();
        assertThat(longOf(pages.min(0))).isEqualTo(5L);
        assertThat(pages.nullCount(0)).hasValue(1000);
    }

    // A declared column index has bounds for the pages holding a value, or for no page.
    @Test
    void oneUnboundedPageDropsAllBounds() {
        LearnedPages.Builder builder = int64Builder(3);
        builder.add(0, 120, 0, 1000, Encoding.PLAIN, int64Statistics(OptionalLong.of(0), 10, 99));
        builder.add(120, 140, 1000, 1000, Encoding.PLAIN, int64Statistics(OptionalLong.of(5), 100, 199));
        builder.add(260, 90, 2000, 500, Encoding.PLAIN, Optional.empty());

        LearnedPages pages = builder.build();

        assertThat(pages.hasBounds()).isFalse();
        assertThat(eachPage(pages, pages::min)).containsOnly(Optional.empty());
        assertThat(eachPage(pages, pages::max)).containsOnly(Optional.empty());
        assertThat(eachPage(pages, pages::nullCount))
                .containsExactly(OptionalLong.of(0), OptionalLong.of(5), OptionalLong.empty());
    }

    @Test
    void minWithoutMaxDropsAllBounds() {
        Statistics minOnly = Statistics.builder()
                .nullCount(OptionalLong.of(0))
                .minValue(int64(100))
                .build();
        LearnedPages.Builder builder = int64Builder(2);
        builder.add(0, 120, 0, 1000, Encoding.PLAIN, int64Statistics(OptionalLong.of(0), 10, 99));
        builder.add(120, 140, 1000, 1000, Encoding.PLAIN, Optional.of(minOnly));

        LearnedPages pages = builder.build();

        assertThat(pages.hasBounds()).isFalse();
        assertThat(eachPage(pages, pages::min)).containsOnly(Optional.empty());
    }

    @Test
    void nullPagesDoNotNeedBounds() {
        LearnedPages.Builder builder = int64Builder(2);
        builder.add(0, 120, 0, 1000, Encoding.PLAIN, int64Statistics(OptionalLong.of(0), 10, 99));
        builder.add(120, 40, 1000, 1000, Encoding.PLAIN, nullsOnly(1000));

        LearnedPages pages = builder.build();

        assertThat(pages.hasBounds()).isTrue();
        assertThat(pages.min(0)).isPresent();
        assertThat(pages.min(1)).isEmpty();
        assertThat(pages.max(1)).isEmpty();
    }

    // No page holds a value, and none needs bounds.
    @Test
    void entryOfNullPagesHasBounds() {
        LearnedPages.Builder builder = int64Builder(2);
        builder.add(0, 40, 0, 1000, Encoding.PLAIN, nullsOnly(1000));
        builder.add(40, 40, 1000, 1000, Encoding.PLAIN, nullsOnly(1000));

        LearnedPages pages = builder.build();

        assertThat(pages.hasBounds()).isTrue();
        assertThat(eachPage(pages, pages::min)).containsOnly(Optional.empty());
        assertThat(eachPage(pages, pages::max)).containsOnly(Optional.empty());
    }

    @Test
    void byteSizeCountsTheSegment() {
        LearnedPages onePage = entryOfPages(1);
        LearnedPages threePages = entryOfPages(3);

        assertThat(onePage.byteSize()).isPositive();
        assertThat(threePages.byteSize()).isGreaterThan(onePage.byteSize());
    }

    @Test
    void emptyBuilderIsRejected() {
        LearnedPages.Builder builder = int64Builder(0);

        assertThatIllegalStateException().isThrownBy(builder::build);
    }

    @Test
    void binaryBoundsReadBack() {
        LearnedPages.Builder builder = LearnedPages.builder(PrimitiveKind.BYTE_ARRAY, Optional.empty(), 3);
        builder.add(0, 100, 0, 10, Encoding.PLAIN, utf8Statistics("", "Bern"));
        builder.add(100, 20, 10, 10, Encoding.PLAIN, nullsOnly(10));
        builder.add(120, 100, 20, 10, Encoding.PLAIN, utf8Statistics("Olten", "Zug"));

        LearnedPages pages = builder.build();

        assertThat(eachPage(pages, page -> textOf(pages.min(page))))
                .containsExactly(Optional.of(""), Optional.empty(), Optional.of("Olten"));
        assertThat(eachPage(pages, page -> textOf(pages.max(page))))
                .containsExactly(Optional.of("Bern"), Optional.empty(), Optional.of("Zug"));
        assertThat(pages.max(2))
                .hasValueSatisfying(bound -> assertThat(bound.isReadOnly()).isTrue());
    }

    static Stream<Arguments> narrowBounds() {
        return Stream.of(
                Arguments.of(PrimitiveKind.BOOLEAN, new byte[][] {{0}, {1}, {0}, {1}}),
                Arguments.of(PrimitiveKind.INT32, new byte[][] {int32(-7), int32(3), int32(10), int32(42)}),
                Arguments.of(
                        PrimitiveKind.FLOAT,
                        new byte[][] {float32(-1.5f), float32(0.5f), float32(2.25f), float32(9f)}));
    }

    // The bounds are narrower than a long. A slot placed one value off would read the bound of a neighbor.
    @ParameterizedTest(name = "{0}")
    @MethodSource("narrowBounds")
    void narrowBoundsReadBack(PrimitiveKind kind, byte[][] bounds) {
        LearnedPages.Builder builder = LearnedPages.builder(kind, Optional.empty(), 2);
        builder.add(0, 50, 0, 10, Encoding.PLAIN, boundedBy(bounds[0], bounds[1]));
        builder.add(50, 50, 10, 10, Encoding.PLAIN, boundedBy(bounds[2], bounds[3]));

        LearnedPages pages = builder.build();

        assertThat(bytesOf(pages.min(0))).isEqualTo(bounds[0]);
        assertThat(bytesOf(pages.max(0))).isEqualTo(bounds[1]);
        assertThat(bytesOf(pages.min(1))).isEqualTo(bounds[2]);
        assertThat(bytesOf(pages.max(1))).isEqualTo(bounds[3]);
    }

    @Test
    void valuesBeyondAnIntAreRejected() {
        LearnedPages.Builder builder = int64Builder(1);
        long beyondAnInt = Integer.MAX_VALUE + 1L;
        Optional<Statistics> none = Optional.empty();

        assertThatIllegalArgumentException()
                .as("an offset")
                .isThrownBy(() -> builder.add(beyondAnInt, 10, 0, 1, Encoding.PLAIN, none));
        assertThatIllegalArgumentException()
                .as("a first row")
                .isThrownBy(() -> builder.add(0, 10, beyondAnInt, 1, Encoding.PLAIN, none));
    }

    private static LearnedPages entryOfPages(int pageCount) {
        LearnedPages.Builder builder = int64Builder(pageCount);
        for (int page = 0; page < pageCount; page++) {
            Optional<Statistics> statistics = int64Statistics(OptionalLong.of(0), page, page + 1L);
            builder.add(page * 100L, 100, page * 10L, 10, Encoding.PLAIN, statistics);
        }
        return builder.build();
    }

    private static LearnedPages.Builder int64Builder(int expectedPages) {
        return LearnedPages.builder(PrimitiveKind.INT64, Optional.empty(), expectedPages);
    }

    private static <T> List<T> eachPage(LearnedPages pages, IntFunction<T> accessor) {
        return IntStream.range(0, pages.pageCount()).mapToObj(accessor).toList();
    }

    private static Optional<Statistics> int64Statistics(OptionalLong nullCount, long min, long max) {
        Statistics statistics = Statistics.builder()
                .nullCount(nullCount)
                .minValue(int64(min))
                .maxValue(int64(max))
                .build();
        return Optional.of(statistics);
    }

    private static Optional<Statistics> utf8Statistics(String min, String max) {
        Statistics statistics = Statistics.builder()
                .nullCount(OptionalLong.of(0))
                .minValue(utf8(min))
                .maxValue(utf8(max))
                .build();
        return Optional.of(statistics);
    }

    private static Optional<Statistics> boundedBy(byte[] min, byte[] max) {
        Statistics statistics = Statistics.builder()
                .nullCount(OptionalLong.of(0))
                .minValue(MemorySegment.ofArray(min))
                .maxValue(MemorySegment.ofArray(max))
                .build();
        return Optional.of(statistics);
    }

    private static Optional<Statistics> nullsOnly(long nullCount) {
        Statistics statistics =
                Statistics.builder().nullCount(OptionalLong.of(nullCount)).build();
        return Optional.of(statistics);
    }

    private static MemorySegment int64(long value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Long.BYTES]);
        segment.set(INT64, 0, value);
        return segment;
    }

    private static byte[] int32(int value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Integer.BYTES]);
        segment.set(INT32, 0, value);
        return segment.toArray(JAVA_BYTE);
    }

    private static byte[] float32(float value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Float.BYTES]);
        segment.set(FLOAT, 0, value);
        return segment.toArray(JAVA_BYTE);
    }

    private static MemorySegment utf8(String text) {
        return MemorySegment.ofArray(text.getBytes(StandardCharsets.UTF_8));
    }

    private static long longOf(Optional<MemorySegment> bound) {
        return bound.orElseThrow().get(INT64, 0);
    }

    private static byte[] bytesOf(Optional<MemorySegment> bound) {
        return bound.orElseThrow().toArray(JAVA_BYTE);
    }

    private static Optional<String> textOf(Optional<MemorySegment> bound) {
        return bound.map(segment -> new String(segment.toArray(JAVA_BYTE), StandardCharsets.UTF_8));
    }
}
