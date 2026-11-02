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

import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.schema.PrimitiveKind;

class PageIndexTest {

    /** Three pages of 100 rows, placed back to back in the bytes [0, 300) of their chunk. */
    private static final OffsetIndex THREE_PAGES = TestPageIndexes.offsetIndex(0, 100, 200);

    /**
     * Three INT64 pages of 100 rows learned from their headers, placed back to back in the first 300 bytes of their
     * chunk. The first page holds only nulls. The second holds 5 nulls and values from 10 to 99, the third values from
     * 100 to 199.
     */
    private static final LearnedPages THREE_LEARNED_PAGES = threeLearnedPages();

    @Test
    void pagesInOrderAreDeclared() {
        PageIndex pages = PageIndex.declared(THREE_PAGES, 0, 300, 300).orElseThrow();

        assertThat(pages.pageCount()).isEqualTo(3);
        assertThat(pages.lastRow(0)).isEqualTo(99);
        assertThat(pages.lastRow(2)).as("the last page ends at the last row").isEqualTo(299);
        assertThat(pages.rows(1)).isEqualTo(100);
        assertThat(pages.hasStatistics()).isFalse();
    }

    // A page without rows places no row, and the rows after it land in the wrong page.
    @Test
    void pageWithoutRowsFails() {
        OffsetIndex pageWithoutRows = TestPageIndexes.offsetIndex(0, 100, 100);

        assertThat(PageIndex.declared(pageWithoutRows, 0, 300, 300)).isEmpty();
    }

    // A fetch narrowed on such a page would read the bytes of another column.
    @Test
    void pageOutsideTheChunkFails() {
        assertThat(PageIndex.declared(THREE_PAGES, 0, 299, 300))
                .as("a last page ending past the chunk")
                .isEmpty();
        assertThat(PageIndex.declared(THREE_PAGES, 1, 299, 300))
                .as("a first page before the chunk")
                .isEmpty();
    }

    // The end of a page near the top of the long range must not wrap around into the chunk.
    @Test
    void wrappingPageEndFails() {
        PageLocation first = new PageLocation(0, 100, 0);
        PageLocation wrapping = new PageLocation(Long.MAX_VALUE - 10, 100, 100);
        OffsetIndex offsetIndex = new OffsetIndex(List.of(first, wrapping), Optional.empty());

        assertThat(PageIndex.declared(offsetIndex, 0, 200, 200)).isEmpty();
    }

    @Test
    void emptyGroupHasNoPages() {
        OffsetIndex noPages = new OffsetIndex(List.of(), Optional.empty());

        assertThat(PageIndex.declared(noPages, 0, 0, 0))
                .hasValueSatisfying(pages -> assertThat(pages.pageCount()).isZero());
        assertThat(PageIndex.declared(noPages, 0, 0, 10))
                .as("rows without pages")
                .isEmpty();
    }

    @Test
    void shortColumnIndexIsDropped() {
        PageIndex pages = TestPageIndexes.pages(300, 0, 100, 200);
        ColumnIndex twoPages = columnIndex(List.of(false, false), Optional.empty());

        assertThat(withStatistics(pages, twoPages).hasStatistics()).isFalse();
    }

    // A page marked as holding only nulls, with fewer nulls than rows, may hold values: skipping it would lose rows.
    @Test
    void nullPageShortOfNullsIsDropped() {
        PageIndex pages = TestPageIndexes.pages(300, 0, 100, 200);
        List<Boolean> middlePageOfNulls = List.of(false, true, false);
        ColumnIndex shortOfNulls = columnIndex(middlePageOfNulls, Optional.of(List.of(0L, 50L, 0L)));
        ColumnIndex eachRowNull = columnIndex(middlePageOfNulls, Optional.of(List.of(0L, 100L, 0L)));

        assertThat(withStatistics(pages, shortOfNulls).hasStatistics()).isFalse();
        assertThat(withStatistics(pages, eachRowNull).hasStatistics()).isTrue();
    }

    @Test
    void nullPageCountsEachRowAsNull() {
        PageIndex pages = TestPageIndexes.pages(300, 0, 100, 200);
        ColumnIndex middlePageOfNulls = columnIndex(List.of(false, true, false), Optional.empty());

        ColumnSummary middle = withStatistics(pages, middlePageOfNulls).statistics(1);

        assertThat(middle.onlyNulls()).isTrue();
        assertThat(middle.min()).isEmpty();
    }

    @Test
    void nullStatisticsSkipTheBounds() {
        PageIndex pages = TestPageIndexes.pages(100, 0);
        PageIndex indexed = withStatistics(pages, columnIndex(List.of(false), Optional.of(List.of(0L))));

        assertThat(indexed.statistics(0).min()).contains(new Value.IntVal(10));
        assertThat(indexed.nullStatistics(0).min()).isEmpty();
        assertThat(indexed.nullStatistics(0).noNulls()).isTrue();
    }

    @Test
    void statisticsNeedAColumnIndex() {
        PageIndex pages = TestPageIndexes.pages(100, 0);

        assertThatIllegalStateException().isThrownBy(() -> pages.statistics(0));
    }

    @Test
    void learnedPagesPlaceLikeDeclared() {
        List<PageLocation> twinPages = List.of(
                new PageLocation(4_000, 100, 0), new PageLocation(4_100, 100, 100), new PageLocation(4_200, 100, 200));
        OffsetIndex declaredTwin = new OffsetIndex(twinPages, Optional.empty());
        PageIndex declared = PageIndex.declared(declaredTwin, 4_000, 300, 300).orElseThrow();

        PageIndex fromHeaders = learned(THREE_LEARNED_PAGES, 4_000, 300, 300).orElseThrow();

        assertThat(fromHeaders.locations()).isEqualTo(declared.locations());
    }

    @Test
    void learnedStatisticsReadBounds() {
        PageIndex pages = learned(THREE_LEARNED_PAGES, 0, 300, 300).orElseThrow();

        ColumnSummary second = pages.statistics(1);

        assertThat(second.min()).contains(new Value.LongVal(10));
        assertThat(second.max()).contains(new Value.LongVal(99));
        assertThat(second.nullCount()).hasValue(5);
    }

    @Test
    void learnedNullPageCountsEachRow() {
        PageIndex pages = learned(THREE_LEARNED_PAGES, 0, 300, 300).orElseThrow();

        ColumnSummary first = pages.statistics(0);

        assertThat(first.onlyNulls()).isTrue();
        assertThat(first.nullCount()).hasValue(100);
        assertThat(first.min()).isEmpty();
    }

    @Test
    void learnedWithoutBoundsKeepsCounts() {
        LearnedPages.Builder builder = int64Builder(2);
        builder.add(0, 100, 0, 100, Encoding.PLAIN, nullCountOnly(3));
        builder.add(100, 100, 100, 100, Encoding.PLAIN, nullCountOnly(7));
        LearnedPages withoutBounds = builder.build();

        PageIndex pages = learned(withoutBounds, 0, 200, 200).orElseThrow();

        assertThat(withoutBounds.hasBounds()).isFalse();
        assertThat(pages.statistics(0).bounded()).isFalse();
        assertThat(pages.statistics(1).bounded()).isFalse();
        assertThat(pages.statistics(0).nullCount()).hasValue(3);
        assertThat(pages.statistics(1).nullCount()).hasValue(7);
    }

    @Test
    void learnedBoundsNeedAnOrder() {
        Optional<PageIndex> unordered =
                PageIndex.learned(THREE_LEARNED_PAGES, 0, 300, 300, PrimitiveKind.INT64, Optional.empty(), false);
        PageIndex pages = unordered.orElseThrow();

        assertThat(pages.boundsOrdered()).isFalse();
        assertThat(pages.statistics(1).bounded()).isFalse();
        assertThat(pages.statistics(1).nullCount()).hasValue(5);
    }

    @Test
    void learnedBoundsUseLogicalType() {
        Optional<LogicalType> decimal = Optional.of(new LogicalType.Decimal(2, 18));
        LearnedPages.Builder builder = LearnedPages.builder(PrimitiveKind.INT64, decimal, 1);
        builder.add(0, 100, 0, 100, Encoding.PLAIN, int64Statistics(0, 1050, 9999));
        LearnedPages prices = builder.build();

        Optional<PageIndex> decimals = PageIndex.learned(prices, 0, 100, 100, PrimitiveKind.INT64, decimal, true);
        PageIndex pages = decimals.orElseThrow();

        assertThat(pages.statistics(0).min()).contains(new Value.DecimalVal(new BigDecimal("10.50")));
        assertThat(pages.statistics(0).max()).contains(new Value.DecimalVal(new BigDecimal("99.99")));
    }

    // A fetch narrowed on such a page would read the bytes of another column.
    @Test
    void misplacedLearnedPagesAreEmpty() {
        assertThat(learned(THREE_LEARNED_PAGES, 0, 299, 300))
                .as("a last page ending past the chunk")
                .isEmpty();
    }

    // Page 0 counts 50 nulls in 100 rows. The first rows of the pages come from the same headers and cannot be trusted.
    @Test
    void nullMarkBelowCountDropsEntry() {
        LearnedPages.Builder builder = int64Builder(2);
        builder.add(0, 100, 0, 50, Encoding.PLAIN, nullCountOnly(50));
        builder.add(100, 100, 100, 100, Encoding.PLAIN, int64Statistics(0, 10, 99));
        LearnedPages shortOfNulls = builder.build();

        Optional<PageIndex> contradicting = learned(shortOfNulls, 0, 200, 200);
        Optional<PageIndex> agreeing = learned(THREE_LEARNED_PAGES, 0, 300, 300);

        assertThat(contradicting).isEmpty();
        assertThat(agreeing).as("a page of nulls counting one null per row").isPresent();
    }

    // The header counts as many nulls as rows, but its bounds prove a value. A null test must not skip the page.
    @Test
    void unmarkedPageIsNotOnlyNulls() {
        LearnedPages.Builder builder = int64Builder(1);
        builder.add(0, 100, 0, 100, Encoding.PLAIN, int64Statistics(100, 5, 5));
        LearnedPages contradicting = builder.build();

        PageIndex pages = learned(contradicting, 0, 100, 100).orElseThrow();

        assertThat(pages.nullStatistics(0).onlyNulls()).isFalse();
        assertThat(pages.statistics(0).bounded()).isTrue();
    }

    private static PageIndex withStatistics(PageIndex pages, ColumnIndex columnIndex) {
        return pages.withStatistics(columnIndex, PrimitiveKind.INT32, Optional.empty(), true);
    }

    /** An INT32 column index bounding each page by 10 and 20, with the given null marks and counts. */
    private static ColumnIndex columnIndex(List<Boolean> nullPages, Optional<List<Long>> nullCounts) {
        int pageCount = nullPages.size();
        List<MemorySegment> mins = Collections.nCopies(pageCount, encodeInt(10));
        List<MemorySegment> maxs = Collections.nCopies(pageCount, encodeInt(20));
        return new ColumnIndex(
                nullPages,
                mins,
                maxs,
                BoundaryOrder.UNORDERED,
                nullCounts,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    private static MemorySegment encodeInt(int value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[4]);
        segment.set(INT32, 0, value);
        return segment.asReadOnly();
    }

    /** The page index learned from {@code pages} for an INT64 column with ordered bounds. */
    private static Optional<PageIndex> learned(LearnedPages pages, long chunkStart, long chunkLength, long rowCount) {
        return PageIndex.learned(pages, chunkStart, chunkLength, rowCount, PrimitiveKind.INT64, Optional.empty(), true);
    }

    private static LearnedPages threeLearnedPages() {
        LearnedPages.Builder builder = int64Builder(3);
        builder.add(0, 100, 0, 100, Encoding.PLAIN, nullCountOnly(100));
        builder.add(100, 100, 100, 100, Encoding.PLAIN, int64Statistics(5, 10, 99));
        builder.add(200, 100, 200, 100, Encoding.PLAIN, int64Statistics(0, 100, 199));
        return builder.build();
    }

    private static LearnedPages.Builder int64Builder(int expectedPages) {
        return LearnedPages.builder(PrimitiveKind.INT64, Optional.empty(), expectedPages);
    }

    /** The statistics of a page header recording a null count and no bound. */
    private static Optional<Statistics> nullCountOnly(long nullCount) {
        Statistics statistics =
                Statistics.builder().nullCount(OptionalLong.of(nullCount)).build();
        return Optional.of(statistics);
    }

    private static Optional<Statistics> int64Statistics(long nullCount, long min, long max) {
        Statistics statistics = Statistics.builder()
                .nullCount(OptionalLong.of(nullCount))
                .minValue(encodeLong(min))
                .maxValue(encodeLong(max))
                .build();
        return Optional.of(statistics);
    }

    private static MemorySegment encodeLong(long value) {
        MemorySegment segment = MemorySegment.ofArray(new byte[Long.BYTES]);
        segment.set(INT64, 0, value);
        return segment.asReadOnly();
    }
}
