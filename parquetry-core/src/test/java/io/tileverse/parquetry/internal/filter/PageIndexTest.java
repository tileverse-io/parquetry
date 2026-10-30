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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.BoundaryOrder;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.schema.PrimitiveKind;

class PageIndexTest {

    /** Three pages of 100 rows, placed back to back in the bytes [0, 300) of their chunk. */
    private static final OffsetIndex THREE_PAGES = TestPageIndexes.offsetIndex(0, 100, 200);

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
}
