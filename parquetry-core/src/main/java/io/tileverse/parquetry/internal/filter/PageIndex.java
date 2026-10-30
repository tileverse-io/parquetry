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

import java.lang.foreign.MemorySegment;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.format.ColumnIndex;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.schema.PrimitiveKind;

/**
 * The page index of one column chunk. It lists the data pages of the chunk in row order, with their bytes, their rows
 * and, when known, their statistics. Only validation builds it. The pruning tiers, the row masks and the fetch read the
 * pages of a chunk through it.
 *
 * <p>The page index declared by the writer is its source: the offset index of the chunk gives the pages, and the column
 * index gives their statistics.
 */
public final class PageIndex {

    private final List<PageLocation> pages;

    private final long rowCount;

    private final Optional<PageStatistics> statistics;

    private PageIndex(List<PageLocation> pages, long rowCount, Optional<PageStatistics> statistics) {
        this.pages = pages;
        this.rowCount = rowCount;
        this.statistics = statistics;
    }

    /**
     * The pages declared by {@code offsetIndex} for a chunk of {@code chunkLength} bytes from {@code chunkStart}, in a
     * row group of {@code rowCount} rows. The result is empty when the pages fail validation. The first page starts at
     * the first row, each later page starts after the rows and the bytes of the previous one, and the last page starts
     * before the last row. Each page lies inside the chunk. An index failing this places rows in the wrong page, or
     * aims a fetch at the bytes of another column. The pages come without statistics; see {@link #withStatistics}.
     */
    public static Optional<PageIndex> declared(
            OffsetIndex offsetIndex, long chunkStart, long chunkLength, long rowCount) {
        List<PageLocation> pages = offsetIndex.pageLocations();
        if (!placesItsPages(pages, chunkStart, chunkStart + chunkLength, rowCount)) {
            return Optional.empty();
        }
        return Optional.of(new PageIndex(pages, rowCount, Optional.empty()));
    }

    /**
     * These pages with the statistics of {@code columnIndex}, or these pages alone when the column index does not agree
     * with them. The column index must have one entry per page. A page marked as holding only nulls must count at least
     * one null per row. If its count is lower, the mark cannot be trusted: the page may hold values, and skipping it
     * would lose rows.
     *
     * @param kind the physical type of the column, used to decode the bounds
     * @param logicalType the logical type of the column, used to decode the bounds into values of that type
     * @param boundsOrdered whether the bounds follow an order applied by this reader. When false the statistics keep
     *     the null marks and the counts alone.
     */
    public PageIndex withStatistics(
            ColumnIndex columnIndex, PrimitiveKind kind, Optional<LogicalType> logicalType, boolean boundsOrdered) {
        if (!hasOneEntryPerPage(columnIndex) || !nullPagesAgreeWithNullCounts(columnIndex)) {
            return new PageIndex(pages, rowCount, Optional.empty());
        }
        PageStatistics agreed = new PageStatistics(columnIndex, kind, logicalType, boundsOrdered);
        return new PageIndex(pages, rowCount, Optional.of(agreed));
    }

    public int pageCount() {
        return pages.size();
    }

    /** The location of each page in the file, in row order. */
    public List<PageLocation> locations() {
        return pages;
    }

    /** The first row of page {@code page}, relative to the row group. */
    public long firstRow(int page) {
        return pages.get(page).firstRowIndex();
    }

    /**
     * The last row of page {@code page}, inclusive. It is the row before the next page, or the last row of the group.
     */
    public long lastRow(int page) {
        boolean lastPage = page + 1 == pages.size();
        if (lastPage) {
            return rowCount - 1;
        }
        return pages.get(page + 1).firstRowIndex() - 1;
    }

    /** The number of rows of page {@code page}. Validation leaves at least one row in each page. */
    public long rows(int page) {
        return lastRow(page) - firstRow(page) + 1;
    }

    /** Whether the column index of the chunk agrees with its pages, and gives the statistics of each page. */
    public boolean hasStatistics() {
        return statistics.isPresent();
    }

    /** Whether the bounds of the pages follow an order applied by this reader. It is false without statistics. */
    public boolean boundsOrdered() {
        return statistics.map(PageStatistics::boundsOrdered).orElse(false);
    }

    /**
     * The statistics of page {@code page} as a unit holding one value per row. A page marked as holding only nulls
     * counts each of its rows as null.
     *
     * @throws IllegalStateException when the page index has no statistics
     */
    public ColumnSummary statistics(int page) {
        return requireStatistics().of(page, rows(page), true);
    }

    /**
     * The null mark and the counts of page {@code page}, without its bounds. A null test needs nothing else, and the
     * bounds are left undecoded.
     *
     * @throws IllegalStateException when the page index has no statistics
     */
    public ColumnSummary nullStatistics(int page) {
        return requireStatistics().of(page, rows(page), false);
    }

    private PageStatistics requireStatistics() {
        return statistics.orElseThrow(() -> new IllegalStateException("the page index has no statistics"));
    }

    /**
     * Whether the pages follow one another in the rows of the row group and in the bytes of the chunk. A row group
     * without rows has no page.
     */
    private static boolean placesItsPages(List<PageLocation> pages, long chunkStart, long chunkEnd, long rowCount) {
        if (pages.isEmpty()) {
            return rowCount == 0L;
        }
        PageLocation previous = pages.get(0);
        if (previous.firstRowIndex() != 0L || !inside(previous, chunkStart, chunkEnd)) {
            return false;
        }
        for (PageLocation page : pages.subList(1, pages.size())) {
            if (!follows(page, previous) || !inside(page, chunkStart, chunkEnd)) {
                return false;
            }
            previous = page;
        }
        return previous.firstRowIndex() < rowCount;
    }

    /** Whether {@code page} holds bytes and starts after the rows and the bytes of {@code previous}. */
    private static boolean follows(PageLocation page, PageLocation previous) {
        boolean laterRows = page.firstRowIndex() > previous.firstRowIndex();
        long endOfPrevious = previous.offset() + previous.compressedPageSize();
        boolean laterBytes = page.offset() >= endOfPrevious;
        return laterRows && laterBytes;
    }

    /** Whether {@code page} holds bytes, all of them within {@code [chunkStart, chunkEnd)}. */
    private static boolean inside(PageLocation page, long chunkStart, long chunkEnd) {
        long size = page.compressedPageSize();
        // Comparing the size with the bytes left keeps the end of a page near the top of the long range from wrapping.
        return size > 0 && page.offset() >= chunkStart && size <= chunkEnd - page.offset();
    }

    /** Whether the null marks and the bounds of {@code columnIndex} have one entry per page. */
    private boolean hasOneEntryPerPage(ColumnIndex columnIndex) {
        int pageCount = pages.size();
        return columnIndex.nullPages().size() == pageCount
                && columnIndex.minValues().size() == pageCount
                && columnIndex.maxValues().size() == pageCount;
    }

    /**
     * Whether each page marked as holding only nulls counts at least one null per row. A column index passes when it
     * has no null counts, or when their number differs from the number of pages.
     */
    private boolean nullPagesAgreeWithNullCounts(ColumnIndex columnIndex) {
        List<Boolean> nullPages = columnIndex.nullPages();
        Optional<List<Long>> perPage = columnIndex.nullCounts().filter(counts -> counts.size() == nullPages.size());
        if (perPage.isEmpty()) {
            return true;
        }
        List<Long> nullCounts = perPage.orElseThrow();
        for (int page = 0; page < nullPages.size(); page++) {
            if (Boolean.TRUE.equals(nullPages.get(page)) && nullCounts.get(page) < rows(page)) {
                return false;
            }
        }
        return true;
    }

    /**
     * The column index of a chunk, validated against its pages, and the type used to decode its bounds.
     *
     * @param index the column index, with one entry per page
     * @param kind the physical type of the column
     * @param logicalType the logical type of the column
     * @param boundsOrdered whether the bounds follow an order applied by this reader
     */
    private record PageStatistics(
            ColumnIndex index, PrimitiveKind kind, Optional<LogicalType> logicalType, boolean boundsOrdered) {

        /**
         * The statistics of page {@code page} of {@code rows} rows. A page marked as holding only nulls counts each of
         * its rows as null, and its bounds are not read. The bounds of the other pages are read only when
         * {@code readBounds} is on.
         */
        ColumnSummary of(int page, long rows, boolean readBounds) {
            OptionalLong values = OptionalLong.of(rows);
            if (Boolean.TRUE.equals(index.nullPages().get(page))) {
                return new ColumnSummary(kind, Optional.empty(), Optional.empty(), values, values, NaNCells.POSSIBLE);
            }
            OptionalLong nullCount = trustedNullCount(countOf(index.nullCounts(), page), rows);
            OptionalLong nanCount = countOf(index.nanCounts(), page);
            Optional<Value> min = readBounds ? bound(index.minValues().get(page)) : Optional.empty();
            Optional<Value> max = readBounds ? bound(index.maxValues().get(page)) : Optional.empty();
            NaNCells counted = NaNCells.ofPageCounts(nanCount, nullCount, rows);
            return ColumnSummary.of(kind, min, max, nullCount, values, counted);
        }

        /**
         * The null count of a page not marked as holding only nulls, or empty when it contradicts the mark. Such a page
         * holds a value, and fewer nulls than rows.
         */
        private static OptionalLong trustedNullCount(OptionalLong recorded, long rows) {
            if (recorded.isPresent() && recorded.getAsLong() >= rows) {
                return OptionalLong.empty();
            }
            return recorded;
        }

        /** The count of page {@code page} in a per-page list of the index, read only when it has one count per page. */
        private OptionalLong countOf(Optional<List<Long>> perPageCounts, int page) {
            int pageCount = index.nullPages().size();
            Optional<List<Long>> counts = perPageCounts.filter(list -> list.size() == pageCount);
            if (counts.isEmpty()) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(counts.orElseThrow().get(page));
        }

        /**
         * A bound of a page decoded to a value. It is empty when the bound does not decode, or when the bounds follow
         * an order not applied by this reader.
         */
        private Optional<Value> bound(MemorySegment bound) {
            if (!boundsOrdered) {
                return Optional.empty();
            }
            return StatisticsValueDecoder.decode(kind, logicalType, bound);
        }
    }
}
