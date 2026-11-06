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
package io.tileverse.parquetry.internal.read.page;

import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.format.Statistics;
import io.tileverse.parquetry.internal.filter.LearnedPages;
import io.tileverse.parquetry.schema.PrimitiveKind;

import lombok.NonNull;

/**
 * Learns the data pages of a column chunk from its page headers. The chunk is read whole, from its first page to its
 * end. The scan reads the header of each page in turn, and it steps over the dictionary page and the other pages
 * holding no data.
 *
 * <p>The scan gives an exact entry or none. The entry lists each data page of the chunk, and the rows of its pages add
 * up to the rows of the row group. A header failing to parse, a negative count, a data page without values, a page
 * running past the chunk, or rows disagreeing with the row group give no entry. A chunk of more than
 * {@link #MAX_DATA_PAGES} data pages gives none either, and neither does a chunk with more than
 * {@link #MAX_BOUND_BYTES} bytes of bounds in its data page headers. A read without an entry reads the chunk as it does
 * without a page index.
 *
 * <p>The entry holds the offsets, the sizes and the first rows of its pages as ints. A chunk above
 * {@link Integer#MAX_VALUE} bytes gives no entry, and neither does a page starting beyond row
 * {@link Integer#MAX_VALUE}. The rows of a V1 page are its values, as in a flat column, and a V2 page must count as
 * many rows as values. The caller scans flat columns only.
 */
public final class PageHeaderScan {

    /**
     * The most data pages learned from one chunk. The scan holds a record of each data page, with the statistics of its
     * header, until it packs the entry. Measured at the ceiling, these records take about 7 MiB of heap without
     * statistics, about 40 MiB with a minimum and a maximum of 8 bytes, and about 52 MiB when the header also records
     * them in the deprecated fields. A chunk of more data pages gives no entry. Chunks written with the usual defaults
     * stay below the ceiling. A chunk under 2 GiB in pages of 1 MiB has 2,048 of them, and the pages of 20,000 rows
     * written by parquet-mr give about 54,000 for a row group of 128 MiB of booleans.
     */
    static final int MAX_DATA_PAGES = 1 << 16;

    /**
     * The most bytes of bounds learned from the data page headers of one chunk, summed over the minimums and the
     * maximums of their current and deprecated fields. The scan holds the bounds copied from the headers until it packs
     * the entry, and the entry cuts the long ones. The cap bounds these copies to 64 MiB of heap on the fetch thread,
     * and a chunk recording more gives no entry. Chunks written with the usual defaults stay below the cap. The Arrow
     * writer used by pyarrow writes no page bound above 4 KiB by default, as set by its {@code max_statistics_size},
     * and 2,048 pages of 1 MiB with two bounds of 4 KiB each hold 16 MiB.
     */
    static final int MAX_BOUND_BYTES = 64 << 20;

    private PageHeaderScan() {}

    /**
     * The entry of a whole chunk, parsed from its page headers, or empty when the headers do not describe the chunk.
     *
     * @param chunk the bytes of the chunk, from its first page to its end
     * @param rowCount the rows of the row group
     * @param kind the physical type of the column, used to choose the bounds kept by the entry
     * @param logicalType the logical type of the column in the schema built by the reader, used to choose the bounds
     *     kept by the entry. That schema gives a geometry type to the WKB columns named by GeoParquet 1.x metadata, and
     *     the entry keeps no bounds for them.
     */
    public static Optional<LearnedPages> scan(
            @NonNull MemorySegment chunk,
            long rowCount,
            @NonNull PrimitiveKind kind,
            @NonNull Optional<LogicalType> logicalType) {
        if (chunk.byteSize() > Integer.MAX_VALUE) {
            return Optional.empty();
        }
        Optional<List<DataPage>> dataPages;
        try {
            dataPages = dataPagesOf(chunk);
        } catch (ParquetFormatException | UncheckedIOException _) {
            return Optional.empty();
        }
        try {
            return dataPages.flatMap(pages -> entryOf(pages, rowCount, kind, logicalType));
        } catch (IllegalArgumentException | IllegalStateException _) {
            // The builder rejects a first row beyond an int, and an entry larger than one heap array.
            return Optional.empty();
        }
    }

    /**
     * The data pages of {@code chunk} in chunk order, or empty when they pass {@link #MAX_DATA_PAGES} or
     * {@link #MAX_BOUND_BYTES}. The pages must fill the chunk to its last byte.
     */
    private static Optional<List<DataPage>> dataPagesOf(MemorySegment chunk) {
        List<DataPage> dataPages = new ArrayList<>();
        long boundBytes = 0;
        long offset = 0;
        while (offset < chunk.byteSize()) {
            MemorySegmentInputStream stream = MemorySegmentInputStream.pageHeaderAt(chunk, offset, chunk.byteSize());
            PageHeader header = stream.readPageHeader();
            int size = pageSize(chunk, offset, stream.position(), header.compressedPageSize());
            if (isDataPage(header)) {
                DataPage page = DataPage.of(header, offset, size);
                boundBytes += page.boundBytes();
                if (passesACeiling(dataPages.size() + 1, boundBytes)) {
                    return Optional.empty();
                }
                dataPages.add(page);
            }
            offset += size;
        }
        return Optional.of(dataPages);
    }

    /**
     * The bytes of a page, its header included. The header runs from {@code offset} to {@code payloadStart}. The
     * payload follows it and must lie inside the chunk.
     */
    private static int pageSize(MemorySegment chunk, long offset, long payloadStart, int payloadBytes) {
        long bytesLeft = chunk.byteSize() - payloadStart;
        if (payloadBytes < 0) {
            throw new MalformedFileException("A page header declares a payload of " + payloadBytes + " bytes");
        }
        if (payloadBytes > bytesLeft) {
            throw new MalformedFileException(
                    "A page payload of " + payloadBytes + " bytes runs past the chunk, " + bytesLeft + " bytes left");
        }
        // The page lies inside the chunk, and the chunk fits an int.
        return (int) (payloadStart - offset + payloadBytes);
    }

    private static boolean isDataPage(PageHeader header) {
        return header.type() == PageType.DATA_PAGE || header.type() == PageType.DATA_PAGE_V2;
    }

    /** Whether {@code pageCount} data pages with {@code boundBytes} bytes of bounds pass a ceiling of the scan. */
    private static boolean passesACeiling(int pageCount, long boundBytes) {
        return pageCount > MAX_DATA_PAGES || boundBytes > MAX_BOUND_BYTES;
    }

    /**
     * The entry of {@code dataPages}, or empty when the chunk has no data page or when the rows of its pages do not add
     * up to {@code rowCount}. The first row of a page is the sum of the rows of the pages before it.
     */
    private static Optional<LearnedPages> entryOf(
            List<DataPage> dataPages, long rowCount, PrimitiveKind kind, Optional<LogicalType> logicalType) {
        if (dataPages.isEmpty() || rowsOf(dataPages) != rowCount) {
            return Optional.empty();
        }
        LearnedPages.Builder entry = LearnedPages.builder(kind, logicalType, dataPages.size());
        long firstRow = 0;
        for (DataPage page : dataPages) {
            entry.add(page.offset(), page.size(), firstRow, page.values(), page.encoding(), page.statistics());
            firstRow += page.rows();
        }
        return Optional.of(entry.build());
    }

    private static long rowsOf(List<DataPage> dataPages) {
        return dataPages.stream().mapToLong(DataPage::rows).sum();
    }

    /**
     * A data page of the chunk, with the counts, the encoding and the statistics of its header.
     *
     * @param offset the position of the page header, in bytes from the start of the chunk
     * @param size the bytes of the page, its header included
     * @param values the values of the page, its nulls included
     * @param rows the rows of the page
     * @param encoding the encoding of the values of the page
     * @param statistics the statistics of the page header, empty when the header records none
     */
    private record DataPage(
            long offset,
            int size,
            long values,
            long rows,
            @NonNull Encoding encoding,
            @NonNull Optional<Statistics> statistics) {

        /**
         * The data page of {@code header}. A V2 header wins over a V1 header, as in {@link DataPageReader#forHeader}.
         */
        static DataPage of(PageHeader header, long offset, int size) {
            Optional<DataPageHeaderV2> v2 = header.dataPageHeaderV2();
            if (v2.isPresent()) {
                return ofV2(v2.orElseThrow(), offset, size);
            }
            Optional<DataPageHeader> v1 = header.dataPageHeader();
            if (v1.isPresent()) {
                return ofV1(v1.orElseThrow(), offset, size);
            }
            throw new MalformedFileException("A " + header.type() + " header holds no data page header");
        }

        /** A V1 page of a flat column holds one value per row. */
        private static DataPage ofV1(DataPageHeader v1, long offset, int size) {
            int values = requireValues(v1.numValues());
            return new DataPage(offset, size, values, values, v1.encoding(), v1.statistics());
        }

        /** A V2 page of a flat column must count as many rows as values. */
        private static DataPage ofV2(DataPageHeaderV2 v2, long offset, int size) {
            int values = requireValues(v2.numValues());
            int rows = requireNonNegative(v2.numRows(), "rows");
            requireNonNegative(v2.numNulls(), "nulls");
            if (rows != values) {
                throw new MalformedFileException(
                        "A V2 data page header counts " + rows + " rows and " + values + " values");
            }
            return new DataPage(offset, size, values, rows, v2.encoding(), v2.statistics());
        }

        /** The values of a data page. A data page holds at least one value. */
        private static int requireValues(int values) {
            if (values < 1) {
                throw new MalformedFileException("A data page header counts " + values + " values");
            }
            return values;
        }

        private static int requireNonNegative(int count, String countedItems) {
            if (count < 0) {
                throw new MalformedFileException("A data page header counts " + count + " " + countedItems);
            }
            return count;
        }

        /** The bytes of the minimums and the maximums recorded by the header, in its current and deprecated fields. */
        long boundBytes() {
            if (statistics.isEmpty()) {
                return 0;
            }
            Statistics recorded = statistics.orElseThrow();
            return recorded.min().byteSize()
                    + recorded.max().byteSize()
                    + recorded.minValue().byteSize()
                    + recorded.maxValue().byteSize();
        }
    }
}
