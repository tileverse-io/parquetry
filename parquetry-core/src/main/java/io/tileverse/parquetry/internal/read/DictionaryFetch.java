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
package io.tileverse.parquetry.internal.read;

import java.lang.foreign.MemorySegment;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.page.MemorySegmentInputStream;
import io.tileverse.parquetry.internal.read.page.PageSelection;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * What a narrowed fetch does with the dictionary prefix of a column, the bytes of its chunk before the first data page.
 * The dictionary stays out of the read when each surviving page is known not to use it.
 */
enum DictionaryFetch {

    /** A surviving page may use the dictionary. The first request reads the prefix. */
    WITH_THE_PAGES,

    /**
     * The count of dictionary-encoded pages rules the dictionary out. The prefix keeps its room in the fetch buffer,
     * and the fetch reads it when a fetched page contradicts the count.
     */
    ON_DEMAND,

    /** The recorded encodings of the surviving pages rule the dictionary out. The plan holds no prefix. */
    LEFT_OUT;

    /**
     * What a fetch of the surviving pages of {@code selection} does with the prefix of the column. Encodings recorded
     * by {@code pages} decide first and give an exact answer. A declared index records none, and the count of
     * dictionary-encoded pages of {@code meta} answers for it. A page at or past that count is taken to use no
     * dictionary. The answer assumes that dictionary-encoded pages come first. parquet-mr and Arrow write them first,
     * because they fall back to PLAIN once per chunk. The format does not promise it, and {@link #ON_DEMAND} covers a
     * file breaking the assumption. Without a count, the fetch cannot rule the dictionary out.
     *
     * @param pages the pages of the column, at least one
     * @param selection the pages of the column surviving the row mask
     * @param meta the chunk of the column
     */
    static DictionaryFetch of(PageIndex pages, PageSelection selection, ChunkMeta meta) {
        if (pages.encoding(0).isPresent()) {
            return ofRecordedEncodings(pages, selection);
        }
        OptionalInt dictionaryEncodedPages = meta.dictionaryEncodedPages();
        if (dictionaryEncodedPages.isEmpty()) {
            return WITH_THE_PAGES;
        }
        return ofCount(selection, dictionaryEncodedPages.getAsInt());
    }

    private static DictionaryFetch ofRecordedEncodings(PageIndex pages, PageSelection selection) {
        for (int page = 0; page < selection.pageCount(); page++) {
            if (selection.isSurviving(page) && recordsDictionaryEncoding(pages, page)) {
                return WITH_THE_PAGES;
            }
        }
        return LEFT_OUT;
    }

    private static boolean recordsDictionaryEncoding(PageIndex pages, int page) {
        return pages.encoding(page).filter(PageDecoders::isDictionaryEncoded).isPresent();
    }

    private static DictionaryFetch ofCount(PageSelection selection, int dictionaryEncodedPages) {
        for (int page = 0; page < selection.pageCount(); page++) {
            if (selection.isSurviving(page) && page < dictionaryEncodedPages) {
                return WITH_THE_PAGES;
            }
        }
        return ON_DEMAND;
    }

    /**
     * Whether a data page of {@code run} is dictionary-encoded, from the page headers of the run. The walk steps from
     * header to header over the bytes read by the column reader. A header failing to parse fails here, as it would fail
     * the decode of its page. A page running past the run ends the walk, and the decode of that page reports it.
     *
     * @param run the bytes of a stretch of fetched pages
     * @param path the column of the pages, named by a failure
     */
    static boolean runUsesDictionary(MemorySegment run, ColumnPath path) {
        long position = 0;
        while (position < run.byteSize()) {
            MemorySegmentInputStream stream = MemorySegmentInputStream.pageHeaderAt(run, position, run.byteSize());
            PageHeader header = readPageHeader(stream, path);
            if (isDictionaryEncoded(header)) {
                return true;
            }
            position = stream.position() + payloadBytes(header, path);
        }
        return false;
    }

    private static PageHeader readPageHeader(MemorySegmentInputStream stream, ColumnPath path) {
        try {
            return stream.readPageHeader();
        } catch (ParquetFormatException e) {
            throw e.withContext(
                    "Failed to read page header for column " + path.dot() + ": " + e.getMessage(), -1L, "PageHeader");
        }
    }

    /** Whether {@code header} is the header of a dictionary-encoded data page. A V2 header wins, as in the decode. */
    private static boolean isDictionaryEncoded(PageHeader header) {
        Optional<Encoding> v2 = header.dataPageHeaderV2().map(DataPageHeaderV2::encoding);
        Optional<Encoding> encoding = v2.or(() -> header.dataPageHeader().map(DataPageHeader::encoding));
        return encoding.filter(PageDecoders::isDictionaryEncoded).isPresent();
    }

    /** The payload of the page. A negative size would send the walk back over the pages already read. */
    private static int payloadBytes(PageHeader header, ColumnPath path) {
        int size = header.compressedPageSize();
        if (size < 0) {
            throw new MalformedFileException("Negative compressedPageSize " + size + " for column " + path.dot());
        }
        return size;
    }
}
