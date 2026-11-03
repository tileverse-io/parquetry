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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.DataPageHeader;
import io.tileverse.parquetry.format.DataPageHeaderV2;
import io.tileverse.parquetry.format.DictionaryPageHeader;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * The walk of the page headers of a fetched run. It finds a page contradicting the count of dictionary-encoded pages.
 * The plan-time decision is covered with a real file in {@link PageNarrowedPlanTest}.
 */
class DictionaryFetchTest {

    private static final ColumnPath COLUMN = ColumnPath.of("label");
    private static final int PAYLOAD_BYTES = 4;

    @Test
    void plainPagesUseNoDictionary() {
        MemorySegment run = runOf(v1Page(Encoding.PLAIN), v1Page(Encoding.DELTA_BINARY_PACKED), v2Page(Encoding.PLAIN));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isFalse();
    }

    @Test
    void aLaterPageUsesIt() {
        MemorySegment run = runOf(v1Page(Encoding.PLAIN), v1Page(Encoding.RLE_DICTIONARY));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isTrue();
    }

    @Test
    void theParquetOneTagUsesIt() {
        MemorySegment run = runOf(v1Page(Encoding.PLAIN_DICTIONARY));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isTrue();
    }

    @Test
    void aV2PageUsesIt() {
        MemorySegment run = runOf(v2Page(Encoding.PLAIN), v2Page(Encoding.RLE_DICTIONARY));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isTrue();
    }

    // The column reader decodes a page by its V2 header when it has one.
    @Test
    void aV2HeaderWinsOverItsV1Header() {
        MemorySegment run = runOf(pageWithBothHeaders(Encoding.RLE_DICTIONARY, Encoding.PLAIN));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isFalse();
    }

    @Test
    void aDictionaryPageIsSteppedOver() {
        MemorySegment run = runOf(dictionaryPage(), v1Page(Encoding.PLAIN));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isFalse();
    }

    // The walk goes backwards on a negative size, and a page ending where it started would never end the walk.
    @Test
    void aNegativePageSizeFails() {
        PageHeader negative = withCompressedSize(v1Header(Encoding.PLAIN), -1);
        MemorySegment run = runOf(pageOf(negative));

        assertThatThrownBy(() -> DictionaryFetch.runUsesDictionary(run, COLUMN))
                .isInstanceOf(MalformedFileException.class)
                .hasMessageContaining(COLUMN.dot());
    }

    @Test
    void anUnreadableHeaderFails() {
        byte[] garbage = new byte[Long.BYTES];
        Arrays.fill(garbage, (byte) 0xFF);
        MemorySegment run = runOf(v1Page(Encoding.PLAIN), garbage);

        assertThatThrownBy(() -> DictionaryFetch.runUsesDictionary(run, COLUMN))
                .isInstanceOf(ParquetFormatException.class)
                .hasMessageContaining(COLUMN.dot());
    }

    // The walk ends past the last byte of the run, and the failure stays with the decode of the page.
    @Test
    void aPagePastTheRunEndsTheWalk() {
        PageHeader longer = withCompressedSize(v1Header(Encoding.PLAIN), PAYLOAD_BYTES * 2);
        MemorySegment run = runOf(pageOf(longer));

        assertThat(DictionaryFetch.runUsesDictionary(run, COLUMN)).isFalse();
    }

    private static MemorySegment runOf(byte[]... pages) {
        ByteArrayOutputStream run = new ByteArrayOutputStream();
        for (byte[] page : pages) {
            run.writeBytes(page);
        }
        return MemorySegment.ofArray(run.toByteArray()).asReadOnly();
    }

    private static byte[] v1Page(Encoding encoding) {
        return pageOf(v1Header(encoding));
    }

    private static byte[] v2Page(Encoding encoding) {
        DataPageHeaderV2 v2 = v2Header(encoding);
        return pageOf(header(PageType.DATA_PAGE_V2, Optional.empty(), Optional.empty(), Optional.of(v2)));
    }

    private static byte[] dictionaryPage() {
        DictionaryPageHeader dictionary = new DictionaryPageHeader(1, Encoding.PLAIN, false);
        return pageOf(header(PageType.DICTIONARY_PAGE, Optional.empty(), Optional.of(dictionary), Optional.empty()));
    }

    private static byte[] pageWithBothHeaders(Encoding v1Encoding, Encoding v2Encoding) {
        DataPageHeader v1 = v1Sections(v1Encoding);
        DataPageHeaderV2 v2 = v2Header(v2Encoding);
        return pageOf(header(PageType.DATA_PAGE_V2, Optional.of(v1), Optional.empty(), Optional.of(v2)));
    }

    /** The bytes of {@code header} and of a payload of zeros, of {@value #PAYLOAD_BYTES} bytes. */
    private static byte[] pageOf(PageHeader header) {
        ByteArrayOutputStream page = new ByteArrayOutputStream();
        ParquetFormat.writePageHeader(page, header);
        page.writeBytes(new byte[PAYLOAD_BYTES]);
        return page.toByteArray();
    }

    private static PageHeader v1Header(Encoding encoding) {
        return header(PageType.DATA_PAGE, Optional.of(v1Sections(encoding)), Optional.empty(), Optional.empty());
    }

    private static DataPageHeader v1Sections(Encoding encoding) {
        return new DataPageHeader(1, encoding, Encoding.RLE, Encoding.RLE, Optional.empty());
    }

    private static DataPageHeaderV2 v2Header(Encoding encoding) {
        return new DataPageHeaderV2(1, 0, 1, encoding, 0, 0, false, Optional.empty());
    }

    private static PageHeader header(
            PageType type,
            Optional<DataPageHeader> v1,
            Optional<DictionaryPageHeader> dictionary,
            Optional<DataPageHeaderV2> v2) {
        return new PageHeader(
                type, PAYLOAD_BYTES, PAYLOAD_BYTES, OptionalInt.empty(), v1, Optional.empty(), dictionary, v2);
    }

    private static PageHeader withCompressedSize(PageHeader header, int compressedSize) {
        return new PageHeader(
                header.type(),
                header.uncompressedPageSize(),
                compressedSize,
                header.crc(),
                header.dataPageHeader(),
                header.indexPageHeader(),
                header.dictionaryPageHeader(),
                header.dataPageHeaderV2());
    }
}
