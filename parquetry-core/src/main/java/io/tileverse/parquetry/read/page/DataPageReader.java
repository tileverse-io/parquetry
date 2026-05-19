/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.read.page;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;

import io.tileverse.io.ByteBufferPool;

/**
 * Owns the difference between Data Page V1 and V2 on-disk shapes (see the package documentation).
 *
 * <p>Sealed over two implementations: {@link DataPageV2Reader} (Parquet 2.x, the writer-side shape) and
 * {@link DataPageV1Reader} (Parquet 1.x compatibility). Both produce a {@link DecodedPage} - the uniform
 * {@code (repLevels, defLevels, values)} triple the column reader and record assembler consume.
 *
 * <p>Use {@link #forHeader(PageHeader)} to dispatch per page; a column chunk may mix V1 and V2 pages in principle (rare
 * but legal), so the dispatch is per-page, not per-chunk.
 *
 * <p>The {@code compressedPagePayload} buffer passed to {@link #read(PageHeader, LevelMaxima, ByteBuffer, Codec,
 * ByteBufferPool)} is a slice of the parent {@code FetchedColumnChunk}'s pooled compressed buffer; implementations must
 * not retain it past the call and must not close it.
 */
public sealed interface DataPageReader permits DataPageV1Reader, DataPageV2Reader {

    /**
     * Reads one data page.
     *
     * @param header the page header (caller has already verified it carries a data-page payload)
     * @param maxLevels the column's max repetition and definition levels; required by V1 to know whether the payload
     *     carries each level's length prefix. V2 ignores it because its level byte lengths come directly from the
     *     header.
     * @param compressedPagePayload a read-positioned slice of the compressed column-chunk buffer covering exactly this
     *     page's bytes; implementations consume it locally and must not retain the reference
     * @param codec the column chunk's compression codec; used to decompress V2 value bytes and the entire V1 payload
     * @param pool the byte-buffer pool to borrow the decompressed value buffer from; the returned {@link DecodedPage}
     *     owns that pooled buffer and is responsible for releasing it via {@link DecodedPage#close()}
     * @return a {@link DecodedPage} whose pooled value buffer the caller must close when advancing to the next page
     * @throws IOException if decompression fails
     */
    DecodedPage read(
            PageHeader header,
            LevelMaxima maxLevels,
            ByteBuffer compressedPagePayload,
            Codec codec,
            ByteBufferPool pool)
            throws IOException;

    /**
     * Dispatches by inspecting {@code header} for a V2 or V1 data-page sub-header.
     *
     * @throws IllegalArgumentException if {@code header} carries neither a data-page nor a data-page-v2 sub-header
     *     (e.g. it is a dictionary or index page header passed in by mistake)
     */
    static DataPageReader forHeader(PageHeader header) {
        if (header.dataPageHeaderV2().isPresent()) {
            return new DataPageV2Reader();
        }
        if (header.dataPageHeader().isPresent()) {
            return new DataPageV1Reader();
        }
        throw new IllegalArgumentException("PageHeader carries no data page header: type=" + header.type());
    }
}
