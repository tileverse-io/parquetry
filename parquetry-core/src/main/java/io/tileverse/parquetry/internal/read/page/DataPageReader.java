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
package io.tileverse.parquetry.internal.read.page;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.schema.LevelMaxima;

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
 * <p>The {@code compressedPagePayload} segment passed to {@link #read(PageHeader, LevelMaxima, MemorySegment,
 * Compression, Arena)} is a slice of the parent {@code FetchedColumnChunk}'s compressed segment; implementations must
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
     * @param compressedPagePayload a read-only slice of the compressed column-chunk segment covering exactly this
     *     page's bytes; implementations consume it locally and must not retain the reference
     * @param codec the column chunk's compression codec; used to decompress V2 value bytes and the entire V1 payload
     * @param pageArena the Arena to allocate the decompressed value bytes from; the caller owns the Arena's lifecycle
     *     and must close it when the page is no longer needed (typically by closing the returned {@link DecodedPage})
     * @return a {@link DecodedPage} backed by {@code pageArena}-allocated segments; the caller must close it when
     *     advancing to the next page
     * @throws IOException if decompression fails
     */
    DecodedPage read(
            PageHeader header,
            LevelMaxima maxLevels,
            MemorySegment compressedPagePayload,
            Compression codec,
            Arena pageArena)
            throws IOException;

    /**
     * Dispatches by inspecting {@code header} for a V2 or V1 data-page sub-header.
     *
     * @throws MalformedFileException if {@code header} carries neither a data-page nor a data-page-v2 sub-header (e.g.
     *     it is a dictionary or index page header passed in by mistake)
     */
    static DataPageReader forHeader(PageHeader header) {
        if (header.dataPageHeaderV2().isPresent()) {
            return new DataPageV2Reader();
        }
        if (header.dataPageHeader().isPresent()) {
            return new DataPageV1Reader();
        }
        throw new MalformedFileException("PageHeader carries no data page header: type=" + header.type());
    }

    /**
     * Collapses Parquet 1.x dictionary tag ({@link Encoding#PLAIN_DICTIONARY}) onto the 2.x tag
     * ({@link Encoding#RLE_DICTIONARY}). Both refer to the same on-disk shape (RLE-bit-packed dictionary indices); the
     * difference is purely metadata. Callers may switch on a single case. Dictionary <em>pages</em> still use
     * {@link Encoding#PLAIN} and are not affected by this normalization.
     */
    static Encoding normalizeEncoding(Encoding encoding) {
        return encoding == Encoding.PLAIN_DICTIONARY ? Encoding.RLE_DICTIONARY : encoding;
    }
}
