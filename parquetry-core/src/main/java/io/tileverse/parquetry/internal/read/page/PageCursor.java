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
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.PageHeader;
import io.tileverse.parquetry.format.PageType;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;

/**
 * Walks the chunk's compressed bytes, yielding one decompressed {@link DecodedPage} per call.
 *
 * <p>The cursor tracks a byte offset inside the chunk's {@link MemorySegment} and dispatches header reads through
 * {@link ParquetFormat#readPageHeader}. Non-data pages (e.g. a misplaced dictionary or index page) are skipped
 * silently; {@link #nextDataPage} returns {@code null} when the chunk is exhausted.
 *
 * <p>Shared by every column reader that walks a chunk page by page.
 */
public final class PageCursor {

    private final MemorySegment chunk;
    private final long limit;
    private final ColumnPath columnPath;
    private final PageSelection selection; // null = no page-skip (decode every data page)
    private long position;
    private int dataPageOrdinal;
    private long currentPageFirstRowIndex;
    private int decodedDataPageCount;
    private int skippedDataPageCount;

    public PageCursor(MemorySegment chunk, ColumnPath columnPath) {
        this(chunk, columnPath, null);
    }

    public PageCursor(MemorySegment chunk, ColumnPath columnPath, PageSelection selection) {
        this.chunk = chunk;
        this.limit = chunk.byteSize();
        this.columnPath = columnPath;
        this.selection = selection;
        this.position = 0L;
    }

    /**
     * Returns the next {@link DecodedPage} from the chunk's compressed bytes, or {@code null} when the cursor is
     * exhausted. Non-data pages are skipped. When a {@link PageSelection} is present, data pages whose row span does
     * not overlap the surviving rows are advanced past without decompressing or decoding.
     */
    public DecodedPage nextDataPage(LevelMaxima maxLevels, Compression codec, Arena pageArena) throws IOException {
        while (position < limit) {
            PageHeader header = readNextPageHeader();
            int compressedSize = header.compressedPageSize();
            if (compressedSize < 0) {
                throw new MalformedFileException(
                        "Negative compressedPageSize " + compressedSize + " for column " + columnPath.dot());
            }
            boolean isDataPage = header.type() == PageType.DATA_PAGE || header.type() == PageType.DATA_PAGE_V2;
            if (!isDataPage) {
                sliceAndAdvance(compressedSize);
                continue;
            }
            int ordinal = dataPageOrdinal++;
            MemorySegment pagePayload = sliceAndAdvance(compressedSize);
            if (selection == null || selection.isSurviving(ordinal)) {
                currentPageFirstRowIndex = (selection != null) ? selection.firstRowIndex(ordinal) : 0L;
                decodedDataPageCount++;
                return DataPageReader.forHeader(header).read(header, maxLevels, pagePayload, codec, pageArena);
            }
            skippedDataPageCount++;
        }
        return null;
    }

    /** First row index (relative to the row group) of the page most recently returned by {@link #nextDataPage}. */
    public long currentPageFirstRowIndex() {
        return currentPageFirstRowIndex;
    }

    /** Data pages decompressed and decoded so far; excludes pages skipped by the {@link PageSelection}. */
    public int decodedDataPageCount() {
        return decodedDataPageCount;
    }

    /** Data pages advanced past without decoding because they fell outside the surviving rows. */
    public int skippedDataPageCount() {
        return skippedDataPageCount;
    }

    /**
     * Returns {@code true} while the cursor has bytes remaining to decode. A {@code true} result does not guarantee
     * that the next call to {@link #nextDataPage} yields a data page; there may only be non-data pages left.
     */
    public boolean hasRemaining() {
        return position < limit;
    }

    /**
     * Reads the next {@link PageHeader} by wrapping the remaining chunk bytes as an {@link java.io.InputStream} and
     * advancing the cursor by exactly the number of header bytes consumed. Throws {@link ParquetFormatException} on
     * premature end-of-stream or Thrift decode failure.
     */
    private PageHeader readNextPageHeader() {
        MemorySegmentInputStream stream = new MemorySegmentInputStream(chunk, position, limit);
        try {
            PageHeader header = ParquetFormat.readPageHeader(stream);
            long consumed = stream.position() - position;
            if (consumed <= 0) {
                throw new MalformedFileException("Page header read advanced the cursor by " + consumed
                        + " bytes for column " + columnPath.dot());
            }
            position = stream.position();
            return header;
        } catch (ParquetFormatException e) {
            throw e.withContext("Failed to read page header for column " + columnPath.dot(), -1L, "PageHeader");
        }
    }

    /**
     * Returns a read-only zero-copy slice of the next {@code length} bytes of the cursor and advances past them. Throws
     * {@link MalformedFileException} when the cursor does not have that many bytes left.
     */
    private MemorySegment sliceAndAdvance(int length) {
        if (limit - position < length) {
            throw new MalformedFileException("Column " + columnPath.dot() + " page payload of " + length
                    + " bytes overruns chunk (remaining=" + (limit - position) + ")");
        }
        MemorySegment slice = chunk.asSlice(position, length).asReadOnly();
        position += length;
        return slice;
    }
}
