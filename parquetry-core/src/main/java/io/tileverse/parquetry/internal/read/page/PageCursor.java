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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.List;

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
 * <p>The chunk arrives as an ordered list of {@link DataPageRun}s - one run for a chunk fetched whole, several when the
 * fetch skipped the pages that cannot hold a surviving row. The cursor tracks a byte offset inside the current run's
 * {@link MemorySegment}, moves on to the next run when the current one is exhausted, and dispatches header reads
 * through {@link ParquetFormat#readPageHeader}. Non-data pages (e.g. a misplaced dictionary or index page) are skipped
 * silently; {@link #nextDataPage} returns {@code null} when the last run is exhausted.
 *
 * <p>Shared by every column reader that walks a chunk page by page.
 */
public final class PageCursor {

    private final List<DataPageRun> runs;
    private final ColumnPath columnPath;
    private final PageSelection selection; // null = no page-skip (decode every data page)
    private int runIndex = -1;
    private MemorySegment runBytes;
    private long runLimit;
    private long position;
    private int dataPageOrdinal;
    private long currentPageFirstRowIndex;
    private int decodedDataPageCount;
    private int skippedDataPageCount;

    public PageCursor(MemorySegment chunk, ColumnPath columnPath) {
        this(chunk, columnPath, null);
    }

    public PageCursor(MemorySegment chunk, ColumnPath columnPath, PageSelection selection) {
        this(List.of(new DataPageRun(chunk, 0)), columnPath, selection);
    }

    /**
     * Walks {@code runs} in order. Each run's first data page takes the run's base ordinal - the offset index ordinal
     * the fetch planner recorded - which keeps {@link PageSelection#isSurviving(int)} and
     * {@link PageSelection#firstRowIndex(int)} correct when the pages between runs were never fetched.
     */
    public PageCursor(List<DataPageRun> runs, ColumnPath columnPath, PageSelection selection) {
        // An empty run yields no page and would break the walk's has-more-bytes invariant.
        this.runs = runs.stream().filter(run -> run.segment().byteSize() > 0).toList();
        this.columnPath = columnPath;
        this.selection = selection;
        if (this.runs.size() > 1 && selection == null) {
            throw new IllegalArgumentException("A multi-run walk needs a PageSelection to resolve run base "
                    + "ordinals to row indexes, column " + columnPath.dot());
        }
    }

    /**
     * Returns the next {@link DecodedPage} from the chunk's compressed bytes, or {@code null} when the cursor is
     * exhausted. Non-data pages are skipped. When a {@link PageSelection} is present, data pages whose row span does
     * not overlap the surviving rows are advanced past without decompressing or decoding.
     */
    public DecodedPage nextDataPage(LevelMaxima maxLevels, Compression codec, Arena pageArena) throws IOException {
        while (position < runLimit || advanceRun()) {
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

    /**
     * Installs the next run as the walk's current bytes, restarting the byte position at the run's start and the page
     * ordinal at the run's base. Returns {@code false} once every run has been walked.
     */
    private boolean advanceRun() {
        if (runIndex + 1 >= runs.size()) {
            return false;
        }
        runIndex++;
        DataPageRun run = runs.get(runIndex);
        this.runBytes = run.segment();
        this.runLimit = runBytes.byteSize();
        this.position = 0L;
        this.dataPageOrdinal = run.firstPageOrdinal();
        return true;
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
        return position < runLimit || runIndex + 1 < runs.size();
    }

    /**
     * Reads the next {@link PageHeader} by wrapping the remaining bytes of the current run as an
     * {@link java.io.InputStream} and advancing the cursor by exactly the number of header bytes consumed. Throws
     * {@link ParquetFormatException} on premature end-of-stream or Thrift decode failure.
     */
    private PageHeader readNextPageHeader() {
        MemorySegmentInputStream stream = new MemorySegmentInputStream(runBytes, position, runLimit);
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
        if (runLimit - position < length) {
            throw new MalformedFileException("Column " + columnPath.dot() + " page payload of " + length
                    + " bytes overruns data page run " + runIndex + " (remaining=" + (runLimit - position) + ")");
        }
        MemorySegment slice = runBytes.asSlice(position, length).asReadOnly();
        position += length;
        return slice;
    }
}
