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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.format.ParquetFormatException;
import io.tileverse.parquetry.internal.read.page.DataPageRun;
import io.tileverse.parquetry.internal.read.page.PageRun;
import io.tileverse.parquetry.internal.read.page.PageSelection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.io.SegmentPool.Pooled;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.observe.FetchPurpose;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Plans and fetches one row group's projected column chunks as a small set of coalesced range reads. Replaces the
 * former per-column fetch: instead of one {@code readRange} per column chunk, it issues one read per coalesced range
 * and hands out zero-copy {@link FetchedColumnChunk} views into the range buffers.
 */
public final class RowGroupFetcher {

    private final ByteRangeSource source;
    private final ParquetSchema fileSchema;
    private final ParquetSchema projectedSchema;
    private final SegmentPool pool;
    private final FetchBufferAllocator mandatoryAllocator;
    private final int maxCoalesceGap;
    private final int maxCoalescedSpan;
    private final FetchAccumulator accumulator;

    // internal fetcher wiring: the eight parameters are cohesive collaborators of one row-group fetch
    @SuppressWarnings("java:S107")
    public RowGroupFetcher(
            @NonNull ByteRangeSource source,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull SegmentPool pool,
            @NonNull FetchBufferAllocator mandatoryAllocator,
            int maxCoalesceGap,
            int maxCoalescedSpan,
            @NonNull FetchAccumulator accumulator) {
        this.source = source;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.pool = pool;
        this.mandatoryAllocator = mandatoryAllocator;
        this.maxCoalesceGap = maxCoalesceGap;
        this.maxCoalescedSpan = maxCoalescedSpan;
        this.accumulator = accumulator;
    }

    /**
     * Builds the coalescing plan for {@code survivor} without performing any I/O (used to size budget reservations).
     *
     * <p>The plan's bytes are a superset of every page any reader will touch. Narrowing to the surviving pages happens
     * exactly when {@code mask} is present - the same mask the row group's column readers receive - and never from a
     * per-column condition the readers do not see. A reader's page selection is therefore always the mask's surviving
     * rows or narrower, and a chunk is never narrowed for a reader that would walk it without a selection.
     *
     * @param mask the row group's decode-time page-skip mask, or empty to plan whole column chunks
     */
    public FetchPlan planFor(RowGroupSurvivor survivor, Optional<RowMask> mask) {
        RowGroupChunks chunks = survivor.chunks();
        List<FetchUnit> units = new ArrayList<>();
        for (ColumnPath path : projectedSchema.leafColumns()) {
            ColumnMetaData meta = requireMeta(chunks, path);
            if (mask.isPresent()) {
                addUnitsFor(units, path, meta, mask.orElseThrow());
            } else {
                units.add(wholeChunkUnit(path, meta));
            }
        }
        return CoalescingFetchPlanner.plan(units, maxCoalesceGap, maxCoalescedSpan);
    }

    private FetchUnit wholeChunkUnit(ColumnPath path, ColumnMetaData meta) {
        return new FetchUnit(path, chunkStart(meta), chunkLength(meta, path), 0, false);
    }

    /**
     * Emits the narrowed units for one column: the dictionary prefix {@code [chunkStart, firstDataPage)} when
     * non-empty, then one unit per surviving-page run. Falls back to the whole chunk when the mask lacks this column's
     * offset index, when the offset index locates any page outside the chunk's own byte extent, or when every page
     * survives (the degenerate case where the whole chunk IS the narrowed plan, trailing bytes included).
     */
    private void addUnitsFor(List<FetchUnit> units, ColumnPath path, ColumnMetaData meta, RowMask mask) {
        OffsetIndex offsetIndex = mask.offsetIndexes().get(path);
        if (offsetIndex == null || offsetIndex.pageLocations().isEmpty()) {
            units.add(wholeChunkUnit(path, meta));
            return;
        }
        long start = chunkStart(meta);
        long chunkEnd = start + chunkLength(meta, path);
        if (locatesPagesOutsideChunk(offsetIndex, start, chunkEnd)) {
            units.add(wholeChunkUnit(path, meta));
            return;
        }
        PageSelection selection = PageSelection.forColumn(offsetIndex, meta.numValues(), mask.survivingRows());
        if (selection.survivingPageCount() == 0) {
            throw new IllegalStateException("No surviving page for column " + path.dot()
                    + " in a row group with surviving rows; the offset index and the row ranges disagree");
        }
        if (selection.survivingPageCount() == selection.pageCount()) {
            units.add(wholeChunkUnit(path, meta));
            return;
        }
        long firstDataPageOffset = offsetIndex.pageLocations().get(0).offset();
        long prefixLength = firstDataPageOffset - start;
        if (prefixLength > 0) {
            // the bound check above put the first data page inside an int-sized chunk, hence the prefix fits an int
            units.add(new FetchUnit(path, start, Math.toIntExact(prefixLength), 0, true));
        }
        for (PageRun run : PageRun.runsFor(selection, offsetIndex.pageLocations())) {
            units.add(new FetchUnit(path, run.fileOffset(), run.length(), run.firstPageOrdinal(), false));
        }
    }

    /**
     * Whether {@code offsetIndex} points at any byte outside {@code [chunkStart, chunkEnd)}. Such an index cannot be
     * trusted to locate this column's pages, and narrowing on it would aim the fetch at bytes the column does not own:
     * another column's pages parse cleanly and decode to plausible garbage. Widening back to the whole chunk keeps a
     * corrupt index's blast radius at wrong rows in this column, which is where it was before per-page fetching.
     */
    private static boolean locatesPagesOutsideChunk(OffsetIndex offsetIndex, long chunkStart, long chunkEnd) {
        for (PageLocation page : offsetIndex.pageLocations()) {
            if (page.offset() < chunkStart || page.offset() + page.compressedPageSize() > chunkEnd) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads {@code plan}'s coalesced ranges into pooled buffers and slices each projected column out of them. On any
     * failure, closes every buffer borrowed so far and releases {@code reservation} before propagating.
     *
     * <p>A speculative prefetch arrives with a real {@code reservation} that already reserved its whole span against
     * the {@link FetchBudget}; its range buffers come straight from the {@link SegmentPool} to avoid reserving the same
     * bytes twice. A mandatory fetch arrives with {@link BudgetReservation#NONE} and routes each range through the
     * {@link FetchBufferAllocator} valve, which reserves RAM when the budget has room and maps a spill file otherwise.
     */
    public RowGroupFetch fetch(RowGroupSurvivor survivor, FetchPlan plan, BudgetReservation reservation)
            throws IOException {
        return fetch(survivor, plan, reservation, false);
    }

    /**
     * Same as {@link #fetch(RowGroupSurvivor, FetchPlan, BudgetReservation)}, additionally timing the fetch when
     * {@code wantsTimings} is on: the elapsed nanoseconds land on the returned {@link RowGroupFetch#fetchNanos()}. When
     * off, no clock is read and the fetch reports zero.
     */
    public RowGroupFetch fetch(
            RowGroupSurvivor survivor, FetchPlan plan, BudgetReservation reservation, boolean wantsTimings)
            throws IOException {
        long startNanos = wantsTimings ? System.nanoTime() : 0L;
        RowGroupChunks chunks = survivor.chunks();
        boolean speculative = reservation != BudgetReservation.NONE;
        List<Pooled> buffers = new ArrayList<>(plan.ranges().size());
        List<MemorySegment> rangeSegments = new ArrayList<>(plan.ranges().size());
        try {
            for (CoalescedRange range : plan.ranges()) {
                Pooled pooled = acquireRangeBuffer(range.length(), speculative);
                buffers.add(pooled);
                MemorySegment rangeSegment = pooled.segment();
                int read = source.read(range.fileOffset(), rangeSegment);
                if (read != range.length()) {
                    throw new IOException("Short read for coalesced range at offset " + range.fileOffset()
                            + ": expected " + range.length() + " bytes, got " + read);
                }
                accumulator.add(FetchPurpose.PAGES, read);
                rangeSegments.add(rangeSegment);
            }
            List<FetchedColumnChunk> columns = sliceColumns(plan, chunks, rangeSegments);
            long fetchNanos = wantsTimings ? System.nanoTime() - startNanos : 0L;
            return new RowGroupFetch(buffers, columns, reservation, fetchNanos);
        } catch (IOException | RuntimeException e) {
            for (Pooled buffer : buffers) {
                try {
                    buffer.close();
                } catch (RuntimeException _) {
                    // best-effort cleanup; rethrow the original failure
                }
            }
            reservation.release();
            throw e;
        }
    }

    /**
     * A speculative prefetch already reserved its span; it borrows RAM directly. A mandatory fetch reserves RAM-or-mmap
     * per range through the valve, retiring the former unconditional-RAM allocation.
     */
    private Pooled acquireRangeBuffer(int length, boolean speculative) {
        if (speculative) {
            return pool.borrow(length);
        }
        return mandatoryAllocator.acquireMandatory(length);
    }

    private List<FetchedColumnChunk> sliceColumns(
            FetchPlan plan, RowGroupChunks chunks, List<MemorySegment> rangeSegments) throws IOException {
        List<FetchedColumnChunk> columns = new ArrayList<>(plan.slices().size());
        for (ColumnPath path : projectedSchema.leafColumns()) {
            ColumnSlices slices = requireSlices(plan, path);
            ColumnMetaData meta = requireMeta(chunks, path);
            Optional<MemorySegment> dictionaryPrefix =
                    slices.dictionaryPrefix().map(slice -> segmentFor(slice, rangeSegments));
            List<DataPageRun> runs = dataPageRuns(slices, rangeSegments);
            columns.add(ColumnChunkSlicer.slice(dictionaryPrefix, runs, meta, path, fileSchema));
        }
        return columns;
    }

    /**
     * The slices {@code plan} recorded for {@code path}. A projected column absent from the plan means the plan and the
     * projection disagree, which would otherwise yield a chunk with no bytes and silently wrong rows.
     */
    private static ColumnSlices requireSlices(FetchPlan plan, ColumnPath path) {
        ColumnSlices slices = plan.slices().get(path);
        if (slices == null) {
            throw new IllegalStateException(
                    "Fetch plan has no slices for projected column " + path.dot() + "; plan and projection disagree");
        }
        return slices;
    }

    private static List<DataPageRun> dataPageRuns(ColumnSlices slices, List<MemorySegment> rangeSegments) {
        List<DataPageRun> runs = new ArrayList<>(slices.runs().size());
        for (RunSlice run : slices.runs()) {
            MemorySegment segment = rangeSegments.get(run.rangeIndex()).asSlice(run.offsetWithinRange(), run.length());
            runs.add(new DataPageRun(segment, run.firstPageOrdinal()));
        }
        return runs;
    }

    private static MemorySegment segmentFor(ColumnSlice slice, List<MemorySegment> rangeSegments) {
        return rangeSegments.get(slice.rangeIndex()).asSlice(slice.offsetWithinRange(), slice.length());
    }

    private ColumnMetaData requireMeta(RowGroupChunks chunks, ColumnPath path) {
        ColumnChunk chunk = chunks.chunk(path)
                .orElseThrow(() -> new IllegalStateException("Row group does not contain column " + path.dot()));
        return chunk.metaData()
                .orElseThrow(() ->
                        new ParquetFormatException("ColumnChunk for " + path.dot() + " is missing inline metaData"));
    }

    /**
     * The byte offset where the column chunk begins. A {@code dictionary_page_offset} only points at a real dictionary
     * page when it is positive and precedes the first data page; some writers leave it unset or store a literal
     * {@code 0} (which would otherwise point at the file's magic header). The chunk then starts at
     * {@code data_page_offset}. This mirrors parquet-mr's {@code getStartingPos}.
     */
    private static long chunkStart(ColumnMetaData meta) {
        long dataPageOffset = meta.dataPageOffset();
        long dictionaryPageOffset = meta.dictionaryPageOffset().orElse(0L);
        boolean dictionaryPagePrecedesData = dictionaryPageOffset > 0 && dictionaryPageOffset < dataPageOffset;
        return dictionaryPagePrecedesData ? dictionaryPageOffset : dataPageOffset;
    }

    private static int chunkLength(ColumnMetaData meta, ColumnPath path) {
        long size = meta.totalCompressedSize();
        if (size <= 0 || size > Integer.MAX_VALUE) {
            throw new MalformedFileException(
                    "Column chunk for " + path.dot() + " has an unsupported totalCompressedSize " + size);
        }
        return (int) size;
    }
}
