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
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.format.PageLocation;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
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
 * Plans and fetches one row group's projected column chunks as the exact byte ranges of its plan. The fetch asks for no
 * byte outside them, joins two of them only where they abut, reads them into one pooled buffer, and hands out zero-copy
 * {@link FetchedColumnChunk} views into it.
 */
public final class RowGroupFetcher {

    /**
     * Longest single read. A read's length is an int at the byte-source seam, and a run of abutting units longer than
     * this becomes more than one read.
     */
    private static final long MAX_MERGED_READ_BYTES = Integer.MAX_VALUE;

    /**
     * Ranges per batch call. A plan wider than this is issued as several calls, which bounds both the fan-out put in
     * flight by an implementation and the scratch memory held by one call.
     */
    private static final int MAX_REQUESTS_PER_CALL = 256;

    private final ByteRangeSource source;
    private final ParquetSchema fileSchema;
    private final ParquetSchema projectedSchema;
    private final SegmentPool pool;
    private final FetchBufferAllocator mandatoryAllocator;
    private final FetchAccumulator accumulator;

    public RowGroupFetcher(
            @NonNull ByteRangeSource source,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull SegmentPool pool,
            @NonNull FetchBufferAllocator mandatoryAllocator,
            @NonNull FetchAccumulator accumulator) {
        this.source = source;
        this.fileSchema = fileSchema;
        this.projectedSchema = projectedSchema;
        this.pool = pool;
        this.mandatoryAllocator = mandatoryAllocator;
        this.accumulator = accumulator;
    }

    /**
     * Builds the fetch plan for {@code survivor} without performing any I/O (used to size budget reservations).
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
            ChunkMeta meta = requireFetchableChunk(chunks, path);
            if (mask.isPresent()) {
                addUnitsFor(units, path, meta, mask.orElseThrow());
            } else {
                units.add(wholeChunkUnit(path, meta));
            }
        }
        return orderedPlan(units);
    }

    /**
     * The units ordered by file offset. That order lets the fetch lay them out back to back in one buffer and keep each
     * column's runs in page order.
     */
    private static FetchPlan orderedPlan(List<FetchUnit> units) {
        List<FetchUnit> ordered = new ArrayList<>(units);
        ordered.sort(Comparator.comparingLong(FetchUnit::fileOffset));
        return new FetchPlan(ordered);
    }

    private FetchUnit wholeChunkUnit(ColumnPath path, ChunkMeta meta) {
        return new FetchUnit(path, meta.chunkStart(), meta.totalCompressedSize(), 0, false);
    }

    /**
     * Emits the narrowed units for one column: the dictionary prefix {@code [chunkStart, firstDataPage)} when
     * non-empty, then one unit per surviving-page run. Falls back to the whole chunk when the mask lacks this column's
     * offset index, when the offset index locates any page outside the chunk's own byte extent, or when every page
     * survives (the degenerate case where the whole chunk IS the narrowed plan, trailing bytes included).
     */
    private void addUnitsFor(List<FetchUnit> units, ColumnPath path, ChunkMeta meta, RowMask mask) {
        OffsetIndex offsetIndex = mask.offsetIndexes().get(path);
        if (offsetIndex == null || offsetIndex.pageLocations().isEmpty()) {
            units.add(wholeChunkUnit(path, meta));
            return;
        }
        long start = meta.chunkStart();
        long chunkEnd = start + meta.totalCompressedSize();
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
     * Reads {@code plan}'s ranges into one pooled buffer and slices each projected column out of it. On any failure,
     * returns the buffer and releases {@code reservation} before propagating.
     *
     * <p>A speculative prefetch arrives with a real {@code reservation} that already reserved the plan's span against
     * the {@link FetchBudget}; its buffer comes straight from the {@link SegmentPool} to avoid reserving the same bytes
     * twice. A mandatory fetch arrives with {@link BudgetReservation#NONE} and routes the buffer through the
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
        boolean speculative = reservation != BudgetReservation.NONE;
        Pooled buffer = acquireRangeBuffer(plan.requestedBytes(), speculative);
        try {
            List<PlacedUnit> placed = placeUnits(plan, buffer.segment());
            readPlacedUnits(placed, buffer.segment());
            List<FetchedColumnChunk> columns = sliceColumns(byColumn(placed), survivor.chunks());
            long fetchNanos = wantsTimings ? System.nanoTime() - startNanos : 0L;
            return new RowGroupFetch(List.of(buffer), columns, reservation, fetchNanos);
        } catch (IOException | RuntimeException e) {
            closeQuietly(buffer);
            reservation.release();
            throw e;
        }
    }

    /**
     * A speculative prefetch already reserved its span and borrows RAM directly. A mandatory fetch takes RAM-or-mmap
     * through the valve.
     */
    private Pooled acquireRangeBuffer(long length, boolean speculative) {
        if (speculative) {
            return pool.borrow(length);
        }
        return mandatoryAllocator.acquireMandatory(length);
    }

    /** The plan's units laid out back to back in {@code buffer}, in the plan's file order. */
    private static List<PlacedUnit> placeUnits(FetchPlan plan, MemorySegment buffer) {
        List<PlacedUnit> placed = new ArrayList<>(plan.units().size());
        long bufferOffset = 0;
        for (FetchUnit unit : plan.units()) {
            placed.add(new PlacedUnit(unit, bufferOffset, buffer.asSlice(bufferOffset, unit.length())));
            bufferOffset += unit.length();
        }
        return placed;
    }

    /** One unit, where its bytes begin in the fetch buffer, and the view of the buffer holding them. */
    private record PlacedUnit(FetchUnit unit, long bufferOffset, MemorySegment bytes) {}

    /** One contiguous read: the file offset to read from and the stretch of the fetch buffer to fill. */
    private record MergedRead(long fileOffset, MemorySegment target) {

        int length() {
            return Math.toIntExact(target.byteSize());
        }
    }

    /** Issues the plan's reads as batch calls of at most {@link #MAX_REQUESTS_PER_CALL} ranges each. */
    private void readPlacedUnits(List<PlacedUnit> placed, MemorySegment buffer) {
        List<MergedRead> reads = mergeAbutting(placed, buffer);
        for (int from = 0; from < reads.size(); from += MAX_REQUESTS_PER_CALL) {
            int to = Math.min(from + MAX_REQUESTS_PER_CALL, reads.size());
            issue(reads.subList(from, to));
        }
    }

    private void issue(List<MergedRead> reads) {
        List<RangeRequest> requests = new ArrayList<>(reads.size());
        long bytes = 0;
        for (MergedRead read : reads) {
            int length = read.length();
            requests.add(
                    RangeRequest.of(read.fileOffset(), length, read.target().asByteBuffer()));
            bytes += length;
        }
        BatchReadResult result = source.readFully(requests);
        accumulator.add(
                FetchPurpose.PAGES,
                bytes,
                requests.size(),
                result.fetches(),
                result.bytesTransferred(),
                result.bytesFromCache());
    }

    /**
     * One read per maximal run of units whose file ranges touch byte for byte. Joining those costs no unrequested byte
     * and keeps a whole-chunk row group at a single read; a hole between two runs always ends a read.
     */
    private static List<MergedRead> mergeAbutting(List<PlacedUnit> placed, MemorySegment buffer) {
        List<MergedRead> reads = new ArrayList<>(placed.size());
        int runStart = 0;
        for (int index = 1; index <= placed.size(); index++) {
            if (index < placed.size() && continuesRun(placed, runStart, index)) {
                continue;
            }
            reads.add(readOver(placed.subList(runStart, index), buffer));
            runStart = index;
        }
        return reads;
    }

    private static boolean continuesRun(List<PlacedUnit> placed, int runStart, int index) {
        FetchUnit previous = placed.get(index - 1).unit();
        FetchUnit next = placed.get(index).unit();
        if (previous.fileOffset() + previous.length() != next.fileOffset()) {
            return false;
        }
        long runLength =
                next.fileOffset() + next.length() - placed.get(runStart).unit().fileOffset();
        return runLength <= MAX_MERGED_READ_BYTES;
    }

    private static MergedRead readOver(List<PlacedUnit> run, MemorySegment buffer) {
        PlacedUnit first = run.get(0);
        PlacedUnit last = run.get(run.size() - 1);
        long length = last.bufferOffset() + last.unit().length() - first.bufferOffset();
        return new MergedRead(first.unit().fileOffset(), buffer.asSlice(first.bufferOffset(), length));
    }

    /** The placed units of each column, in the plan's file order, which for data pages is page-ordinal order. */
    private static Map<ColumnPath, List<PlacedUnit>> byColumn(List<PlacedUnit> placed) {
        Map<ColumnPath, List<PlacedUnit>> byPath = new HashMap<>();
        for (PlacedUnit placedUnit : placed) {
            byPath.computeIfAbsent(placedUnit.unit().path(), _ -> new ArrayList<>())
                    .add(placedUnit);
        }
        return byPath;
    }

    private List<FetchedColumnChunk> sliceColumns(
            Map<ColumnPath, List<PlacedUnit>> placedByColumn, RowGroupChunks chunks) throws IOException {
        List<ColumnPath> leaves = projectedSchema.leafColumns();
        List<FetchedColumnChunk> columns = new ArrayList<>(leaves.size());
        for (ColumnPath path : leaves) {
            List<PlacedUnit> placed = requirePlacedUnits(placedByColumn, path);
            ChunkMeta meta = requireMeta(chunks, path);
            Optional<MemorySegment> dictionaryPrefix = dictionaryPrefixOf(placed);
            List<DataPageRun> runs = dataPageRunsOf(placed);
            columns.add(ColumnChunkSlicer.slice(dictionaryPrefix, runs, meta, path, fileSchema));
        }
        return columns;
    }

    /**
     * The placed units of {@code path}. A projected column with none means the plan and the projection disagree, which
     * would otherwise yield a chunk with no bytes and silently wrong rows.
     */
    private static List<PlacedUnit> requirePlacedUnits(
            Map<ColumnPath, List<PlacedUnit>> placedByColumn, ColumnPath path) {
        List<PlacedUnit> placed = placedByColumn.get(path);
        if (placed == null) {
            throw new IllegalStateException(
                    "Fetch plan has no bytes for projected column " + path.dot() + "; plan and projection disagree");
        }
        return placed;
    }

    private static Optional<MemorySegment> dictionaryPrefixOf(List<PlacedUnit> placed) {
        for (PlacedUnit placedUnit : placed) {
            if (placedUnit.unit().dictionaryPrefix()) {
                return Optional.of(placedUnit.bytes());
            }
        }
        return Optional.empty();
    }

    private static List<DataPageRun> dataPageRunsOf(List<PlacedUnit> placed) {
        List<DataPageRun> runs = new ArrayList<>(placed.size());
        for (PlacedUnit placedUnit : placed) {
            FetchUnit unit = placedUnit.unit();
            if (!unit.dictionaryPrefix()) {
                runs.add(new DataPageRun(placedUnit.bytes(), unit.firstPageOrdinal()));
            }
        }
        return runs;
    }

    private static void closeQuietly(Pooled buffer) {
        try {
            buffer.close();
        } catch (RuntimeException _) {
            // best-effort cleanup; the failure being handled is the one to report
        }
    }

    /**
     * The chunk of {@code path}, proven spannable by a fetch. The compact footer addresses a chunk by ordinal and keeps
     * no column names, which is why both failures below are raised here, where the column has one.
     */
    private static ChunkMeta requireFetchableChunk(RowGroupChunks chunks, ColumnPath path) {
        ChunkMeta meta = requireMeta(chunks, path);
        int rowGroupIndex = chunks.rowGroupIndex();
        if (meta.compressedSizeBeyondRange()) {
            throw new MalformedFileException(
                    describe(rowGroupIndex, path) + " has a totalCompressedSize beyond the addressable range");
        }
        int size = meta.totalCompressedSize();
        if (size <= 0) {
            throw new MalformedFileException(
                    describe(rowGroupIndex, path) + " has an unsupported totalCompressedSize " + size);
        }
        return meta;
    }

    private static ChunkMeta requireMeta(RowGroupChunks chunks, ColumnPath path) {
        return chunks.chunk(path)
                .orElseThrow(() -> new MalformedFileException(
                        "Row group " + chunks.rowGroupIndex() + " does not contain column " + path.dot()));
    }

    private static String describe(int rowGroupIndex, ColumnPath path) {
        return "Column chunk " + path.dot() + " in row group " + rowGroupIndex;
    }
}
