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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import io.tileverse.storage.BatchReadResult;
import io.tileverse.storage.RangeRequest;

import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.footer.ChunkMeta;
import io.tileverse.parquetry.internal.read.PageIndexLearner.WholeChunk;
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
 * byte outside the plan's ranges and asks for each of them as its own range. It reads them into one pooled buffer, and
 * hands out zero-copy {@link FetchedColumnChunk} views into it. A fetch planned without a mask reads whole chunks. It
 * hands them to the {@link PageIndexLearner} of the read when the learner may learn their row group, before it returns
 * and while the buffer still holds them.
 *
 * <p>A narrowed fetch leaves the dictionary page of a column out when each fetched page is known not to use it; see
 * {@link DictionaryFetch}. A prefix read on demand keeps its room in the buffer, and the fetch checks the headers of
 * the fetched pages of its column. A dictionary-encoded page takes one more read into that room. The fetch never asks
 * for a second fetch buffer.
 */
public final class RowGroupFetcher {

    private final ByteRangeSource source;
    private final ParquetSchema projectedSchema;
    private final SegmentPool pool;
    private final FetchBufferAllocator mandatoryAllocator;
    private final FetchAccumulator accumulator;
    private final PageIndexLearner learner;
    private final ColumnChunkSlicer slicer;

    /** @param learner the learner of the read, receiving the row groups fetched whole when it may learn them */
    public RowGroupFetcher(
            @NonNull ByteRangeSource source,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema projectedSchema,
            @NonNull SegmentPool pool,
            @NonNull FetchBufferAllocator mandatoryAllocator,
            @NonNull FetchAccumulator accumulator,
            @NonNull PageIndexLearner learner) {
        this.source = source;
        this.projectedSchema = projectedSchema;
        this.pool = pool;
        this.mandatoryAllocator = mandatoryAllocator;
        this.accumulator = accumulator;
        this.learner = learner;
        this.slicer = new ColumnChunkSlicer(fileSchema, pool);
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
        Set<ColumnPath> dictionariesLeftOut = new HashSet<>();
        for (ColumnPath path : projectedSchema.leafColumns()) {
            ChunkMeta meta = requireFetchableChunk(chunks, path, source.size());
            if (mask.isPresent()) {
                addUnitsFor(units, dictionariesLeftOut, path, meta, mask.orElseThrow());
            } else {
                units.add(wholeChunkUnit(path, meta));
            }
        }
        return orderedPlan(units, mask.isEmpty(), dictionariesLeftOut);
    }

    /**
     * The units ordered by file offset. That order lets the fetch lay them out back to back in one buffer and keep each
     * column's runs in page order.
     */
    private static FetchPlan orderedPlan(List<FetchUnit> units, boolean unmasked, Set<ColumnPath> dictionariesLeftOut) {
        List<FetchUnit> ordered = new ArrayList<>(units);
        ordered.sort(Comparator.comparingLong(FetchUnit::fileOffset));
        return new FetchPlan(ordered, unmasked, dictionariesLeftOut);
    }

    private FetchUnit wholeChunkUnit(ColumnPath path, ChunkMeta meta) {
        return new FetchUnit(path, meta.chunkStart(), meta.totalCompressedSize(), 0, false, false);
    }

    /**
     * Emits the narrowed units for one column: the dictionary prefix {@code [chunkStart, firstDataPage)} when non-empty
     * and a surviving page may use it, then one unit per surviving-page run. A prefix left out names its column in
     * {@code dictionariesLeftOut}. Falls back to the whole chunk when the mask lacks the page index of this column, or
     * when each page survives (the degenerate case where the whole chunk IS the narrowed plan, trailing bytes
     * included). A page index places its pages inside the bytes of the chunk; see {@link PageIndex#declared}.
     */
    private void addUnitsFor(
            List<FetchUnit> units, Set<ColumnPath> dictionariesLeftOut, ColumnPath path, ChunkMeta meta, RowMask mask) {
        PageIndex pages = mask.pageIndexes().get(path);
        if (pages == null || pages.pageCount() == 0) {
            units.add(wholeChunkUnit(path, meta));
            return;
        }
        long start = meta.chunkStart();
        PageSelection selection = PageSelection.forColumn(pages, mask.survivingRows());
        if (selection.survivingPageCount() == 0) {
            throw new IllegalStateException("No surviving page for column " + path.dot()
                    + " in a row group with surviving rows; the page index and the row ranges disagree");
        }
        if (selection.survivingPageCount() == selection.pageCount()) {
            units.add(wholeChunkUnit(path, meta));
            return;
        }
        long firstDataPageOffset = pages.locations().get(0).offset();
        long prefixLength = firstDataPageOffset - start;
        if (prefixLength > 0) {
            DictionaryFetch dictionary = DictionaryFetch.of(pages, selection, meta);
            if (dictionary == DictionaryFetch.LEFT_OUT) {
                dictionariesLeftOut.add(path);
            } else {
                // The page index places the first data page inside the chunk, and the chunk length is an int.
                int length = Math.toIntExact(prefixLength);
                units.add(new FetchUnit(path, start, length, 0, true, dictionary == DictionaryFetch.ON_DEMAND));
            }
        }
        for (PageRun run : PageRun.runsFor(selection, pages.locations())) {
            units.add(new FetchUnit(path, run.fileOffset(), run.length(), run.firstPageOrdinal(), false, false));
        }
    }

    /**
     * Reads {@code plan}'s ranges into one pooled buffer and slices each projected column out of it. An on-demand
     * prefix is read only when a fetched page of its column uses the dictionary. On any failure, returns the buffer and
     * releases {@code reservation} before propagating.
     *
     * <p>A speculative prefetch arrives with a real {@code reservation} that already reserved the plan's span against
     * the {@link FetchBudget}; its buffer comes straight from the {@link SegmentPool} to avoid reserving the same bytes
     * twice. A mandatory fetch arrives with {@link BudgetReservation#NONE} and routes the buffer through the
     * {@link FetchBufferAllocator} valve, which reserves RAM when the budget has room and maps a spill file otherwise.
     */
    public RowGroupFetch fetch(RowGroupSurvivor survivor, FetchPlan plan, BudgetReservation reservation) {
        return fetch(survivor, plan, reservation, false);
    }

    /**
     * Same as {@link #fetch(RowGroupSurvivor, FetchPlan, BudgetReservation)}, additionally timing the fetch when
     * {@code wantsTimings} is on: the elapsed nanoseconds land on the returned {@link RowGroupFetch#fetchNanos()}. When
     * off, no clock is read and the fetch reports zero.
     */
    public RowGroupFetch fetch(
            RowGroupSurvivor survivor, FetchPlan plan, BudgetReservation reservation, boolean wantsTimings) {
        long startNanos = wantsTimings ? System.nanoTime() : 0L;
        boolean speculative = reservation != BudgetReservation.NONE;
        Pooled buffer = acquireRangeBuffer(plan.requestedBytes(), speculative);
        try {
            List<PlacedUnit> placed = placeUnits(plan, buffer.segment());
            readPlacedUnits(unitsReadAtOnce(placed));
            Map<ColumnPath, List<PlacedUnit>> placedByColumn = byColumn(placed);
            Set<ColumnPath> dictionariesLeftOut = settleOnDemandPrefixes(plan, placed, placedByColumn);
            List<FetchedColumnChunk> columns = sliceColumns(placedByColumn, survivor.chunks(), dictionariesLeftOut);
            long fetchNanos = wantsTimings ? System.nanoTime() - startNanos : 0L;
            learnFrom(survivor, plan, placed);
            return new RowGroupFetch(List.of(buffer), columns, reservation, fetchNanos);
        } catch (RuntimeException e) {
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
            placed.add(new PlacedUnit(unit, buffer.asSlice(bufferOffset, unit.length())));
            bufferOffset += unit.length();
        }
        return placed;
    }

    /** The units of the first read. An on-demand prefix stays out. */
    private static List<PlacedUnit> unitsReadAtOnce(List<PlacedUnit> placed) {
        List<PlacedUnit> readAtOnce = new ArrayList<>(placed.size());
        for (PlacedUnit placedUnit : placed) {
            if (!placedUnit.unit().onDemand()) {
                readAtOnce.add(placedUnit);
            }
        }
        return readAtOnce;
    }

    /** One unit and the view of the fetch buffer holding its bytes. */
    private record PlacedUnit(FetchUnit unit, MemorySegment bytes) {}

    /** Asks for the units of {@code placed} in one call to the byte source, each as its own range. */
    private void readPlacedUnits(List<PlacedUnit> placed) {
        // A settle pass with no prefix in use must not reach the source or count a call in the fetch statistics.
        if (placed.isEmpty()) {
            return;
        }
        List<RangeRequest> requests = new ArrayList<>(placed.size());
        for (PlacedUnit placedUnit : placed) {
            FetchUnit unit = placedUnit.unit();
            ByteBuffer target = placedUnit.bytes().asByteBuffer();
            requests.add(RangeRequest.of(unit.fileOffset(), unit.length(), target));
        }
        issue(requests);
    }

    private void issue(List<RangeRequest> requests) {
        long bytes = 0;
        for (RangeRequest request : requests) {
            bytes += request.range().length();
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

    /** The placed units of each column, in the plan's file order, which for data pages is page-ordinal order. */
    private static Map<ColumnPath, List<PlacedUnit>> byColumn(List<PlacedUnit> placed) {
        Map<ColumnPath, List<PlacedUnit>> byPath = new HashMap<>();
        for (PlacedUnit placedUnit : placed) {
            byPath.computeIfAbsent(placedUnit.unit().path(), _ -> new ArrayList<>())
                    .add(placedUnit);
        }
        return byPath;
    }

    /**
     * Reads the on-demand prefixes used by a fetched page, and returns the columns left without dictionary bytes: those
     * named by {@code plan} and those with a prefix left unread. The room of an unread prefix holds earlier bytes of
     * the pool, and this result is what keeps the slicer away from them. The prefixes share one more read of the same
     * buffer, because a fetch holding a fetch buffer never asks for another.
     */
    private Set<ColumnPath> settleOnDemandPrefixes(
            FetchPlan plan, List<PlacedUnit> placed, Map<ColumnPath, List<PlacedUnit>> placedByColumn) {
        Set<ColumnPath> leftOut = new HashSet<>(plan.dictionariesLeftOut());
        List<PlacedUnit> inUse = new ArrayList<>();
        for (PlacedUnit placedUnit : placed) {
            ColumnPath path = placedUnit.unit().path();
            if (placedUnit.unit().onDemand()) {
                if (aFetchedPageUsesTheDictionary(placedByColumn.get(path), path)) {
                    inUse.add(placedUnit);
                } else {
                    leftOut.add(path);
                }
            }
        }
        readPlacedUnits(inUse);
        return leftOut;
    }

    private static boolean aFetchedPageUsesTheDictionary(List<PlacedUnit> ofColumn, ColumnPath path) {
        for (PlacedUnit placedUnit : ofColumn) {
            if (!placedUnit.unit().dictionaryPrefix() && DictionaryFetch.runUsesDictionary(placedUnit.bytes(), path)) {
                return true;
            }
        }
        return false;
    }

    private List<FetchedColumnChunk> sliceColumns(
            Map<ColumnPath, List<PlacedUnit>> placedByColumn,
            RowGroupChunks chunks,
            Set<ColumnPath> dictionariesLeftOut) {
        List<ColumnPath> leaves = projectedSchema.leafColumns();
        List<FetchedColumnChunk> columns = new ArrayList<>(leaves.size());
        for (ColumnPath path : leaves) {
            List<PlacedUnit> placed = requirePlacedUnits(placedByColumn, path);
            ChunkMeta meta = requireMeta(chunks, path);
            DictionarySource dictionary = dictionarySourceOf(placed, dictionariesLeftOut.contains(path));
            List<DataPageRun> runs = dataPageRunsOf(placed);
            columns.add(slicer.slice(dictionary, runs, meta, path));
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

    private static DictionarySource dictionarySourceOf(List<PlacedUnit> placed, boolean leftOut) {
        if (leftOut) {
            return new DictionarySource.LeftOut();
        }
        for (PlacedUnit placedUnit : placed) {
            if (placedUnit.unit().dictionaryPrefix()) {
                return new DictionarySource.Prefix(placedUnit.bytes());
            }
        }
        return new DictionarySource.FirstRun();
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

    /**
     * Hands the chunks of {@code survivor} to the learner, when the plan was built without a mask and the learner may
     * learn the row group. Each unit of such a plan is one chunk, read from its first byte. The fetch reports its time
     * without this pass.
     */
    private void learnFrom(RowGroupSurvivor survivor, FetchPlan plan, List<PlacedUnit> placed) {
        if (!plan.unmasked() || !learner.learns(survivor.index())) {
            return;
        }
        learner.learn(survivor, wholeChunksOf(placed, survivor.chunks()));
    }

    private static List<WholeChunk> wholeChunksOf(List<PlacedUnit> placed, RowGroupChunks chunks) {
        List<WholeChunk> wholeChunks = new ArrayList<>(placed.size());
        for (PlacedUnit placedUnit : placed) {
            ColumnPath path = placedUnit.unit().path();
            wholeChunks.add(new WholeChunk(path, requireMeta(chunks, path), placedUnit.bytes()));
        }
        return wholeChunks;
    }

    private static void closeQuietly(Pooled buffer) {
        try {
            buffer.close();
        } catch (RuntimeException _) {
            // best-effort cleanup; the failure being handled is the one to report
        }
    }

    /**
     * The chunk of {@code path}, proven spannable by a fetch from a source of {@code sourceSize} bytes. The compact
     * footer addresses a chunk by ordinal and keeps no column names, which is why the failures below are raised here,
     * where the column has one.
     */
    private static ChunkMeta requireFetchableChunk(RowGroupChunks chunks, ColumnPath path, long sourceSize) {
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
        requireChunkInsideSource(meta, describe(rowGroupIndex, path), sourceSize);
        return meta;
    }

    /**
     * A footer placing a chunk's bytes before the start of the file or past its end describes other bytes. Fetching
     * such a chunk would fail as a short read, or hand the decoders bytes of no page. The check compares the size with
     * the bytes left after the start, because a corrupt start near {@code Long.MAX_VALUE} would overflow the end
     * offset.
     */
    private static void requireChunkInsideSource(ChunkMeta meta, String chunkDescription, long sourceSize) {
        long start = meta.chunkStart();
        int size = meta.totalCompressedSize();
        if (start < 0 || start > sourceSize || size > sourceSize - start) {
            throw new MalformedFileException(chunkDescription + " spans " + size + " bytes from offset " + start
                    + ", outside the " + sourceSize + "-byte file");
        }
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
