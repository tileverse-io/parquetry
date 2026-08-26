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

import static io.tileverse.parquetry.format.ParquetLayouts.DOUBLE;
import static io.tileverse.parquetry.format.ParquetLayouts.FLOAT;
import static io.tileverse.parquetry.format.ParquetLayouts.INT32;
import static io.tileverse.parquetry.format.ParquetLayouts.INT64;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.BooleanVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DoubleVector;
import io.tileverse.parquetry.columnar.FixedLenBinaryVector;
import io.tileverse.parquetry.columnar.FloatVector;
import io.tileverse.parquetry.columnar.Int96Vector;
import io.tileverse.parquetry.columnar.IntSequence;
import io.tileverse.parquetry.columnar.IntVector;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.LongVector;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.read.page.DecodedPage;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;
import io.tileverse.parquetry.internal.read.page.PageCursor;
import io.tileverse.parquetry.internal.read.page.PageDecoder;
import io.tileverse.parquetry.internal.read.page.PageSelection;
import io.tileverse.parquetry.internal.read.page.RleDictionaryPageDecoder;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.LevelMaxima;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;

import lombok.NonNull;

/**
 * Per-column reader for the batch API. Each call to {@link #readBatch} returns a {@link ColumnVector} sliced from the
 * current page's pre-decoded values.
 *
 * <p>Page lifecycle: {@link #loadNextPage()} decompresses the next page into a confined {@link Arena}, fully decodes
 * rep-levels / def-levels into reader-owned off-heap scratch buffers (the {@link Levels} views over them stay valid
 * until the next page decode), and decodes values into one of three backings: row-indexed positions over the still-open
 * page for contiguous PLAIN / DELTA_LENGTH binary and PLAIN all-valid INT32 / INT64 / FLOAT / DOUBLE; a pooled
 * page-lifetime segment for the remaining unmasked fixed-width pages and for unmasked dictionary-binary indexes; or
 * heap arrays for masked reads and the other kinds. For the heap and pooled-segment backings the Arena is closed before
 * {@link #loadNextPage()} returns; for the live-page kinds the Arena is kept open across the page's batches and closed
 * at page-advance, because the per-batch slices copy their bytes straight from the page. Each {@link #readBatch} call
 * then slices the current page; within-page splitting is correct because the page state (level scratches, heap arrays,
 * the pooled value and metadata segments, or the live page plus its offsets) outlives every slice of the page.
 *
 * <p>{@link ValueDecode} chooses when a page's values materialize. {@code EAGER} and {@code PAGE_MASK} decode at page
 * load, as above. {@code WINDOWED_MASK} loads a page's levels and validity only and keeps the page's value decoder
 * open; each {@link #readMaskedWindow} then decodes just the value slots its surviving rows cover, and the page Arena
 * stays open until the page's last window. A windowed reader built with a row mask serves the mask's surviving rows as
 * its row space - the same rows, in the same order, that the eager masked lane serves - and resolves each of them to
 * the raw page slot it occupies before the decoder walk.
 */
final class BatchColumnReader {

    private static final Levels EMPTY_LEVELS = Levels.of(new int[0]);
    private static final int INT96_WIDTH = 12;

    private final ColumnPath columnPath;
    private final SchemaNode.Primitive leaf;
    private final LevelMaxima maxLevels;
    private final Compression codec;
    private final long totalValues;
    private final PageCursor pageCursor;
    private final int defBitWidth;
    private final int repBitWidth;
    private final Optional<Dictionary<?>> dictionary;
    private final RowRanges survivingRows; // null when this column is not masked
    private final ValueDecode valueDecode;
    private final DecodeBufferAllocator decodeBufferAllocator;
    private final LevelScratch repLevelScratch;
    private final LevelScratch defLevelScratch;
    private final IntScratch densePositionsScratch = new IntScratch();
    private final IntScratch denseLengthsScratch = new IntScratch();

    // Per-page state. Populated by loadNextPage; cleared on advance.
    private boolean pageLoaded;
    private int pageSize;
    private int pageLogicalRowCount;
    // The page's per-row null mask, whichever representation Validity collapsed it to: all-valid (no bitmap), a heap
    // BitSet, or - on the flat optional fast path - the pooled off-heap bitmap decoded straight from the def-level
    // stream. The pooled buffer behind that last representation is pageValidityPooled, held for the page's lifetime
    // and released at page-advance; it is non-null exactly when pageValidity reads the off-heap bitmap. Null in the
    // windowed lane, which moves the page's mask to pageSlotValidity when it opens the page.
    private Validity pageValidity;
    private SegmentPool.Pooled pageValidityPooled;
    private Levels pageRepLevels; // null when maxRep == 0
    private Levels pageDefLevels; // null when maxDef == 0, or when the origin fast path consumed the stream
    private int[] pageInts;
    private long[] pageLongs;
    private float[] pageFloats;
    private double[] pageDoubles;
    // The page's fixed-width value segment in the column's little-endian layout, when the page decoded off-heap:
    // either the live page value segment (PLAIN all-valid; pageBackingIsLivePage is set and the page Arena stays open
    // across the batches) or a pooled page-lifetime buffer (pageValuesPooled non-null, released at page advance)
    // holding row-positioned values with null slots zeroed. Null when the page decoded into a heap array (masked
    // reads) or the column is not fixed-width.
    private MemorySegment pageValues;
    private SegmentPool.Pooled pageValuesPooled;
    private boolean[] pageBooleans;

    // Scratch holder for PLAIN/DELTA binary between value decode and the freeze step that consolidates it into the
    // shared backing. Never populated for dictionary-encoded binary, which decodes into pageIndices instead.
    private MemorySegment[] pageSegments;
    // PLAIN/DELTA binary is frozen into one shared heap backing; offsets are meaningful only for the variable kind.
    private MemorySegment pageBinaryBacking;
    // Row-indexed cumulative value-byte offsets (size pageSize + 1), every slot written. The backing depends on the
    // lane: a window of the pooled page-metadata segment for the direct path, a heap array from the freeze lane.
    private IntSequence pageBinaryOffsets;
    private boolean pageWasDictionary;

    // Per-row source positions for the direct off-heap binary path: pageValuePos.get(row) is the absolute byte offset
    // of the row's value bytes within the live page value segment (pageBinaryBacking). Null rows are never read and
    // may hold stale bytes from the pooled segment.
    private IntSequence pageValuePos;
    // The pooled page-lifetime segment behind the direct lane's pageValuePos and pageBinaryOffsets windows, or behind
    // an unmasked dictionary binary page's pageIndices (the two uses never coexist: dictionary and direct-binary pages
    // are mutually exclusive per page); released at page-advance. Null when the page produced heap-backed sequences
    // (freeze and masked lanes) or is not binary.
    private SegmentPool.Pooled pageBinaryMetaPooled;
    // True while a page payload - the binary backing or the fixed-width live value segment - points into a still-open
    // page Arena the slices read from directly. The Arena is then kept alive across the page's batches and closed at
    // page-advance.
    private boolean pageBackingIsLivePage;
    // The page Arena kept alive across the page's batches when pageBackingIsLivePage; closed at advance / reader close.
    private Arena livePageArena;

    // One row-positioned dictionary index per row for dictionary-encoded binary, resolved through the chunk-level
    // dictEntries array. A null row holds the harmless placeholder index zero and its nullness lives in pageValidity.
    // Unmasked pages back this with a window of the pooled page-metadata segment (pageBinaryMetaPooled, released at
    // page-advance); masked reads keep a heap array behind the same sequence.
    private IntSequence pageIndices;
    // Heap index slots for the masked skip-decode lane, filled one kept row at a time and wrapped into pageIndices at
    // the end of decodeSelectedRows; null outside that window.
    private int[] maskedIndices;
    // Chunk-level dictionary entries (shared heap-owned segments), built once from this.dictionary and reused across
    // pages.
    private MemorySegment[] dictEntries;
    // Total byte size of dictEntries, summed as the entries are built and handed to every vector over them.
    private long dictEntryBytes;

    // The page's decoder, held open across the page's windows in WINDOWED_MASK mode: value decode is forward-only, and
    // consecutive windows resume where the previous one stopped rather than re-reading the page.
    private PageDecoder<?> windowDecoder;
    // The page value slot the window decoder's cursor stands at: every non-null slot below it has been decoded or
    // skipped. Slots between the last kept slot of a window and the next window's start are skipped lazily, when the
    // next window's walk reaches them.
    private int windowDecoderSlot;
    // The page's whole per-slot null mask, read by each window to place its decoder skips. A window's own mask is
    // built per window and belongs to that window's vector.
    private Validity pageSlotValidity;
    // The page's value slots as written, which the window walk steps over. Distinct from pageSize once a row mask has
    // narrowed the reader's row space to the page's surviving rows.
    private int pageRawSlotCount;
    // The raw page slot of each of the page's mask-surviving rows, ascending: row r of the windowed masked reader's
    // row space sits at page slot pageSurvivingSlots[r]. Null when the reader has no row mask.
    private int[] pageSurvivingSlots;

    private int valuesConsumedInCurrentPage;
    private int logicalRowsConsumedInCurrentPage;
    private long valuesConsumedTotal;
    // Rows consumed out of a row mask's surviving rows, the unit hasMore() compares against RowRanges.totalRows() to
    // decide the chunk is drained. Stays zero for an unmasked reader, which ends on the page cursor instead.
    private long survivingRowsConsumedTotal;
    private long decodedValueCount;

    BatchColumnReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf) {
        this(decodeBufferAllocator, chunk, leaf, null, null, ValueDecode.EAGER);
    }

    BatchColumnReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf,
            RowRanges survivingRows,
            OffsetIndex offsetIndex) {
        this(decodeBufferAllocator, chunk, leaf, survivingRows, offsetIndex, ValueDecode.EAGER);
    }

    BatchColumnReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf,
            RowRanges survivingRows,
            OffsetIndex offsetIndex,
            @NonNull ValueDecode valueDecode) {
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.repLevelScratch = new LevelScratch(decodeBufferAllocator);
        this.defLevelScratch = new LevelScratch(decodeBufferAllocator);
        this.valueDecode = valueDecode;
        this.columnPath = chunk.path();
        this.leaf = leaf;
        this.maxLevels = new LevelMaxima(chunk.maxRepetitionLevel(), chunk.maxDefinitionLevel());
        this.codec = Compression.forWireCodec(chunk.metadata().codec());
        this.totalValues = chunk.metadata().numValues();
        this.defBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxDefinitionLevel());
        this.repBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxRepetitionLevel());
        this.dictionary = chunk.dictionary();
        if (survivingRows != null) {
            requireOffsetIndexForRowMask(chunk, offsetIndex);
            requireFlatColumnForWindowedMask(chunk, valueDecode);
            this.survivingRows = survivingRows;
            PageSelection selection = PageSelection.forColumn(offsetIndex, totalValues, survivingRows);
            this.pageCursor = new PageCursor(chunk.dataPageRuns(), columnPath, selection);
        } else {
            this.survivingRows = null;
            this.pageCursor = new PageCursor(chunk.dataPageRuns(), columnPath, null);
        }
    }

    /**
     * Fails a masked read of a column with no offset index. A row mask states row-group row indexes, and every masked
     * lane places them in the column's pages through the offset index; without one the reader has no way to tell which
     * of a page's values the mask keeps.
     */
    private static void requireOffsetIndexForRowMask(FetchedColumnChunk chunk, OffsetIndex offsetIndex) {
        if (offsetIndex == null) {
            throw new IllegalArgumentException("Column " + chunk.path().dot()
                    + " was given a row mask without an offset index; a row mask requires the column's offset index");
        }
    }

    /**
     * Fails a windowed masked read of a repeated column. A row mask indexes top-level rows, and both masked lanes
     * resolve a surviving row to exactly one value slot, which only a flat column has.
     */
    private static void requireFlatColumnForWindowedMask(FetchedColumnChunk chunk, ValueDecode valueDecode) {
        if (valueDecode == ValueDecode.WINDOWED_MASK && chunk.maxRepetitionLevel() > 0) {
            throw new IllegalArgumentException("A row mask over the windowed value decode is only defined for flat "
                    + "columns; column " + chunk.path().dot() + " is repeated");
        }
    }

    // ---- public iteration API ----

    /** True while the current page has unconsumed values or the page cursor has more pages. */
    boolean hasMore() {
        if (pageLoaded && valuesConsumedInCurrentPage < pageSize) {
            return true;
        }
        if (survivingRows != null) {
            return survivingRowsConsumedTotal < survivingRows.totalRows();
        }
        return pageCursor.hasRemaining();
    }

    /** Leaf-value count remaining in the current page. Loads the page on first call. */
    int rowsRemainingInCurrentPage() {
        ensurePageLoaded();
        return pageSize - valuesConsumedInCurrentPage;
    }

    /**
     * Logical (top-level) row count remaining in the current page that is safe to serve from this page alone. For flat
     * columns this equals {@link #rowsRemainingInCurrentPage()}; for repeated columns it counts rep=0 markers ahead of
     * the consume cursor, minus the page's last-started row while that row may continue into the next page.
     *
     * <p>The subtraction is what keeps a batch from ending mid-row: a row is credited to the page holding its rep=0
     * start, but a V1 page written without a page index may legally end before the row's last value - whether it does
     * is undecidable until the next page's first rep level is seen. Deferring the last-started row leaves it to
     * {@link #readSpanningRow}, which follows the row across pages. Masked readers never defer: a row mask requires an
     * offset index, and a page index implies row-aligned pages.
     */
    int logicalRowsRemainingInCurrentPage() {
        ensurePageLoaded();
        if (pageRepLevels == null) {
            return pageSize - valuesConsumedInCurrentPage;
        }
        int remaining = pageLogicalRowCount - logicalRowsConsumedInCurrentPage;
        if (lastRowMayContinuePastPageEnd()) {
            remaining--;
        }
        return Math.max(0, remaining);
    }

    private boolean lastRowMayContinuePastPageEnd() {
        return survivingRows == null && pageCursor.hasRemaining();
    }

    /**
     * Rep-level stream for the current page; one entry per leaf element. Null for flat columns. The returned view is
     * valid only until the reader decodes its next page.
     */
    Levels currentPageRepLevels() {
        ensurePageLoaded();
        return pageRepLevels;
    }

    /**
     * Definition-level stream for the current page; one entry per leaf element. Null when the column has no optional
     * ancestor ({@code maxDef == 0}). Unlike the present/absent bitmap derived from these levels, this retains the
     * intermediate def levels that distinguish a null list from an empty list from a null element. The returned view is
     * valid only until the reader decodes its next page.
     */
    Levels currentPageDefLevels() {
        ensurePageLoaded();
        return pageDefLevels;
    }

    /** Position in the current page's value stream where the next {@link #readBatch} call will start. */
    int valuesConsumedInCurrentPage() {
        ensurePageLoaded();
        return valuesConsumedInCurrentPage;
    }

    /**
     * Number of leaf values that cover the next {@code logicalRows} logical rows in the current page. For flat columns
     * this is {@code logicalRows}; for repeated columns it counts entries from the consume cursor until the
     * {@code logicalRows}'th rep=0 marker.
     */
    int valuesForLogicalRows(int logicalRows) {
        ensurePageLoaded();
        if (logicalRows <= 0) {
            return 0;
        }
        if (pageRepLevels == null) {
            return logicalRows;
        }
        return pageRepLevels.valuesForRows(valuesConsumedInCurrentPage, logicalRows);
    }

    /**
     * Reads up to {@code maxValues} values from the current page into a new {@link ColumnVector}. The actual size is
     * {@code min(maxValues, pageSize - valuesConsumedInCurrentPage)}. Subsequent calls continue within the same page
     * until it is exhausted, then advance to the next page on demand.
     */
    ColumnVector readBatch(int maxValues, List<AutoCloseable> acquiredBuffers) {
        requireMaterializedValueDecode();
        if (maxValues <= 0) {
            throw new IllegalArgumentException("maxValues must be positive; got " + maxValues);
        }
        ensurePageLoaded();
        if (valuesConsumedInCurrentPage >= pageSize) {
            throw new IllegalStateException(
                    "No more values in column " + columnPath.dot() + " (totalValues=" + totalValues + ")");
        }
        int start = valuesConsumedInCurrentPage;
        int n = Math.min(maxValues, pageSize - start);

        ColumnVector vec = sliceVector(start, n, acquiredBuffers);
        int rowsInSlice = countLogicalRowsInSlice(start, n);

        valuesConsumedInCurrentPage += n;
        logicalRowsConsumedInCurrentPage += rowsInSlice;
        valuesConsumedTotal += n;
        if (survivingRows != null) {
            survivingRowsConsumedTotal += rowsInSlice;
        }
        if (valuesConsumedInCurrentPage >= pageSize) {
            advancePastCurrentPage();
        }
        return vec;
    }

    /**
     * Fails a whole-batch read on a reader that leaves its pages' values to the windowed lane. Such a reader holds no
     * materialized page payload a batch could slice; its values come one window at a time through
     * {@link #readMaskedWindow}.
     */
    private void requireMaterializedValueDecode() {
        if (valueDecode == ValueDecode.WINDOWED_MASK) {
            throw new IllegalStateException("Column " + columnPath.dot()
                    + " serves values one window at a time under the windowed value decode; it has no materialized "
                    + "page a batch read can slice");
        }
    }

    // ---- page-spanning row read ----

    /**
     * One logical row read whole, with batch-owned copies of its rep- and def-level windows. {@code repLevels} is null
     * for a flat column; {@code defLevels} is null when the column has no def-level stream to expose (no optional
     * ancestor, or the flat origin fast path consumed it into the validity bitmap).
     */
    record SpanningRow(ColumnVector vector, Levels repLevels, Levels defLevels) {}

    /**
     * Reads exactly one logical row, following its values across data-page boundaries when the row's rep=0 start sits
     * in one page and its remaining values in the following page(s). The driver calls this when some column's current
     * page holds no complete row ({@link #logicalRowsRemainingInCurrentPage()} deferred its last-started row); every
     * column then advances one row through this path to stay in lockstep.
     *
     * <p>A row confined to the current page comes back as a single {@link #readBatch} slice. A row that reaches the
     * page end continues part by part - decoding each following page - until a page opens with a rep=0 marker or the
     * chunk's value stream ends; the parts merge through {@link SpanningRowVectors}. Level windows come back as copies
     * because following the row decodes the next page, which overwrites the per-page level scratch the normal batch
     * windows view.
     */
    SpanningRow readSpanningRow(List<AutoCloseable> acquiredBuffers) {
        ensurePageLoaded();
        skipEmptyPages();
        if (pageRepLevels == null) {
            return readSingleRowFlat(acquiredBuffers);
        }
        List<ColumnVector> parts = new ArrayList<>(2);
        List<int[]> repParts = new ArrayList<>(2);
        List<int[]> defParts = new ArrayList<>(2);
        consumeRowPart(1, parts, repParts, defParts, acquiredBuffers);
        while (rowContinuesIntoNextPage()) {
            consumeRowPart(0, parts, repParts, defParts, acquiredBuffers);
        }
        ColumnVector vector = parts.size() == 1
                ? parts.get(0)
                : SpanningRowVectors.merge(leaf, parts, decodeBufferAllocator, acquiredBuffers);
        return new SpanningRow(vector, Levels.of(concatLevels(repParts)), Levels.of(concatLevels(defParts)));
    }

    /** The one-row read of a flat column: one value, plus a one-entry copy of its def window when the stream exists. */
    private SpanningRow readSingleRowFlat(List<AutoCloseable> acquiredBuffers) {
        Levels defWindow = null;
        if (pageDefLevels != null) {
            defWindow = Levels.of(new int[] {pageDefLevels.get(valuesConsumedInCurrentPage)});
        }
        ColumnVector vector = readBatch(1, acquiredBuffers);
        return new SpanningRow(vector, null, defWindow);
    }

    /**
     * Consumes the current page's values of the row under the cursor - up to the row's next in-page boundary or the
     * page end - collecting the part's vector and copies of its level windows. {@code rowsToCross} is 1 for the part
     * holding the row's rep=0 start and 0 for a continuation part that begins mid-row.
     */
    private void consumeRowPart(
            int rowsToCross,
            List<ColumnVector> parts,
            List<int[]> repParts,
            List<int[]> defParts,
            List<AutoCloseable> acquiredBuffers) {
        int start = valuesConsumedInCurrentPage;
        int take = pageRepLevels.valuesForRows(start, rowsToCross);
        repParts.add(pageRepLevels.toArray(start, take));
        defParts.add(pageDefLevels.toArray(start, take));
        parts.add(readBatch(take, acquiredBuffers));
    }

    /**
     * True when the row being followed spills into the next data page. A part that stopped at an in-page rep=0 boundary
     * left the page loaded with its cursor mid-page: the row is complete. A part that consumed the page to its end
     * advanced past it; the row continues if a next non-empty page exists and opens mid-row (first rep level above
     * zero). An exhausted cursor ends the row with the value stream.
     */
    private boolean rowContinuesIntoNextPage() {
        if (pageLoaded) {
            return false;
        }
        if (!loadNextNonEmptyPage()) {
            return false;
        }
        return pageRepLevels.get(0) != 0;
    }

    /**
     * Advances past zero-value data pages under the cursor. The one-row read must start on a page with values; a cursor
     * that exhausts here fails loud through {@link #loadNextPage()}'s malformed-stream error, because the driver only
     * asks for a row while values remain.
     */
    private void skipEmptyPages() {
        while (pageSize == 0) {
            advancePastCurrentPage();
            loadNextPage();
        }
    }

    /** Loads pages until one holds values, advancing past empty data pages; false when the cursor is exhausted. */
    private boolean loadNextNonEmptyPage() {
        while (tryLoadNextPage()) {
            if (pageSize > 0) {
                return true;
            }
            advancePastCurrentPage();
        }
        return false;
    }

    private static int[] concatLevels(List<int[]> parts) {
        int total = 0;
        for (int[] part : parts) {
            total += part.length;
        }
        int[] out = new int[total];
        int offset = 0;
        for (int[] part : parts) {
            System.arraycopy(part, 0, out, offset, part.length);
            offset += part.length;
        }
        return out;
    }

    // ---- windowed masked read ----

    /**
     * One window of a masked column: the surviving rows' values in window order, plus the matching windows of the rep-
     * and def-level streams. {@code repLevels} is null for a flat column; {@code defLevels} is null when the column has
     * no def-level stream.
     *
     * <p>Every component outlives the page it was read from. The vector's values sit in decode buffers the owning batch
     * closes, or - for a dictionary-encoded column - in the chunk's heap-owned dictionary entries; the level windows
     * are heap-owned copies of the page's level streams.
     */
    record MaskedWindow(ColumnVector vector, Levels repLevels, Levels defLevels) {}

    /**
     * Reads the next {@code windowRows} logical rows, materializing only the values of the rows set in
     * {@code survivingRows} (indexed from the window's first row). The values of the dropped rows are stepped over in
     * the page's decoder rather than decoded. The window must lie within the current page, which
     * {@link #logicalRowsRemainingInCurrentPage()} bounds.
     *
     * <p>A reader built with a row mask counts these rows over the mask's surviving rows: the window advances past
     * {@code windowRows} of them and the rows the mask dropped cost no decode either.
     */
    MaskedWindow readMaskedWindow(int windowRows, BitSet survivingRows, List<AutoCloseable> acquiredBuffers) {
        requireWindowedValueDecode();
        ensurePageLoaded();
        requireWindowFitsCurrentPage(windowRows);
        return readWindow(windowRows, valuesForLogicalRows(windowRows), survivingRows, acquiredBuffers);
    }

    /** Advances past {@code windowRows} logical rows of the current page without materializing any of their values. */
    void skipMaskedWindow(int windowRows) {
        requireWindowedValueDecode();
        ensurePageLoaded();
        requireWindowFitsCurrentPage(windowRows);
        advancePastWindow(valuesForLogicalRows(windowRows), windowRows);
    }

    /**
     * Fails a windowed read on a reader that materializes its pages at load. Only the windowed lane keeps a page's
     * value decoder open across the page's windows; the other lanes have none to serve a window from and would answer
     * an all-absent vector.
     */
    private void requireWindowedValueDecode() {
        if (valueDecode != ValueDecode.WINDOWED_MASK) {
            throw new IllegalStateException("Column " + columnPath.dot()
                    + " serves windows only under the windowed value decode; this reader decodes " + valueDecode);
        }
    }

    /**
     * Fails a window that reaches past the rows the current page can serve on its own. A window is resolved against one
     * page's level stream and one page's decoder; a longer one would stop at the page end and return fewer rows than
     * asked for, leaving the reader's row accounting adrift. A row that outruns its page goes through
     * {@link #readMaskedSpanningRow} instead.
     */
    private void requireWindowFitsCurrentPage(int windowRows) {
        int servableRows = logicalRowsRemainingInCurrentPage();
        if (windowRows > servableRows) {
            throw new IllegalArgumentException("Window of " + windowRows + " rows exceeds the " + servableRows
                    + " rows the current page of column " + columnPath.dot() + " can serve");
        }
    }

    /**
     * Reads exactly one logical row, following its values across data-page boundaries, and materializes them only when
     * {@code keep} is set. The driver calls this when some column's current page holds no complete row; every column
     * then advances one row through this path to stay in lockstep.
     */
    MaskedWindow readMaskedSpanningRow(boolean keep, List<AutoCloseable> acquiredBuffers) {
        requireWindowedValueDecode();
        BitSet survivors = new BitSet(1);
        if (keep) {
            survivors.set(0);
        }
        ensurePageLoaded();
        skipEmptyPages();
        if (pageRepLevels == null) {
            return readRowStartPart(survivors, acquiredBuffers);
        }
        List<MaskedWindow> parts = new ArrayList<>(2);
        parts.add(readRowStartPart(survivors, acquiredBuffers));
        while (rowContinuesIntoNextPage()) {
            parts.add(readMaskedContinuation(survivors, acquiredBuffers));
        }
        return mergeMaskedParts(parts, acquiredBuffers);
    }

    /**
     * Reads the current page's values of the row opening under the cursor, up to the row's next boundary or the page
     * end. This is the one window allowed to reach past {@link #logicalRowsRemainingInCurrentPage()}, which defers a
     * row that may continue into the next page precisely for this path to follow.
     */
    private MaskedWindow readRowStartPart(BitSet survivors, List<AutoCloseable> acquiredBuffers) {
        return readWindow(1, valuesForLogicalRows(1), survivors, acquiredBuffers);
    }

    /**
     * Reads the current page's leading values of a row that started in a previous page. The window spans no complete
     * logical row, hence bit zero of {@code survivingRows} decides the whole continuation run.
     */
    private MaskedWindow readMaskedContinuation(BitSet survivingRows, List<AutoCloseable> acquiredBuffers) {
        int continuationValues = pageRepLevels.valuesForRows(valuesConsumedInCurrentPage, 0);
        return readWindow(0, continuationValues, survivingRows, acquiredBuffers);
    }

    /**
     * Materializes the surviving rows of the window that opens at the reader's page position, covers {@code windowRows}
     * complete logical rows and advances the position by {@code windowValues}. The two counts are the page's own value
     * slots for an unmasked reader and the page's mask-surviving rows for a masked one; {@link #keptSlots} is where
     * either resolves to the raw page slots the decoder walk visits.
     */
    private MaskedWindow readWindow(
            int windowRows, int windowValues, BitSet survivingRows, List<AutoCloseable> acquiredBuffers) {
        int windowStart = valuesConsumedInCurrentPage;
        int[] keep = keptSlots(windowStart, windowRows, windowValues, survivingRows);
        Validity keptValidity = decodeKeptSlots(keep);
        ColumnVector vector = sliceVector(0, keep.length, keptValidity, acquiredBuffers);
        Levels repWindow = (pageRepLevels == null) ? null : pageRepLevels.gather(keep);
        Levels defWindow = (pageDefLevels == null) ? null : pageDefLevels.gather(keep);
        advancePastWindow(windowValues, windowRows);
        return new MaskedWindow(vector, repWindow, defWindow);
    }

    /**
     * The page value slots the window's surviving rows cover. A masked reader's window rows index the page's
     * mask-surviving rows and resolve through {@link #pageSurvivingSlots}; an unmasked window's flat rows map one to
     * one onto the page's slots and its repeated rows by run.
     */
    private int[] keptSlots(int windowStart, int windowRows, int windowValues, BitSet survivingRows) {
        if (pageSurvivingSlots != null) {
            return maskSurvivingSlots(windowStart, windowRows, survivingRows);
        }
        if (pageRepLevels == null) {
            return MaskedValues.flatSlots(windowStart, windowRows, survivingRows);
        }
        return MaskedValues.repeatedSlots(windowStart, pageRepLevels, windowStart, windowValues, survivingRows);
    }

    /** Resolves the window's kept rows, which index the page's mask-surviving rows, to the page's raw value slots. */
    private int[] maskSurvivingSlots(int windowStart, int windowRows, BitSet survivingRows) {
        int[] keptRows = MaskedValues.flatSlots(windowStart, windowRows, survivingRows);
        int[] slots = new int[keptRows.length];
        for (int i = 0; i < slots.length; i++) {
            slots[i] = pageSurvivingSlots[keptRows[i]];
        }
        return slots;
    }

    /**
     * Decodes the values at {@code keep} into a fresh window-sized payload, stepping the page decoder over the non-null
     * slots between them. Returns the window's own null mask, indexed by kept slot.
     */
    private Validity decodeKeptSlots(int[] keep) {
        clearTypedPayloads();
        allocateCompactedPayload(keep.length);
        BitSet keptValidity = new BitSet(keep.length);
        if (windowDecoder != null) {
            gatherKeptSlots(keep, keptValidity);
        }
        if (maskedIndices != null) {
            pageIndices = IntSequence.of(maskedIndices);
            maskedIndices = null;
        }
        freezeBinaryPageIfNeeded();
        return Validity.of(keptValidity, keep.length);
    }

    /**
     * Walks the page's slots from the decoder's cursor to the last kept slot, decoding one value per kept non-null slot
     * and coalescing the non-null slots between them into a single skip. The walk holds the decoder's invariant - every
     * non-null slot below {@link #windowDecoderSlot} has been decoded or skipped - by applying the coalesced skip both
     * before the next decoded value and once the walk ends: a walk ending on a kept null slot has no value to decode
     * after it and would otherwise leave the decoder behind for every later window of the page. Slots past the last
     * kept one are left for the next window's walk, which starts where this one stopped.
     */
    private void gatherKeptSlots(int[] keep, BitSet keptValidity) {
        int keepCursor = 0;
        int pendingSkip = 0;
        for (int slot = windowDecoderSlot; slot < pageRawSlotCount && keepCursor < keep.length; slot++) {
            boolean nonNull = pageSlotValidity.isValid(slot);
            if (keep[keepCursor] == slot) {
                if (nonNull) {
                    skipDroppedValues(pendingSkip);
                    pendingSkip = 0;
                    decodeOneInto(windowDecoder, keepCursor);
                    keptValidity.set(keepCursor);
                    decodedValueCount++;
                }
                keepCursor++;
            } else if (nonNull) {
                pendingSkip++;
            }
        }
        skipDroppedValues(pendingSkip);
        if (keep.length > 0) {
            windowDecoderSlot = keep[keep.length - 1] + 1;
        }
    }

    /** Steps the page decoder over the values of the non-null slots a window's walk crossed without keeping. */
    private void skipDroppedValues(int nonNullSlots) {
        if (nonNullSlots > 0) {
            windowDecoder.skip(nonNullSlots);
        }
    }

    /** Joins the per-page parts of one page-spanning row into a single window over the whole row. */
    private MaskedWindow mergeMaskedParts(List<MaskedWindow> parts, List<AutoCloseable> acquiredBuffers) {
        if (parts.size() == 1) {
            return parts.get(0);
        }
        List<ColumnVector> vectors = new ArrayList<>(parts.size());
        List<int[]> repParts = new ArrayList<>(parts.size());
        List<int[]> defParts = new ArrayList<>(parts.size());
        for (MaskedWindow part : parts) {
            vectors.add(part.vector());
            repParts.add(wholeLevels(part.repLevels()));
            defParts.add(wholeLevels(part.defLevels()));
        }
        ColumnVector merged = SpanningRowVectors.merge(leaf, vectors, decodeBufferAllocator, acquiredBuffers);
        return new MaskedWindow(merged, Levels.of(concatLevels(repParts)), Levels.of(concatLevels(defParts)));
    }

    private static int[] wholeLevels(Levels levels) {
        return levels.toArray(0, levels.size());
    }

    /** Moves the page cursors past a window, advancing to the next page when the window consumed the last value. */
    private void advancePastWindow(int windowValues, int windowRows) {
        valuesConsumedInCurrentPage += windowValues;
        logicalRowsConsumedInCurrentPage += windowRows;
        valuesConsumedTotal += windowValues;
        if (survivingRows != null) {
            survivingRowsConsumedTotal += windowRows;
        }
        if (valuesConsumedInCurrentPage >= pageSize) {
            advancePastCurrentPage();
        }
    }

    // ---- page lifecycle ----

    private void ensurePageLoaded() {
        if (!pageLoaded) {
            loadNextPage();
        }
    }

    private void loadNextPage() {
        if (!tryLoadNextPage()) {
            throw new MalformedFileException("Column " + columnPath.dot() + " exhausted page stream after "
                    + valuesConsumedTotal + " values; expected " + totalValues);
        }
    }

    /**
     * Loads and decodes the next data page, returning {@code false} when the page cursor is exhausted. The
     * page-spanning row read uses the {@code false} form directly: a row that reaches the end of the value stream ends
     * there, which is not a malformed file.
     */
    private boolean tryLoadNextPage() {
        Arena arena = Arena.ofConfined();
        try {
            DecodedPage page = readNextDataPage(arena);
            if (page == null) {
                arena.close();
                return false;
            }
            decodePage(page);
        } catch (RuntimeException e) {
            unwindPartialPageLoad();
            arena.close();
            throw e;
        }
        if (pageArenaOutlivesLoad()) {
            livePageArena = arena;
        } else {
            arena.close();
        }
        return true;
    }

    /**
     * Drops what a page load that threw already parked in the reader's fields: the pooled buffers, whose decode-budget
     * reservations would otherwise be held until the reader closes, and the windowed lane's page-scoped decoder - which
     * reads the page bytes the Arena is about to release - along with the page mask that places its skips.
     */
    private void unwindPartialPageLoad() {
        releaseOriginValidity();
        releasePageValues();
        releaseBinaryMeta();
        windowDecoder = null;
        windowDecoderSlot = 0;
        pageSlotValidity = null;
        pageRawSlotCount = 0;
        pageSurvivingSlots = null;
    }

    /**
     * True when something loaded with the page still reads the decompressed page bytes after the load returns: a page
     * payload sliced straight from the live page, or the windowed lane's decoder, which the page's windows keep pulling
     * values from.
     */
    private boolean pageArenaOutlivesLoad() {
        return pageBackingIsLivePage || windowDecoder != null;
    }

    private DecodedPage readNextDataPage(Arena arena) {
        try {
            return pageCursor.nextDataPage(maxLevels, codec, arena);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read next page for column " + columnPath.dot(), e);
        }
    }

    private void decodePage(DecodedPage page) {
        pageSize = page.valueCount();
        pageRepLevels = decodeRepLevels(page, pageSize);
        if (canDecodeOriginValidity()) {
            decodeOriginValidity(page);
        } else {
            pageDefLevels = decodeDefLevels(page, pageSize);
            pageValidity = pageDefLevels == null
                    ? Validity.allValid(pageSize)
                    : pageDefLevels.validityAt(maxLevels.maxDefinitionLevel());
        }
        pageLogicalRowCount = (pageRepLevels == null) ? pageSize : pageRepLevels.countOf(0);
        pageCursor.recordCurrentPageRowCount(pageLogicalRowCount);
        clearTypedPayloads();
        pageWasDictionary = PageDecoders.isDictionaryEncoded(page.valuesEncoding());
        if (valueDecode == ValueDecode.WINDOWED_MASK) {
            openWindowDecoder(page);
            narrowRowSpaceToSurvivingRows();
        } else {
            decodePageValues(page);
        }
        valuesConsumedInCurrentPage = 0;
        logicalRowsConsumedInCurrentPage = 0;
        pageLoaded = true;
    }

    /** The eager and whole-page-masked value lanes: both materialize the page's values before the first batch. */
    private void decodePageValues(DecodedPage page) {
        if (valueDecode == ValueDecode.PAGE_MASK && survivingRows != null) {
            decodeSelectedRows(page, pageCursor.currentPageFirstRowIndex());
        } else {
            decodeValuesByKind(page);
            if (survivingRows != null) {
                compactToSurvivingRows(pageCursor.currentPageFirstRowIndex());
            }
        }
        freezeBinaryPageIfNeeded();
    }

    /**
     * Opens the page's value decoder for the windowed masked lane and parks the page's per-slot null mask, which the
     * windows read to place their skips. The mask moves to {@link #pageSlotValidity}, where it stays indexed by raw
     * page slot while the reader's row space narrows to the rows a row mask keeps. An all-null page needs no decoder at
     * all.
     */
    private void openWindowDecoder(DecodedPage page) {
        pageSlotValidity = pageValidity;
        pageValidity = null;
        pageRawSlotCount = pageSize;
        windowDecoderSlot = 0;
        int nonNullCount = pageSlotValidity.cardinality();
        if (nonNullCount == 0) {
            windowDecoder = null;
            return;
        }
        if (isDictionaryBinaryPage()) {
            dictionaryEntries();
        }
        windowDecoder = PageDecoders.decoderFor(
                leaf.kind(), this::requiredByteWidth, page.valuesEncoding(), dictionary.orElse(null));
        windowDecoder.load(page.valueBytes(), nonNullCount);
    }

    /**
     * Narrows the windowed lane's row space to the rows a row mask keeps, leaving the reader serving those rows in
     * their order - the row space the eager masked lane's callers see. The page's raw slots stay reachable through
     * {@link #pageSurvivingSlots}, which every window's kept rows resolve through before the decoder walk. An unmasked
     * reader keeps the page's own row space.
     */
    private void narrowRowSpaceToSurvivingRows() {
        if (survivingRows == null) {
            return;
        }
        pageSurvivingSlots = survivingLocalPositions(pageCursor.currentPageFirstRowIndex(), pageRawSlotCount);
        pageSize = pageSurvivingSlots.length;
        pageLogicalRowCount = pageSize;
    }

    private void advancePastCurrentPage() {
        pageLoaded = false;
        pageSize = 0;
        pageLogicalRowCount = 0;
        pageValidity = null;
        pageRepLevels = null;
        pageDefLevels = null;
        windowDecoder = null;
        windowDecoderSlot = 0;
        pageSlotValidity = null;
        pageRawSlotCount = 0;
        pageSurvivingSlots = null;
        releaseOriginValidity();
        releasePageValues();
        releaseBinaryMeta();
        clearTypedPayloads();
        closeLivePageArena();
        valuesConsumedInCurrentPage = 0;
        logicalRowsConsumedInCurrentPage = 0;
    }

    /** Releases the off-heap origin validity bitmap held for the flat fast path, if any. */
    private void releaseOriginValidity() {
        if (pageValidityPooled != null) {
            pageValidityPooled.close();
            pageValidityPooled = null;
        }
    }

    /** Releases the pooled page-values segment held for the page's lifetime, if any. */
    private void releasePageValues() {
        if (pageValuesPooled != null) {
            pageValuesPooled.close();
            pageValuesPooled = null;
        }
    }

    /** Releases the pooled binary-metadata segment held for the page's lifetime, if any. */
    private void releaseBinaryMeta() {
        if (pageBinaryMetaPooled != null) {
            pageBinaryMetaPooled.close();
            pageBinaryMetaPooled = null;
        }
    }

    private void closeLivePageArena() {
        if (livePageArena != null) {
            livePageArena.close();
            livePageArena = null;
        }
    }

    private void clearTypedPayloads() {
        pageInts = null;
        pageLongs = null;
        pageFloats = null;
        pageDoubles = null;
        pageValues = null;
        pageBooleans = null;
        pageSegments = null;
        pageBinaryBacking = null;
        pageBinaryOffsets = null;
        pageValuePos = null;
        pageBackingIsLivePage = false;
        pageIndices = null;
        maskedIndices = null;
    }

    // ---- level decoding ----

    /**
     * True for the flat optional fast path: a top-level primitive column ({@code numParts == 1}, parent is the schema
     * root) that is optional with no repetition ({@code maxRep == 0}, {@code maxDef == 1}) and no row mask. There the
     * def-level stream is one present/absent bit per value, which decodes straight into an off-heap validity bitmap,
     * skipping both the {@code int[]} of def levels and the heap {@link BitSet}.
     *
     * <p>The top-level condition is load-bearing, not a convenience. A required leaf nested under an optional struct
     * also has {@code maxRep == 0, maxDef == 1} (the optional struct contributes the one definition level), but its
     * def-level stream is consumed downstream: {@code DremelAssembler} reads a struct's first row-aligned descendant
     * leaf's def levels to rebuild the optional struct's per-row null mask. Dropping that stream would silently lose
     * the struct's null rows. Only a top-level-flat leaf has no group ancestor that will ever ask for its def levels.
     *
     * <p>Both masked lanes keep the heap path. The whole-page lane rewrites the page validity row by row as it
     * compacts, and the windowed lane hands each window the def levels of the rows it kept, which the fast path has
     * already collapsed into a bitmap.
     */
    private boolean canDecodeOriginValidity() {
        if (columnPath.numParts() != 1) {
            return false;
        }
        if (maxLevels.maxRepetitionLevel() != 0 || maxLevels.maxDefinitionLevel() != 1) {
            return false;
        }
        if (survivingRows != null || valueDecode != ValueDecode.EAGER) {
            return false;
        }
        return pageSize > 0;
    }

    /**
     * Decodes the page's def-level stream straight into an off-heap LSB-first validity bitmap (bit set == present). A
     * null-bearing page keeps the pooled buffer behind {@link #pageValidity} for the page's lifetime, released at
     * page-advance; an all-valid page collapses to the bitmap-free representation and returns its buffer immediately.
     */
    private void decodeOriginValidity(DecodedPage page) {
        long byteSize = Math.max(1L, (pageSize + 7) / 8);
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        try {
            MemorySegment bitmap = pooled.segment();
            LevelDecoder defDecoder = new LevelDecoder(defBitWidth);
            defDecoder.load(page.defLevelBytes());
            int validCount = defDecoder.decodeValidityBitmap(pageSize, maxLevels.maxDefinitionLevel(), bitmap);
            pageDefLevels = null;
            if (validCount == pageSize) {
                pageValidity = Validity.allValid(pageSize);
                pooled.close();
            } else {
                pageValidity = Validity.ofSegment(bitmap, pageSize - validCount, pageSize);
                pageValidityPooled = pooled;
            }
        } catch (RuntimeException e) {
            pooled.close();
            throw e;
        }
    }

    // null marks "flat column, no rep levels exist" - distinct from "repeated column with zero values in the page"
    private Levels decodeRepLevels(DecodedPage page, int values) {
        if (maxLevels.maxRepetitionLevel() == 0) {
            return null;
        }
        if (values == 0) {
            return EMPTY_LEVELS;
        }
        LevelDecoder repDecoder = new LevelDecoder(repBitWidth);
        repDecoder.load(page.repLevelBytes());
        return repLevelScratch.decode(repDecoder, values);
    }

    // null marks "no optional ancestor, all values present" - the def-level section is absent from the page
    private Levels decodeDefLevels(DecodedPage page, int values) {
        if (maxLevels.maxDefinitionLevel() == 0) {
            return null;
        }
        if (values == 0) {
            return EMPTY_LEVELS;
        }
        LevelDecoder defDecoder = new LevelDecoder(defBitWidth);
        defDecoder.load(page.defLevelBytes());
        return defLevelScratch.decode(defDecoder, values);
    }

    private int countLogicalRowsInSlice(int start, int count) {
        if (pageRepLevels == null) {
            return count;
        }
        return pageRepLevels.rowsInRange(start, count);
    }

    // ---- per-kind value decoding ----

    private void decodeValuesByKind(DecodedPage page) {
        int nonNullCount = pageValidity.cardinality();
        decodedValueCount += nonNullCount;
        Encoding encoding = page.valuesEncoding();
        MemorySegment valueBuf = page.valueBytes();
        Dictionary<?> dict = dictionary.orElse(null);
        if (isDictionaryBinaryPage()) {
            if (survivingRows == null) {
                pageIndices = decodeDictionaryIndices(valueBuf, encoding, nonNullCount, dict);
            } else {
                pageIndices = IntSequence.of(decodeDictionaryIndicesToHeap(valueBuf, encoding, nonNullCount, dict));
            }
            return;
        }
        switch (leaf.kind()) {
            case INT32 -> decodeInt32Page(valueBuf, encoding, nonNullCount, dict);
            case INT64 -> decodeInt64Page(valueBuf, encoding, nonNullCount, dict);
            case FLOAT -> decodeFloatPage(valueBuf, encoding, nonNullCount, dict);
            case DOUBLE -> decodeDoublePage(valueBuf, encoding, nonNullCount, dict);
            case BOOLEAN -> pageBooleans = decodeBooleans(valueBuf, encoding, nonNullCount);
            case BYTE_ARRAY -> decodeByteArray(valueBuf, encoding, nonNullCount, dict);
            case FIXED_LEN_BYTE_ARRAY -> pageSegments = decodeFixedLenBinary(valueBuf, encoding, nonNullCount, dict);
            case INT96 -> pageSegments = decodeInt96(valueBuf, encoding, nonNullCount, dict);
        }
    }

    /** Routes an INT32 page to its value backing: live-page slice, pooled segment, or the masked heap lane. */
    private void decodeInt32Page(MemorySegment valueBuf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
            retainLiveValues(valueBuf);
        } else if (survivingRows == null) {
            decodeIntsIntoPageValues(valueBuf, encoding, nonNullCount, dict);
        } else {
            pageInts = decodeInts(valueBuf, encoding, nonNullCount, dict);
        }
    }

    /** Routes an INT64 page to its value backing: live-page slice, pooled segment, or the masked heap lane. */
    private void decodeInt64Page(MemorySegment valueBuf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
            retainLiveValues(valueBuf);
        } else if (survivingRows == null) {
            decodeLongsIntoPageValues(valueBuf, encoding, nonNullCount, dict);
        } else {
            pageLongs = decodeLongs(valueBuf, encoding, nonNullCount, dict);
        }
    }

    /** Routes a FLOAT page to its value backing: live-page slice, pooled segment, or the masked heap lane. */
    private void decodeFloatPage(MemorySegment valueBuf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
            retainLiveValues(valueBuf);
        } else if (survivingRows == null) {
            decodeFloatsIntoPageValues(valueBuf, encoding, nonNullCount, dict);
        } else {
            pageFloats = decodeFloats(valueBuf, encoding, nonNullCount, dict);
        }
    }

    /** Routes a DOUBLE page to its value backing: live-page slice, pooled segment, or the masked heap lane. */
    private void decodeDoublePage(MemorySegment valueBuf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
            retainLiveValues(valueBuf);
        } else if (survivingRows == null) {
            decodeDoublesIntoPageValues(valueBuf, encoding, nonNullCount, dict);
        } else {
            pageDoubles = decodeDoubles(valueBuf, encoding, nonNullCount, dict);
        }
    }

    /**
     * True when a fixed-width page's values (INT32 / INT64 / FLOAT / DOUBLE) can be sliced straight from the live page
     * segment with no heap array. Holds only for a PLAIN, all-valid page that is not later compacted to surviving rows:
     * PLAIN value bytes are already the little-endian layout the slice copies from, and a surviving-rows page compacts
     * through the heap arrays the {@link #gatherTypedPayloads(int[])} path reorders.
     */
    private boolean canSliceFixedWidthFromLivePage(Encoding encoding, int nonNullCount) {
        if (survivingRows != null) {
            return false;
        }
        return encoding == Encoding.PLAIN && nonNullCount == pageSize;
    }

    /** Keeps the live page segment as the page's value backing; the page Arena then stays open across the batches. */
    private void retainLiveValues(MemorySegment valueBuf) {
        pageValues = valueBuf.asReadOnly();
        pageBackingIsLivePage = true;
    }

    /**
     * Decodes a fixed-width page's values into one pooled page-lifetime segment instead of a fresh heap array. The
     * decoder writes the non-null values densely at the segment's head; a nullable page then spreads them to their row
     * slots in place. Null slots read zero - the contract the fresh heap arrays gave for free: validity-blind typed
     * getters and serialized buffers read deterministic zeros, never stale pool bytes from a previous borrower.
     */
    private void decodeIntsIntoPageValues(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment out = acquirePageValues((long) pageSize * Integer.BYTES);
        if (nonNullCount == 0) {
            out.fill((byte) 0);
            return;
        }
        PageDecoder<?> decoder = PageDecoders.intDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        decoder.decodeInts(nonNullCount, out, 0L);
        if (nonNullCount < pageSize) {
            spreadIntsInPlace(out, nonNullCount);
        }
    }

    /** INT64 form of {@link #decodeIntsIntoPageValues}: pooled page-lifetime segment, null slots zeroed. */
    private void decodeLongsIntoPageValues(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment out = acquirePageValues((long) pageSize * Long.BYTES);
        if (nonNullCount == 0) {
            out.fill((byte) 0);
            return;
        }
        PageDecoder<?> decoder = PageDecoders.longDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        decoder.decodeLongs(nonNullCount, out, 0L);
        if (nonNullCount < pageSize) {
            spreadLongsInPlace(out, nonNullCount);
        }
    }

    /** FLOAT form of {@link #decodeIntsIntoPageValues}: pooled page-lifetime segment, null slots zeroed. */
    private void decodeFloatsIntoPageValues(
            MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment out = acquirePageValues((long) pageSize * Float.BYTES);
        if (nonNullCount == 0) {
            out.fill((byte) 0);
            return;
        }
        PageDecoder<?> decoder = PageDecoders.floatDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        decoder.decodeFloats(nonNullCount, out, 0L);
        if (nonNullCount < pageSize) {
            spreadFloatsInPlace(out, nonNullCount);
        }
    }

    /** DOUBLE form of {@link #decodeIntsIntoPageValues}: pooled page-lifetime segment, null slots zeroed. */
    private void decodeDoublesIntoPageValues(
            MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment out = acquirePageValues((long) pageSize * Double.BYTES);
        if (nonNullCount == 0) {
            out.fill((byte) 0);
            return;
        }
        PageDecoder<?> decoder = PageDecoders.doubleDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        decoder.decodeDoubles(nonNullCount, out, 0L);
        if (nonNullCount < pageSize) {
            spreadDoublesInPlace(out, nonNullCount);
        }
    }

    /** Acquires the page-lifetime value segment from the decode valve; released at page advance. */
    private MemorySegment acquirePageValues(long byteSize) {
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(Math.max(1L, byteSize));
        pageValuesPooled = pooled;
        pageValues = pooled.segment();
        return pageValues;
    }

    /**
     * Spreads the {@code nonNullCount} dense i32 elements at the segment's head to their row slots, walking rows in
     * reverse and zeroing null slots in the same pass. In reverse order each move's source index never exceeds its
     * destination row ({@code j <= row} throughout: the j-th set bit's row is at least j), hence no dense element is
     * overwritten before it is read. The walk stops when {@code row == j}: the remaining prefix is all-valid and
     * already in place. Allocates nothing - a reused per-reader dense scratch would amortize nothing on
     * one-page-per-chunk files. Serves both the pooled page-values segment (INT32 values) and the pooled
     * dictionary-index segment: same element width, same {@link #pageValidity} positioning.
     */
    private void spreadIntsInPlace(MemorySegment values, int nonNullCount) {
        int j = nonNullCount - 1;
        for (int row = pageSize - 1; row >= 0 && row != j; row--) {
            if (pageValidity.isValid(row)) {
                values.setAtIndex(INT32, row, values.getAtIndex(INT32, j--));
            } else {
                values.setAtIndex(INT32, row, 0);
            }
        }
    }

    /** INT64 form of {@link #spreadIntsInPlace}. */
    private void spreadLongsInPlace(MemorySegment values, int nonNullCount) {
        int j = nonNullCount - 1;
        for (int row = pageSize - 1; row >= 0 && row != j; row--) {
            if (pageValidity.isValid(row)) {
                values.setAtIndex(INT64, row, values.getAtIndex(INT64, j--));
            } else {
                values.setAtIndex(INT64, row, 0L);
            }
        }
    }

    /** FLOAT form of {@link #spreadIntsInPlace}. */
    private void spreadFloatsInPlace(MemorySegment values, int nonNullCount) {
        int j = nonNullCount - 1;
        for (int row = pageSize - 1; row >= 0 && row != j; row--) {
            if (pageValidity.isValid(row)) {
                values.setAtIndex(FLOAT, row, values.getAtIndex(FLOAT, j--));
            } else {
                values.setAtIndex(FLOAT, row, 0f);
            }
        }
    }

    /** DOUBLE form of {@link #spreadIntsInPlace}. */
    private void spreadDoublesInPlace(MemorySegment values, int nonNullCount) {
        int j = nonNullCount - 1;
        for (int row = pageSize - 1; row >= 0 && row != j; row--) {
            if (pageValidity.isValid(row)) {
                values.setAtIndex(DOUBLE, row, values.getAtIndex(DOUBLE, j--));
            } else {
                values.setAtIndex(DOUBLE, row, 0d);
            }
        }
    }

    /**
     * Decodes an unmasked dictionary binary page's raw indexes into a pooled page-lifetime i32 segment, row-positioned.
     * Null rows must read the harmless placeholder index 0 because the dictionary vectors hand out the raw index
     * sequence wholesale (the Arrow buffer codec serializes every slot, null rows included) and a stale slot would
     * resolve {@code dictEntries[indices.get(row)]} to the wrong entry - or out of the dictionary's bounds entirely on
     * reused pool bytes. An all-valid page overwrites every slot; a nullable page decodes densely at the segment's head
     * and the in-place reverse spread zeroes the null slots; an all-null page is one explicit zero fill. Dictionary and
     * direct-binary pages are mutually exclusive per page, hence the binary-metadata handle is free to hold the index
     * segment. The chunk-level {@link #dictEntries} is materialized here on first use.
     */
    private IntSequence decodeDictionaryIndices(
            MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        dictionaryEntries();
        MemorySegment out = acquireBinaryMeta((long) pageSize * Integer.BYTES);
        if (nonNullCount == 0) {
            out.fill((byte) 0);
            return IntSequence.ofSegment(out, pageSize);
        }
        RleDictionaryPageDecoder<?> decoder = PageDecoders.dictionaryDecoderFor(leaf.kind(), encoding, dict);
        decoder.load(buf, nonNullCount);
        decoder.decodeIndicesInto(nonNullCount, out);
        if (nonNullCount < pageSize) {
            spreadIntsInPlace(out, nonNullCount);
        }
        return IntSequence.ofSegment(out, pageSize);
    }

    /** Heap decode of a dictionary binary page's raw indexes for masked reads, null slots zero. */
    private int[] decodeDictionaryIndicesToHeap(
            MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        dictionaryEntries();
        int[] indices = new int[pageSize];
        if (nonNullCount == 0) {
            return indices;
        }
        RleDictionaryPageDecoder<?> decoder = PageDecoders.dictionaryDecoderFor(leaf.kind(), encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeIndices(pageSize, indices, 0);
            return indices;
        }
        int[] dense = new int[nonNullCount];
        decoder.decodeIndices(nonNullCount, dense, 0);
        spreadInts(dense, indices);
        return indices;
    }

    /** Heap decode for masked reads; unmasked pages decode into the pooled page-values segment. */
    private int[] decodeInts(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        int[] out = new int[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.intDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeInts(pageSize, out, 0);
            return out;
        }
        int[] dense = new int[nonNullCount];
        decoder.decodeInts(nonNullCount, dense, 0);
        spreadInts(dense, out);
        return out;
    }

    /** Heap decode for masked reads; unmasked pages decode into the pooled page-values segment. */
    private long[] decodeLongs(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        long[] out = new long[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.longDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeLongs(pageSize, out, 0);
            return out;
        }
        long[] dense = new long[nonNullCount];
        decoder.decodeLongs(nonNullCount, dense, 0);
        spreadLongs(dense, out);
        return out;
    }

    /** Heap decode for masked reads; unmasked pages decode into the pooled page-values segment. */
    private float[] decodeFloats(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        float[] out = new float[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.floatDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeFloats(pageSize, out, 0);
            return out;
        }
        float[] dense = new float[nonNullCount];
        decoder.decodeFloats(nonNullCount, dense, 0);
        spreadFloats(dense, out);
        return out;
    }

    /** Heap decode for masked reads; unmasked pages decode into the pooled page-values segment. */
    private double[] decodeDoubles(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        double[] out = new double[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.doubleDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeDoubles(pageSize, out, 0);
            return out;
        }
        double[] dense = new double[nonNullCount];
        decoder.decodeDoubles(nonNullCount, dense, 0);
        spreadDoubles(dense, out);
        return out;
    }

    private boolean[] decodeBooleans(MemorySegment buf, Encoding encoding, int nonNullCount) {
        boolean[] out = new boolean[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.booleanDecoderFor(encoding);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBooleans(pageSize, out, 0);
            return out;
        }
        boolean[] dense = new boolean[nonNullCount];
        decoder.decodeBooleans(nonNullCount, dense, 0);
        spreadBooleans(dense, out);
        return out;
    }

    /**
     * Decodes a {@code BYTE_ARRAY} page. Contiguous PLAIN/DELTA_LENGTH pages decode straight into the shared backing
     * via the sink; every other case (dictionary, DELTA_BYTE_ARRAY, surviving-rows compaction) keeps the scratch path
     * that fills {@code pageSegments} and freezes afterward.
     */
    private void decodeByteArray(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        if (canDecodeBinaryDirect(encoding)) {
            decodeBinaryDirect(buf, encoding, nonNullCount);
        } else {
            pageSegments = decodeBinary(buf, encoding, nonNullCount, dict);
        }
    }

    /**
     * True when a {@code BYTE_ARRAY} page can decode straight into the backing buffer with no per-value segment. Holds
     * only for contiguous PLAIN and DELTA_LENGTH_BYTE_ARRAY pages whose values are not later compacted to surviving
     * rows.
     */
    private boolean canDecodeBinaryDirect(Encoding encoding) {
        if (pageWasDictionary || survivingRows != null) {
            return false; // dictionary uses indexes; surviving-rows pages compact through the scratch path
        }
        if (leaf.kind() != PrimitiveKind.BYTE_ARRAY) {
            return false; // fixed-width kinds handled elsewhere; DELTA_BYTE_ARRAY is non-contiguous
        }
        return encoding == Encoding.PLAIN || encoding == Encoding.DELTA_LENGTH_BYTE_ARRAY;
    }

    /**
     * Points the page binary backing at the live decompressed page value segment and records each row's source byte
     * position and the row-indexed byte offsets in one pooled page-lifetime segment (positions first, offsets after),
     * copying no value bytes. The page Arena is kept alive across the page's batches (see {@link #loadNextPage()});
     * each batch copies its rows' bytes straight from the page at slice time. Leaves {@code pageSegments} null because
     * no per-value scratch segments are produced.
     */
    private void decodeBinaryDirect(MemorySegment buf, Encoding encoding, int nonNullCount) {
        pageBinaryBacking = buf.asReadOnly();
        pageBackingIsLivePage = true;
        int[] densePositions = densePositionsScratch.array(nonNullCount);
        int[] denseLengths = denseLengthsScratch.array(nonNullCount);
        if (nonNullCount > 0) {
            PageDecoder<?> decoder = PageDecoders.binaryDecoderFor(encoding, null); // PLAIN/DELTA need no dictionary
            decoder.load(buf, nonNullCount);
            decoder.decodeBinaryLayout(nonNullCount, densePositions, denseLengths, 0);
        }
        long valuePosBytes = (long) pageSize * Integer.BYTES;
        long offsetsBytes = (pageSize + 1L) * Integer.BYTES;
        MemorySegment meta = acquireBinaryMeta(valuePosBytes + offsetsBytes);
        MemorySegment valuePosWindow = meta.asSlice(0L, valuePosBytes);
        MemorySegment offsetsWindow = meta.asSlice(valuePosBytes, offsetsBytes);
        spreadPositions(densePositions, pageSize, valuePosWindow);
        rowOffsetsFromDenseLengths(denseLengths, pageSize, offsetsWindow);
        pageValuePos = IntSequence.ofSegment(valuePosWindow, pageSize);
        pageBinaryOffsets = IntSequence.ofSegment(offsetsWindow, pageSize + 1);
        pageSegments = null;
    }

    /** Acquires the page-lifetime binary-metadata segment from the decode valve; released at page advance. */
    private MemorySegment acquireBinaryMeta(long byteSize) {
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(Math.max(1L, byteSize));
        pageBinaryMetaPooled = pooled;
        return pooled.segment();
    }

    /**
     * Writes the row-indexed source positions into {@code valuePos}: present rows take the next dense position; null
     * rows are never read and may hold stale bytes from the pooled segment.
     */
    private void spreadPositions(int[] densePositions, int rowCount, MemorySegment valuePos) {
        int dense = 0;
        for (int row = 0; row < rowCount; row++) {
            if (pageValidity.isValid(row)) {
                valuePos.setAtIndex(INT32, row, densePositions[dense++]);
            }
        }
    }

    /**
     * Writes the row-indexed cumulative value-byte offsets into {@code offsets}, filling every slot of {@code [0,
     * rowCount]} unconditionally; null rows are zero-length runs.
     */
    private void rowOffsetsFromDenseLengths(int[] denseLengths, int rowCount, MemorySegment offsets) {
        int dense = 0;
        int acc = 0;
        for (int row = 0; row < rowCount; row++) {
            offsets.setAtIndex(INT32, row, acc);
            if (pageValidity.isValid(row)) {
                acc += denseLengths[dense++];
            }
        }
        offsets.setAtIndex(INT32, rowCount, acc);
    }

    private MemorySegment[] decodeBinary(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.binaryDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBinary(pageSize, out, 0);
        } else {
            MemorySegment[] dense = new MemorySegment[nonNullCount];
            decoder.decodeBinary(nonNullCount, dense, 0);
            spreadSegments(dense, out);
        }
        return out;
    }

    private MemorySegment[] decodeFixedLenBinary(
            MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        int byteWidth = requiredByteWidth();
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.fixedLenBinaryDecoderFor(encoding, byteWidth, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBinary(pageSize, out, 0);
        } else {
            MemorySegment[] dense = new MemorySegment[nonNullCount];
            decoder.decodeBinary(nonNullCount, dense, 0);
            spreadSegments(dense, out);
        }
        return out;
    }

    private MemorySegment[] decodeInt96(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = PageDecoders.int96DecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeBinary(pageSize, out, 0);
        } else {
            MemorySegment[] dense = new MemorySegment[nonNullCount];
            decoder.decodeBinary(nonNullCount, dense, 0);
            spreadSegments(dense, out);
        }
        return out;
    }

    private int requiredByteWidth() {
        return leaf.typeLength()
                .orElseThrow(() -> new IllegalStateException(
                        "FIXED_LEN_BYTE_ARRAY column " + columnPath.dot() + " is missing typeLength in schema"));
    }

    /**
     * True when the current page is dictionary-encoded and the column is one of the binary kinds. These pages decode
     * into {@code pageIndices} over the shared {@code dictEntries} rather than per-value segments.
     */
    private boolean isDictionaryBinaryPage() {
        if (!pageWasDictionary) {
            return false;
        }
        return switch (leaf.kind()) {
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> true;
            default -> false;
        };
    }

    /**
     * Builds the chunk-level array of dictionary entries the first time a dictionary binary page is decoded, then
     * caches it along with its total byte size. Each entry is a heap-owned, read-only {@link MemorySegment} the rows
     * index into.
     */
    private MemorySegment[] dictionaryEntries() {
        if (dictEntries != null) {
            return dictEntries;
        }
        Dictionary<?> dict = dictionary.orElseThrow(() -> new IllegalStateException(
                "Dictionary-encoded data page requires a loaded Dictionary; none supplied for column "
                        + columnPath.dot()));
        MemorySegment[] entries = new MemorySegment[dict.size()];
        long entryBytes = 0;
        for (int k = 0; k < entries.length; k++) {
            entries[k] = (MemorySegment) dict.get(k);
            entryBytes += entries[k].byteSize();
        }
        dictEntries = entries;
        dictEntryBytes = entryBytes;
        return dictEntries;
    }

    /**
     * Converts the just-decoded PLAIN/DELTA binary page from per-value slice objects (views into the page Arena) into a
     * single heap-owned backing buffer the page Arena can no longer invalidate. Dictionary-encoded binary pages do not
     * reach the freeze: they decode into row-positioned {@code pageIndices} over the heap-owned {@code dictEntries},
     * which outlive the Arena on their own.
     *
     * <p>The scratch {@code MemorySegment[]} that the decoders, compaction, and selected-rows decode filled is read
     * once here, then released; from this point on the page's binary values live only in {@code pageBinaryBacking}
     * (plus {@code pageBinaryOffsets} for the variable kind). One allocation per page replaces one wrapper per value.
     *
     * <p>A null {@code pageSegments} also means the contiguous-binary direct path already produced the final layout;
     * nothing remains to freeze.
     */
    private void freezeBinaryPageIfNeeded() {
        if (pageSegments == null || pageWasDictionary) {
            return;
        }
        switch (leaf.kind()) {
            case BYTE_ARRAY -> freezeVariableBinary(pageSegments);
            case FIXED_LEN_BYTE_ARRAY -> freezeFixedBinary(pageSegments, requiredByteWidth());
            case INT96 -> freezeFixedBinary(pageSegments, INT96_WIDTH);
            default -> {
                /* non-binary kinds keep their primitive arrays */
            }
        }
        pageSegments = null;
    }

    /**
     * Concatenates the non-null values into one heap backing plus a row-indexed {@code int[]} of absolute byte offsets,
     * reusing the same packing the consolidating vector factory uses so the two layouts cannot drift.
     */
    private void freezeVariableBinary(MemorySegment[] values) {
        BinaryVector.VariableLayout layout = BinaryVector.consolidate(values);
        pageBinaryBacking = layout.backing();
        pageBinaryOffsets = IntSequence.of(layout.offsets());
    }

    /**
     * Packs the full-width slots into one heap backing of {@code size * byteWidth} (null rows zeroed), reusing the same
     * packing the fixed-width vector factories use so the two layouts cannot drift.
     */
    private void freezeFixedBinary(MemorySegment[] values, int byteWidth) {
        pageBinaryBacking = FixedLenBinaryVector.packFullSlots(values, byteWidth);
    }

    // ---- spread (dense -> validity-positioned) ----

    private void spreadInts(int[] dense, int[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadLongs(long[] dense, long[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadFloats(float[] dense, float[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadDoubles(double[] dense, double[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadBooleans(boolean[] dense, boolean[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    private void spreadSegments(MemorySegment[] dense, MemorySegment[] dst) {
        int j = 0;
        for (int i = pageValidity.nextSetBit(0); i >= 0; i = pageValidity.nextSetBit(i + 1)) {
            dst[i] = dense[j++];
        }
    }

    // ---- skip-decode for masked reads ----

    /**
     * Decodes only the surviving rows' non-null values for the just-loaded page, advancing the decoder past the
     * unselected non-null values with {@link PageDecoder#skip(int)} instead of materializing them. Produces per-page
     * arrays identical to {@link #compactToSurvivingRows(long)} while touching only the kept values. Flat columns only:
     * one value per row, no repetition levels.
     */
    private void decodeSelectedRows(DecodedPage page, long pageFirstRow) {
        if (pageRepLevels != null) {
            throw new IllegalStateException(
                    "skip-decode is only defined for flat columns; column " + columnPath.dot() + " is repeated");
        }
        int[] keep = survivingLocalPositions(pageFirstRow, pageSize);
        int nonNullCount = pageValidity.cardinality();
        Encoding encoding = page.valuesEncoding();
        Dictionary<?> dict = dictionary.orElse(null);
        allocateCompactedPayload(keep.length);
        BitSet keptValidity = new BitSet(keep.length);

        if (nonNullCount > 0) {
            PageDecoder<?> decoder = PageDecoders.decoderFor(leaf.kind(), this::requiredByteWidth, encoding, dict);
            decoder.load(page.valueBytes(), nonNullCount);
            gatherSelectedValues(decoder, keep, keptValidity);
        }

        if (pageDefLevels != null) {
            pageDefLevels = pageDefLevels.gather(keep);
        }
        if (maskedIndices != null) {
            pageIndices = IntSequence.of(maskedIndices);
            maskedIndices = null;
        }
        pageValidity = Validity.of(keptValidity, keep.length);
        pageSize = keep.length;
        pageLogicalRowCount = keep.length;
    }

    /**
     * Walks the page rows in order, decoding one value per kept non-null row into slot {@code keepCursor} and skipping
     * over the unselected non-null values that precede it. Non-null values past the last kept row are never consumed.
     */
    private void gatherSelectedValues(PageDecoder<?> decoder, int[] keep, BitSet keptValidity) {
        int keepCursor = 0;
        int pendingSkip = 0;
        for (int row = 0; row < pageSize && keepCursor < keep.length; row++) {
            boolean nonNull = pageValidity.isValid(row);
            boolean kept = keep[keepCursor] == row;
            if (kept) {
                if (nonNull) {
                    if (pendingSkip > 0) {
                        decoder.skip(pendingSkip);
                        pendingSkip = 0;
                    }
                    decodeOneInto(decoder, keepCursor);
                    keptValidity.set(keepCursor);
                    decodedValueCount++;
                }
                keepCursor++;
            } else if (nonNull) {
                pendingSkip++;
            }
        }
    }

    private void allocateCompactedPayload(int size) {
        if (isDictionaryBinaryPage()) {
            dictionaryEntries();
            maskedIndices = new int[size];
            return;
        }
        switch (leaf.kind()) {
            case INT32 -> pageInts = new int[size];
            case INT64 -> pageLongs = new long[size];
            case FLOAT -> pageFloats = new float[size];
            case DOUBLE -> pageDoubles = new double[size];
            case BOOLEAN -> pageBooleans = new boolean[size];
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> pageSegments = new MemorySegment[size];
        }
    }

    private void decodeOneInto(PageDecoder<?> decoder, int index) {
        if (isDictionaryBinaryPage()) {
            ((RleDictionaryPageDecoder<?>) decoder).decodeIndices(1, maskedIndices, index);
            return;
        }
        switch (leaf.kind()) {
            case INT32 -> decoder.decodeInts(1, pageInts, index);
            case INT64 -> decoder.decodeLongs(1, pageLongs, index);
            case FLOAT -> decoder.decodeFloats(1, pageFloats, index);
            case DOUBLE -> decoder.decodeDoubles(1, pageDoubles, index);
            case BOOLEAN -> decoder.decodeBooleans(1, pageBooleans, index);
            case BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, INT96 -> decoder.decodeBinary(1, pageSegments, index);
        }
    }

    // ---- row compaction for masked reads ----

    /**
     * Drops the rows of the just-decoded page that fall outside the surviving ranges, leaving only the surviving rows
     * (in order) in the per-page arrays. Flat columns only: one value per row, no repetition levels.
     */
    private void compactToSurvivingRows(long pageFirstRow) {
        int[] keep = survivingLocalPositions(pageFirstRow, pageSize);
        if (keep.length == pageSize) {
            return;
        }
        pageValidity = gatherValidity(pageValidity, keep);
        if (pageDefLevels != null) {
            pageDefLevels = pageDefLevels.gather(keep);
        }
        gatherTypedPayloads(keep);
        pageSize = keep.length;
        pageLogicalRowCount = keep.length;
    }

    /** Local positions {@code p} of the page whose row group index {@code pageFirstRow + p} is in the surviving set. */
    private int[] survivingLocalPositions(long pageFirstRow, int pageRows) {
        long pageLast = pageFirstRow + pageRows - 1;
        int[] keep = new int[pageRows];
        int count = 0;
        for (RowRanges.Range range : survivingRows.ranges()) {
            if (range.last() < pageFirstRow || range.first() > pageLast) {
                continue;
            }
            long from = Math.max(range.first(), pageFirstRow);
            long to = Math.min(range.last(), pageLast);
            for (long row = from; row <= to; row++) {
                keep[count++] = (int) (row - pageFirstRow);
            }
        }
        return (count == pageRows) ? keep : Arrays.copyOf(keep, count);
    }

    private void gatherTypedPayloads(int[] keep) {
        if (pageIndices != null) {
            pageIndices = gatherIndices(pageIndices, keep);
        } else if (pageInts != null) {
            pageInts = gatherInts(pageInts, keep);
        } else if (pageLongs != null) {
            pageLongs = gatherLongs(pageLongs, keep);
        } else if (pageFloats != null) {
            pageFloats = gatherFloats(pageFloats, keep);
        } else if (pageDoubles != null) {
            pageDoubles = gatherDoubles(pageDoubles, keep);
        } else if (pageBooleans != null) {
            pageBooleans = gatherBooleans(pageBooleans, keep);
        } else if (pageSegments != null) {
            pageSegments = gatherSegments(pageSegments, keep);
        }
    }

    private static Validity gatherValidity(Validity source, int[] keep) {
        BitSet out = new BitSet(keep.length);
        for (int j = 0; j < keep.length; j++) {
            if (source.isValid(keep[j])) {
                out.set(j);
            }
        }
        return Validity.of(out, keep.length);
    }

    private static IntSequence gatherIndices(IntSequence source, int[] keep) {
        int[] out = new int[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source.get(keep[j]);
        }
        return IntSequence.of(out);
    }

    private static int[] gatherInts(int[] source, int[] keep) {
        int[] out = new int[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static long[] gatherLongs(long[] source, int[] keep) {
        long[] out = new long[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static float[] gatherFloats(float[] source, int[] keep) {
        float[] out = new float[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static double[] gatherDoubles(double[] source, int[] keep) {
        double[] out = new double[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static boolean[] gatherBooleans(boolean[] source, int[] keep) {
        boolean[] out = new boolean[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    private static MemorySegment[] gatherSegments(MemorySegment[] source, int[] keep) {
        MemorySegment[] out = new MemorySegment[keep.length];
        for (int j = 0; j < keep.length; j++) {
            out[j] = source[keep[j]];
        }
        return out;
    }

    // ---- vector slicing ----

    private ColumnVector sliceVector(int start, int n, List<AutoCloseable> acquiredBuffers) {
        return sliceVector(start, n, sliceCurrentValidity(start, n, acquiredBuffers), acquiredBuffers);
    }

    private ColumnVector sliceVector(
            int start, int n, Validity sliceValidityMask, List<AutoCloseable> acquiredBuffers) {
        return switch (leaf.kind()) {
            case INT32 -> sliceInt(start, n, sliceValidityMask, acquiredBuffers);
            case INT64 -> sliceLong(start, n, sliceValidityMask, acquiredBuffers);
            case FLOAT -> sliceFloat(start, n, sliceValidityMask, acquiredBuffers);
            case DOUBLE -> sliceDouble(start, n, sliceValidityMask, acquiredBuffers);
            case BOOLEAN -> sliceBoolean(start, n, sliceValidityMask, acquiredBuffers);
            case BYTE_ARRAY -> sliceVariableBinary(start, n, sliceValidityMask, acquiredBuffers);
            case FIXED_LEN_BYTE_ARRAY ->
                sliceFixedBinary(start, n, requiredByteWidth(), sliceValidityMask, acquiredBuffers);
            case INT96 -> sliceInt96(start, n, sliceValidityMask, acquiredBuffers);
        };
    }

    /**
     * Copies the page's double values for the slice into a buffer the decode valve hands out (a native segment while
     * the off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch closes the buffer on
     * {@link io.tileverse.parquetry.columnar.ParquetRecordBatch#close()}; the resulting vector holds no heap value
     * array.
     *
     * <p>When the page decoded off-heap, {@link #pageValues} - the live page segment (PLAIN all-valid) or the pooled
     * decode buffer - already has the little-endian DOUBLE layout of the target, making the copy a raw byte copy with
     * no heap {@code double[]}. The heap branch remains only for masked reads, which compact into {@link #pageDoubles}
     * and copy element by element.
     */
    private DoubleVector sliceDouble(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Double.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageValues != null) {
            MemorySegment.copy(pageValues, (long) start * Double.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageDoubles, start, dst, DOUBLE, 0L, n);
        }
        return DoubleVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Copies the page's int values for the slice into a buffer the decode valve hands out (a native segment while the
     * off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch closes the buffer on
     * {@link io.tileverse.parquetry.columnar.ParquetRecordBatch#close()}; the resulting vector holds no heap value
     * array.
     *
     * <p>When the page decoded off-heap, {@link #pageValues} - the live page segment (PLAIN all-valid) or the pooled
     * decode buffer - already has the little-endian INT32 layout of the target, making the copy a raw byte copy with no
     * heap {@code int[]}. The heap branch remains only for masked reads, which compact into {@link #pageInts} and copy
     * element by element.
     */
    private IntVector sliceInt(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Integer.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageValues != null) {
            MemorySegment.copy(pageValues, (long) start * Integer.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageInts, start, dst, INT32, 0L, n);
        }
        return IntVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Copies the page's long values for the slice into a buffer the decode valve hands out. When the page decoded
     * off-heap, {@link #pageValues} - the live page segment (PLAIN all-valid) or the pooled decode buffer - already has
     * the little-endian INT64 layout of the target, making the copy a raw byte copy with no heap {@code long[]}. The
     * heap branch remains only for masked reads, which compact into {@link #pageLongs} and copy element by element.
     */
    private LongVector sliceLong(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Long.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageValues != null) {
            MemorySegment.copy(pageValues, (long) start * Long.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageLongs, start, dst, INT64, 0L, n);
        }
        return LongVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Copies the page's float values for the slice into a buffer the decode valve hands out. When the page decoded
     * off-heap, {@link #pageValues} - the live page segment (PLAIN all-valid) or the pooled decode buffer - already has
     * the little-endian FLOAT layout of the target, making the copy a raw byte copy with no heap {@code float[]}. The
     * heap branch remains only for masked reads, which compact into {@link #pageFloats} and copy element by element.
     */
    private FloatVector sliceFloat(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Float.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageValues != null) {
            MemorySegment.copy(pageValues, (long) start * Float.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageFloats, start, dst, FLOAT, 0L, n);
        }
        return FloatVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Packs the page's boolean values for the slice into an LSB-first bit-packed buffer the decode valve hands out (a
     * native segment while the off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch
     * closes the buffer on {@link io.tileverse.parquetry.columnar.ParquetRecordBatch#close()}; the resulting vector
     * holds no heap value array. The bit layout matches the Arrow validity bitmap convention.
     */
    private BooleanVector sliceBoolean(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = Math.max(1L, (n + 7) / 8);
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        packBooleanBitmap(pageBooleans, start, n, dst);
        return BooleanVector.segmentBacked(dst, n, sliceValidity);
    }

    private static void packBooleanBitmap(boolean[] source, int start, int n, MemorySegment dst) {
        long byteCount = (n + 7) / 8;
        for (long byteIndex = 0; byteIndex < byteCount; byteIndex++) {
            int packed = 0;
            for (int bit = 0; bit < 8; bit++) {
                int row = (int) (byteIndex * 8 + bit);
                if (row < n && source[start + row]) {
                    packed |= 1 << bit;
                }
            }
            dst.set(ValueLayout.JAVA_BYTE, byteIndex, (byte) packed);
        }
    }

    private BinaryVector sliceVariableBinary(
            int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        if (isDictionaryBinaryPage()) {
            IntSequence sliceIndices = sliceDictionaryIndices(start, n, acquiredBuffers);
            return BinaryVector.dictionary(dictEntries, sliceIndices, sliceValidity, dictEntryBytes);
        }
        return sliceConsolidatedBinary(start, n, sliceValidity, acquiredBuffers);
    }

    /**
     * Copies the slice's dictionary indexes into a buffer the decode valve hands out, registered on the batch. The copy
     * is required: the page's index backing - the pooled page-metadata segment, or the masked lanes' heap array - is
     * released or dropped at page-advance while the batch and its vectors live on.
     */
    private IntSequence sliceDictionaryIndices(int start, int n, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Integer.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(Math.max(1L, byteSize));
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        pageIndices.copyInto(start, n, dst, 0L);
        return IntSequence.ofSegment(dst, n);
    }

    /**
     * Copies the slice's variable-length value bytes into a buffer the decode valve hands out (a native segment while
     * the off-heap decode budget has room, an mmap of an on-disk file otherwise) and rebases the row offsets to that
     * window, in a second valve buffer. The bytes come straight from the live decompressed page for the direct path, or
     * in one contiguous run from the frozen page backing for DELTA_BYTE_ARRAY and surviving-rows pages. The owning
     * batch closes both buffers on {@link io.tileverse.parquetry.columnar.ParquetRecordBatch#close()}; the resulting
     * vector holds no heap value bytes and no heap offsets, only the validity.
     */
    private BinaryVector sliceConsolidatedBinary(
            int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        int base = pageBinaryOffsets.get(start);
        long windowBytes = (long) pageBinaryOffsets.get(start + n) - base;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(Math.max(1L, windowBytes));
        acquiredBuffers.add(pooled);
        // The valve never hands out a zero-length buffer; slice back to the true window so an all-null slice is empty.
        MemorySegment dst = pooled.segment().asSlice(0L, windowBytes);
        if (pageBackingIsLivePage) {
            copyRowsFromLivePage(start, n, dst);
        } else if (windowBytes > 0) {
            MemorySegment.copy(pageBinaryBacking, base, dst, 0L, windowBytes);
        }
        long offsetsBytes = (n + 1L) * Integer.BYTES;
        SegmentPool.Pooled offsetsPooled = decodeBufferAllocator.acquireMandatory(offsetsBytes);
        acquiredBuffers.add(offsetsPooled);
        MemorySegment rebased = offsetsPooled.segment();
        for (int i = 0; i <= n; i++) {
            rebased.setAtIndex(INT32, i, pageBinaryOffsets.get(start + i) - base);
        }
        return BinaryVector.of(dst, IntSequence.ofSegment(rebased, n + 1), sliceValidity);
    }

    /**
     * Copies each slice row's value bytes straight from the live page into {@code dst}, packed contiguously. The live
     * page's bytes are not concatenated (PLAIN interleaves a length prefix per value), hence the per-row copy keyed by
     * {@link #pageValuePos}; null rows are zero-length and skipped.
     */
    private void copyRowsFromLivePage(int start, int n, MemorySegment dst) {
        long writePos = 0L;
        for (int i = 0; i < n; i++) {
            int row = start + i;
            int len = pageBinaryOffsets.get(row + 1) - pageBinaryOffsets.get(row);
            if (len > 0) {
                MemorySegment.copy(pageBinaryBacking, pageValuePos.get(row), dst, writePos, len);
                writePos += len;
            }
        }
    }

    /**
     * Copies the page's fixed-width binary slots for the slice into a buffer the decode valve hands out (a native
     * segment while the off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch closes
     * the buffer on {@link io.tileverse.parquetry.columnar.ParquetRecordBatch#close()}. The dictionary branch reuses
     * shared heap entries and acquires nothing.
     */
    private FixedLenBinaryVector sliceFixedBinary(
            int start, int n, int width, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        if (isDictionaryBinaryPage()) {
            IntSequence sliceIndices = sliceDictionaryIndices(start, n, acquiredBuffers);
            return FixedLenBinaryVector.dictionary(dictEntries, sliceIndices, width, sliceValidity);
        }
        long byteSize = (long) n * width;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        MemorySegment.copy(pageBinaryBacking, (long) start * width, dst, 0L, byteSize);
        return FixedLenBinaryVector.of(dst, width, sliceValidity);
    }

    private Int96Vector sliceInt96(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        if (isDictionaryBinaryPage()) {
            IntSequence sliceIndices = sliceDictionaryIndices(start, n, acquiredBuffers);
            return Int96Vector.dictionary(dictEntries, sliceIndices, sliceValidity);
        }
        long byteSize = (long) n * INT96_WIDTH;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        MemorySegment.copy(pageBinaryBacking, (long) start * INT96_WIDTH, dst, 0L, byteSize);
        return Int96Vector.of(dst, sliceValidity);
    }

    /**
     * The validity mask for the page slice {@code [start, start + n)}. A null-free page answers all-valid without
     * touching its bitmap. The off-heap origin representation copies the slice's bytes into its own off-heap bitmap
     * (registered on the batch) because the page's origin bitmap is released at page-advance while the batch and its
     * vectors live on; the heap representation slices through {@link Validity#slice}. Either way the resulting mask
     * owns memory independent of the page.
     */
    private Validity sliceCurrentValidity(int start, int n, List<AutoCloseable> acquiredBuffers) {
        if (!pageValidity.hasNulls()) {
            return Validity.allValid(n);
        }
        if (pageValidityPooled != null) {
            return sliceOriginValidity(start, n, acquiredBuffers);
        }
        return pageValidity.slice(start, n);
    }

    /**
     * Copies the off-heap origin bitmap's bits for {@code [start, start + n)} into a fresh off-heap slice bitmap
     * rebased to bit zero: whole bytes when {@code start} is byte-aligned (the common page-aligned batch case),
     * shift-combined byte pairs otherwise - never per-bit walks. A range that turns out fully present needs no bitmap;
     * its buffer goes straight back to the pool. Otherwise the slice bitmap is registered on the batch, which closes it
     * on batch close.
     */
    private Validity sliceOriginValidity(int start, int n, List<AutoCloseable> acquiredBuffers) {
        MemorySegment origin = pageValidityPooled.segment();
        long byteSize = Math.max(1L, (n + 7) / 8);
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        MemorySegment dst = pooled.segment();
        int valid = copyPresenceBits(origin, start, n, dst);
        if (valid == n) {
            pooled.close();
            return Validity.allValid(n);
        }
        acquiredBuffers.add(pooled);
        return Validity.ofSegment(dst, n - valid, n);
    }

    /**
     * Copies presence bits {@code [start, start + n)} of {@code origin} into {@code dst} rebased to bit zero,
     * LSB-first, returning the count of set (present) bits. Bits of {@code dst}'s final byte at or beyond {@code n} are
     * cleared. The unaligned form combines each destination byte from two source bytes; the high source byte of the
     * final pair may not exist (the range can end inside the low byte), which the bounds guard covers.
     */
    private static int copyPresenceBits(MemorySegment origin, int start, int n, MemorySegment dst) {
        int dstBytes = (n + 7) >>> 3;
        int srcByte = start >>> 3;
        int shift = start & 7;
        long originBytes = origin.byteSize();
        int valid = 0;
        for (int i = 0; i < dstBytes; i++) {
            int combined = origin.get(ValueLayout.JAVA_BYTE, (long) srcByte + i) & 0xff;
            if (shift != 0) {
                long highIndex = (long) srcByte + i + 1;
                int high = highIndex < originBytes ? origin.get(ValueLayout.JAVA_BYTE, highIndex) & 0xff : 0;
                combined = ((combined >>> shift) | (high << (8 - shift))) & 0xff;
            }
            if (i == dstBytes - 1) {
                int bitsInLastByte = n - (i << 3);
                combined &= (1 << bitsInLastByte) - 1;
            }
            valid += Integer.bitCount(combined);
            dst.set(ValueLayout.JAVA_BYTE, i, (byte) combined);
        }
        return valid;
    }

    // ---- close ----

    /** Releases the page Arena, the level scratches, and any pooled page state; idempotent via {@code advance}. */
    void close() {
        advancePastCurrentPage();
        repLevelScratch.close();
        defLevelScratch.close();
    }

    // ---- diagnostics ----

    /** Data pages this reader actually decoded; pages skipped by the surviving-row mask are not counted. */
    int decodedDataPageCount() {
        return pageCursor.decodedDataPageCount();
    }

    /** Data pages this reader advanced past without decoding because they fell outside the surviving rows. */
    int skippedDataPageCount() {
        return pageCursor.skippedDataPageCount();
    }

    /** Count of non-null values run through a value decoder by this reader. */
    long decodedValueCount() {
        return decodedValueCount;
    }

    ColumnPath columnPath() {
        return columnPath;
    }

    Optional<Dictionary<?>> dictionary() {
        return dictionary;
    }
}
