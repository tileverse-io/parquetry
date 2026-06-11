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
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.Levels;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.Validity;
import io.tileverse.parquetry.data.Compression;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.read.page.ByteStreamSplitDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.ByteStreamSplitFloatDecoder;
import io.tileverse.parquetry.internal.read.page.DecodedPage;
import io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt32Decoder;
import io.tileverse.parquetry.internal.read.page.DeltaBinaryPackedInt64Decoder;
import io.tileverse.parquetry.internal.read.page.DeltaByteArrayDecoder;
import io.tileverse.parquetry.internal.read.page.DeltaLengthByteArrayDecoder;
import io.tileverse.parquetry.internal.read.page.Dictionary;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;
import io.tileverse.parquetry.internal.read.page.PageCursor;
import io.tileverse.parquetry.internal.read.page.PageDecoder;
import io.tileverse.parquetry.internal.read.page.PageSelection;
import io.tileverse.parquetry.internal.read.page.PlainBinaryDecoder;
import io.tileverse.parquetry.internal.read.page.PlainBooleanDecoder;
import io.tileverse.parquetry.internal.read.page.PlainDoubleDecoder;
import io.tileverse.parquetry.internal.read.page.PlainFixedLenBinaryDecoder;
import io.tileverse.parquetry.internal.read.page.PlainFloatDecoder;
import io.tileverse.parquetry.internal.read.page.PlainInt32Decoder;
import io.tileverse.parquetry.internal.read.page.PlainInt64Decoder;
import io.tileverse.parquetry.internal.read.page.PlainInt96Decoder;
import io.tileverse.parquetry.internal.read.page.RleBooleanDecoder;
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
 * until the next page decode), and decodes values either into heap arrays (fixed-width and dictionary kinds) or, for
 * contiguous PLAIN / DELTA_LENGTH binary and PLAIN all-valid INT32 / INT64 / FLOAT / DOUBLE, into row-indexed positions
 * over the still-open page. For the heap kinds the Arena is closed before {@link #loadNextPage()} returns; for the
 * live-page kinds the Arena is kept open across the page's batches and closed at page-advance, because the per-batch
 * slices copy their bytes straight from the page. Each {@link #readBatch} call then slices the current page;
 * within-page splitting is correct because the page state (level scratches, heap arrays, or the live page plus its
 * offsets) outlives every slice of the page.
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
    private final boolean skipDecode;
    private final DecodeBufferAllocator decodeBufferAllocator;
    private final LevelScratch repLevelScratch;
    private final LevelScratch defLevelScratch;
    private final IntScratch valuePosScratch = new IntScratch();
    private final IntScratch binaryOffsetsScratch = new IntScratch();
    private final IntScratch densePositionsScratch = new IntScratch();
    private final IntScratch denseLengthsScratch = new IntScratch();
    private final IntScratch indicesScratch = new IntScratch();

    // Per-page state. Populated by loadNextPage; cleared on advance.
    private boolean pageLoaded;
    private int pageSize;
    private int pageLogicalRowCount;
    // The page's per-row null mask, whichever representation Validity collapsed it to: all-valid (no bitmap), a heap
    // BitSet, or - on the flat optional fast path - the pooled off-heap bitmap decoded straight from the def-level
    // stream. The pooled buffer behind that last representation is pageValidityPooled, held for the page's lifetime
    // and released at page-advance; it is non-null exactly when pageValidity reads the off-heap bitmap.
    private Validity pageValidity;
    private SegmentPool.Pooled pageValidityPooled;
    private Levels pageRepLevels; // null when maxRep == 0
    private Levels pageDefLevels; // null when maxDef == 0, or when the origin fast path consumed the stream
    private int[] pageInts;
    private long[] pageLongs;
    private float[] pageFloats;
    private double[] pageDoubles;
    // Set only for PLAIN, all-valid fixed-width pages (INT32 / INT64 / FLOAT / DOUBLE): the live page value segment in
    // the column's little-endian layout. When non-null the heap array of the same kind is left null and slices copy
    // straight from this segment (no heap array).
    private MemorySegment pageLiveValues;
    private boolean[] pageBooleans;

    // Scratch holder for PLAIN/DELTA binary between value decode and the freeze step that consolidates it into the
    // shared backing. Never populated for dictionary-encoded binary, which decodes into pageIndices instead.
    private MemorySegment[] pageSegments;
    // PLAIN/DELTA binary is frozen into one shared heap backing; offsets are meaningful only for the variable kind.
    private MemorySegment pageBinaryBacking;
    private int[] pageBinaryOffsets;
    private boolean pageWasDictionary;

    // Per-row source positions for the direct off-heap binary path: pageValuePos[row] is the absolute byte offset of
    // the row's value bytes within the live page value segment (pageBinaryBacking). Null rows are never read and may
    // hold stale values from the reused scratch.
    private int[] pageValuePos;
    // True while a page payload - the binary backing or the fixed-width live value segment - points into a still-open
    // page Arena the slices read from directly. The Arena is then kept alive across the page's batches and closed at
    // page-advance.
    private boolean pageBackingIsLivePage;
    // The page Arena kept alive across the page's batches when pageBackingIsLivePage; closed at advance / reader close.
    private Arena livePageArena;

    // One row-positioned dictionary index per row for dictionary-encoded binary, resolved through the chunk-level
    // dictEntries array. A null row holds the harmless placeholder index zero and its nullness lives in pageValidity.
    private int[] pageIndices;
    // Chunk-level dictionary entries (shared heap-owned segments), built once from this.dictionary and reused across
    // pages.
    private MemorySegment[] dictEntries;

    private int valuesConsumedInCurrentPage;
    private int logicalRowsConsumedInCurrentPage;
    private long valuesConsumedTotal;
    private long survivingRowsConsumedTotal;
    private long decodedValueCount;

    BatchColumnReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf) {
        this(decodeBufferAllocator, chunk, leaf, null, null, false);
    }

    BatchColumnReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf,
            RowRanges survivingRows,
            OffsetIndex offsetIndex) {
        this(decodeBufferAllocator, chunk, leaf, survivingRows, offsetIndex, false);
    }

    BatchColumnReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull FetchedColumnChunk chunk,
            @NonNull SchemaNode.Primitive leaf,
            RowRanges survivingRows,
            OffsetIndex offsetIndex,
            boolean skipDecode) {
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.repLevelScratch = new LevelScratch(decodeBufferAllocator);
        this.defLevelScratch = new LevelScratch(decodeBufferAllocator);
        this.skipDecode = skipDecode;
        this.columnPath = chunk.path();
        this.leaf = leaf;
        this.maxLevels = new LevelMaxima(chunk.maxRepetitionLevel(), chunk.maxDefinitionLevel());
        this.codec = Compression.forWireCodec(chunk.metadata().codec());
        this.totalValues = chunk.metadata().numValues();
        this.defBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxDefinitionLevel());
        this.repBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxRepetitionLevel());
        this.dictionary = chunk.dictionary();
        if (survivingRows != null && offsetIndex != null) {
            this.survivingRows = survivingRows;
            PageSelection selection = PageSelection.forRanges(offsetIndex.pageLocations(), totalValues, survivingRows);
            this.pageCursor = new PageCursor(chunk.compressedSegment(), columnPath, selection);
        } else {
            this.survivingRows = null;
            this.pageCursor = new PageCursor(chunk.compressedSegment(), columnPath);
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
     * Logical (top-level) row count remaining in the current page. For flat columns this equals
     * {@link #rowsRemainingInCurrentPage()}; for repeated columns it counts rep=0 markers ahead of the consume cursor.
     */
    int logicalRowsRemainingInCurrentPage() {
        ensurePageLoaded();
        if (pageRepLevels == null) {
            return pageSize - valuesConsumedInCurrentPage;
        }
        return pageLogicalRowCount - logicalRowsConsumedInCurrentPage;
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

        valuesConsumedInCurrentPage += n;
        logicalRowsConsumedInCurrentPage += countLogicalRowsInSlice(start, n);
        valuesConsumedTotal += n;
        if (survivingRows != null) {
            survivingRowsConsumedTotal += n;
        }
        if (valuesConsumedInCurrentPage >= pageSize) {
            advancePastCurrentPage();
        }
        return vec;
    }

    // ---- page lifecycle ----

    private void ensurePageLoaded() {
        if (!pageLoaded) {
            loadNextPage();
        }
    }

    private void loadNextPage() {
        Arena arena = Arena.ofConfined();
        try {
            DecodedPage page = readNextDataPage(arena);
            decodePage(page);
        } catch (RuntimeException e) {
            // the origin-validity bitmap may already be parked in its field when the value decode throws; unwind it
            // here rather than holding it (and its decode-budget reservation) until the reader is closed
            releaseOriginValidity();
            arena.close();
            throw e;
        }
        if (pageBackingIsLivePage) {
            livePageArena = arena;
        } else {
            arena.close();
        }
    }

    private DecodedPage readNextDataPage(Arena arena) {
        DecodedPage page;
        try {
            page = pageCursor.nextDataPage(maxLevels, codec, arena);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read next page for column " + columnPath.dot(), e);
        }
        if (page == null) {
            throw new MalformedFileException("Column " + columnPath.dot() + " exhausted page stream after "
                    + valuesConsumedTotal + " values; expected " + totalValues);
        }
        return page;
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
        clearTypedPayloads();
        pageWasDictionary = isDictionaryEncoded(page.valuesEncoding());
        if (skipDecode && survivingRows != null) {
            decodeSelectedRows(page, pageCursor.currentPageFirstRowIndex());
        } else {
            decodeValuesByKind(page);
            if (survivingRows != null) {
                compactToSurvivingRows(pageCursor.currentPageFirstRowIndex());
            }
        }
        freezeBinaryPageIfNeeded();
        valuesConsumedInCurrentPage = 0;
        logicalRowsConsumedInCurrentPage = 0;
        pageLoaded = true;
    }

    private void advancePastCurrentPage() {
        pageLoaded = false;
        pageSize = 0;
        pageLogicalRowCount = 0;
        pageValidity = null;
        pageRepLevels = null;
        pageDefLevels = null;
        releaseOriginValidity();
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
        pageLiveValues = null;
        pageBooleans = null;
        pageSegments = null;
        pageBinaryBacking = null;
        pageBinaryOffsets = null;
        pageValuePos = null;
        pageBackingIsLivePage = false;
        pageIndices = null;
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
     * <p>Masked reads keep the heap path because their compaction rewrites the page validity row by row.
     */
    private boolean canDecodeOriginValidity() {
        if (columnPath.numParts() != 1) {
            return false;
        }
        if (maxLevels.maxRepetitionLevel() != 0 || maxLevels.maxDefinitionLevel() != 1) {
            return false;
        }
        if (survivingRows != null || skipDecode) {
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
            pageIndices = decodeDictionaryIndices(valueBuf, encoding, nonNullCount, dict);
            return;
        }
        switch (leaf.kind()) {
            case INT32 -> {
                if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
                    retainLiveValues(valueBuf);
                } else {
                    pageInts = decodeInts(valueBuf, encoding, nonNullCount, dict);
                }
            }
            case INT64 -> {
                if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
                    retainLiveValues(valueBuf);
                } else {
                    pageLongs = decodeLongs(valueBuf, encoding, nonNullCount, dict);
                }
            }
            case FLOAT -> {
                if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
                    retainLiveValues(valueBuf);
                } else {
                    pageFloats = decodeFloats(valueBuf, encoding, nonNullCount, dict);
                }
            }
            case DOUBLE -> {
                if (canSliceFixedWidthFromLivePage(encoding, nonNullCount)) {
                    retainLiveValues(valueBuf);
                } else {
                    pageDoubles = decodeDoubles(valueBuf, encoding, nonNullCount, dict);
                }
            }
            case BOOLEAN -> pageBooleans = decodeBooleans(valueBuf, encoding, nonNullCount);
            case BYTE_ARRAY -> decodeByteArray(valueBuf, encoding, nonNullCount, dict);
            case FIXED_LEN_BYTE_ARRAY -> pageSegments = decodeFixedLenBinary(valueBuf, encoding, nonNullCount, dict);
            case INT96 -> pageSegments = decodeInt96(valueBuf, encoding, nonNullCount, dict);
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
        pageLiveValues = valueBuf.asReadOnly();
        pageBackingIsLivePage = true;
    }

    /**
     * Decodes a dictionary binary page into a row-positioned {@code int[]} of dictionary indexes over the reused
     * scratch. Null rows must read the harmless placeholder index 0 because the dictionary vectors hand out the raw
     * index array wholesale (the Arrow buffer codec serializes every slot, null rows included) and a stale slot would
     * resolve {@code dictEntries[indices[row]]} to the wrong entry; a nullable page therefore zero-fills the scratch
     * before spreading (an all-valid page overwrites every slot). The chunk-level {@link #dictEntries} is materialized
     * here on first use.
     */
    private int[] decodeDictionaryIndices(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        dictionaryEntries();
        int[] indices = indicesScratch.array(pageSize);
        if (nonNullCount < pageSize) {
            Arrays.fill(indices, 0, pageSize, 0);
        }
        if (nonNullCount == 0) {
            return indices;
        }
        RleDictionaryPageDecoder<?> decoder = dictionaryDecoderFor(encoding, dict);
        decoder.load(buf, nonNullCount);
        if (nonNullCount == pageSize) {
            decoder.decodeIndices(pageSize, indices, 0);
            return indices;
        }
        // dictionary pages and direct-binary pages are mutually exclusive per page; the dense scratch is shared
        int[] dense = densePositionsScratch.array(nonNullCount);
        decoder.decodeIndices(nonNullCount, dense, 0);
        spreadInts(dense, indices);
        return indices;
    }

    private int[] decodeInts(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        int[] out = new int[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = intDecoderFor(encoding, dict);
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

    private long[] decodeLongs(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        long[] out = new long[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = longDecoderFor(encoding, dict);
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

    private float[] decodeFloats(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        float[] out = new float[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = floatDecoderFor(encoding, dict);
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

    private double[] decodeDoubles(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        double[] out = new double[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = doubleDecoderFor(encoding, dict);
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
        PageDecoder<?> decoder = booleanDecoderFor(encoding);
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
     * position and the row-indexed byte offsets, copying no value bytes. The page Arena is kept alive across the page's
     * batches (see {@link #loadNextPage()}); each batch copies its rows' bytes straight from the page at slice time.
     * Leaves {@code pageSegments} null because no per-value scratch segments are produced.
     */
    private void decodeBinaryDirect(MemorySegment buf, Encoding encoding, int nonNullCount) {
        pageBinaryBacking = buf.asReadOnly();
        pageBackingIsLivePage = true;
        int[] densePositions = densePositionsScratch.array(nonNullCount);
        int[] denseLengths = denseLengthsScratch.array(nonNullCount);
        if (nonNullCount > 0) {
            PageDecoder<?> decoder = binaryDecoderFor(encoding, null); // PLAIN/DELTA need no dictionary
            decoder.load(buf, nonNullCount);
            decoder.decodeBinaryLayout(nonNullCount, densePositions, denseLengths, 0);
        }
        pageValuePos = spreadPositions(densePositions, pageSize);
        pageBinaryOffsets = rowOffsetsFromDenseLengths(denseLengths, pageSize);
        pageSegments = null;
    }

    /**
     * Row-indexed source positions: present rows take the next dense position; null rows are never read and may hold
     * stale values from the reused scratch.
     */
    private int[] spreadPositions(int[] densePositions, int rowCount) {
        int[] positions = valuePosScratch.array(rowCount);
        int dense = 0;
        for (int row = 0; row < rowCount; row++) {
            if (pageValidity.isValid(row)) {
                positions[row] = densePositions[dense++];
            }
        }
        return positions;
    }

    /** Row-indexed cumulative value-byte offsets (length {@code rowCount + 1}); null rows are zero-length runs. */
    private int[] rowOffsetsFromDenseLengths(int[] denseLengths, int rowCount) {
        int[] offsets = binaryOffsetsScratch.array(rowCount + 1);
        int dense = 0;
        int acc = 0;
        for (int row = 0; row < rowCount; row++) {
            offsets[row] = acc;
            if (pageValidity.isValid(row)) {
                acc += denseLengths[dense++];
            }
        }
        offsets[rowCount] = acc;
        return offsets;
    }

    private MemorySegment[] decodeBinary(MemorySegment buf, Encoding encoding, int nonNullCount, Dictionary<?> dict) {
        MemorySegment[] out = new MemorySegment[pageSize];
        if (nonNullCount == 0) {
            return out;
        }
        PageDecoder<?> decoder = binaryDecoderFor(encoding, dict);
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
        PageDecoder<?> decoder = fixedLenBinaryDecoderFor(encoding, byteWidth, dict);
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
        PageDecoder<?> decoder = int96DecoderFor(encoding, dict);
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
     * Dictionary-encoded binary values are references into the column's {@link Dictionary}, whose values are
     * heap-owned, immutable, GC-managed segments (not page-Arena or pool memory). They outlive the page Arena and
     * survive the chunk's close, so they need no per-row heap copy. PLAIN/DELTA values are zero-copy views into the
     * page Arena and must be copied out before that Arena closes.
     */
    private static boolean isDictionaryEncoded(Encoding encoding) {
        return encoding == Encoding.RLE_DICTIONARY || encoding == Encoding.PLAIN_DICTIONARY;
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
     * caches it. Each entry is a heap-owned, read-only {@link MemorySegment} the rows index into.
     */
    private MemorySegment[] dictionaryEntries() {
        if (dictEntries != null) {
            return dictEntries;
        }
        Dictionary<?> dict = dictionary.orElseThrow(() -> new IllegalStateException(
                "Dictionary-encoded data page requires a loaded Dictionary; none supplied for column "
                        + columnPath.dot()));
        MemorySegment[] entries = new MemorySegment[dict.size()];
        for (int k = 0; k < entries.length; k++) {
            entries[k] = (MemorySegment) dict.get(k);
        }
        dictEntries = entries;
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
        pageBinaryOffsets = layout.offsets();
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

    // ---- decoder dispatch ----

    private PageDecoder<?> intDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt32Decoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedInt32Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT32");
            default -> throw unsupported(encoding, "INT32");
        };
    }

    private PageDecoder<?> longDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt64Decoder();
            case DELTA_BINARY_PACKED -> new DeltaBinaryPackedInt64Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT64");
            default -> throw unsupported(encoding, "INT64");
        };
    }

    private PageDecoder<?> floatDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainFloatDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitFloatDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "FLOAT");
            default -> throw unsupported(encoding, "FLOAT");
        };
    }

    private PageDecoder<?> doubleDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainDoubleDecoder();
            case BYTE_STREAM_SPLIT -> new ByteStreamSplitDoubleDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "DOUBLE");
            default -> throw unsupported(encoding, "DOUBLE");
        };
    }

    private static PageDecoder<?> booleanDecoderFor(Encoding encoding) {
        return switch (encoding) {
            case PLAIN -> new PlainBooleanDecoder();
            case RLE -> new RleBooleanDecoder();
            default -> throw unsupported(encoding, "BOOLEAN");
        };
    }

    private PageDecoder<?> binaryDecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainBinaryDecoder();
            case DELTA_BYTE_ARRAY -> new DeltaByteArrayDecoder();
            case DELTA_LENGTH_BYTE_ARRAY -> new DeltaLengthByteArrayDecoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "BYTE_ARRAY");
            default -> throw unsupported(encoding, "BYTE_ARRAY");
        };
    }

    private PageDecoder<?> fixedLenBinaryDecoderFor(Encoding encoding, int byteWidth, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainFixedLenBinaryDecoder(byteWidth);
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "FIXED_LEN_BYTE_ARRAY");
            default -> throw unsupported(encoding, "FIXED_LEN_BYTE_ARRAY");
        };
    }

    private PageDecoder<?> int96DecoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (encoding) {
            case PLAIN -> new PlainInt96Decoder();
            case RLE_DICTIONARY, PLAIN_DICTIONARY -> requireDictionaryDecoder(dict, "INT96");
            default -> throw unsupported(encoding, "INT96");
        };
    }

    private static PageDecoder<?> requireDictionaryDecoder(Dictionary<?> dict, String kindLabel) {
        if (dict == null) {
            throw new IllegalStateException(
                    "Dictionary-encoded data page requires a loaded Dictionary; none supplied for " + kindLabel);
        }
        return new RleDictionaryPageDecoder<>(dict);
    }

    /** The index decoder for a dictionary binary page; raw indexes are read with {@code decodeIndices}. */
    private RleDictionaryPageDecoder<?> dictionaryDecoderFor(Encoding encoding, Dictionary<?> dict) {
        if (!isDictionaryEncoded(encoding)) {
            throw unsupported(encoding, leaf.kind().name());
        }
        return (RleDictionaryPageDecoder<?>)
                requireDictionaryDecoder(dict, leaf.kind().name());
    }

    private static UnsupportedOperationException unsupported(Encoding encoding, String kindLabel) {
        return new UnsupportedOperationException(
                "BatchColumnReader has no decoder wired for encoding " + encoding + " on " + kindLabel);
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
            PageDecoder<?> decoder = decoderFor(encoding, dict);
            decoder.load(page.valueBytes(), nonNullCount);
            gatherSelectedValues(decoder, keep, keptValidity);
        }

        if (pageDefLevels != null) {
            pageDefLevels = pageDefLevels.gather(keep);
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
            pageIndices = new int[size];
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
            ((RleDictionaryPageDecoder<?>) decoder).decodeIndices(1, pageIndices, index);
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

    private PageDecoder<?> decoderFor(Encoding encoding, Dictionary<?> dict) {
        return switch (leaf.kind()) {
            case INT32 -> intDecoderFor(encoding, dict);
            case INT64 -> longDecoderFor(encoding, dict);
            case FLOAT -> floatDecoderFor(encoding, dict);
            case DOUBLE -> doubleDecoderFor(encoding, dict);
            case BOOLEAN -> booleanDecoderFor(encoding);
            case BYTE_ARRAY -> binaryDecoderFor(encoding, dict);
            case FIXED_LEN_BYTE_ARRAY -> fixedLenBinaryDecoderFor(encoding, requiredByteWidth(), dict);
            case INT96 -> int96DecoderFor(encoding, dict);
        };
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
            pageIndices = gatherInts(pageIndices, keep);
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
        Validity sliceValidityMask = sliceCurrentValidity(start, n, acquiredBuffers);
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
     * {@link io.tileverse.parquetry.batch.ParquetRecordBatch#close()}; the resulting vector holds no heap value array.
     *
     * <p>For a PLAIN all-valid page the bytes come straight from the live page segment ({@link #pageLiveValues}), whose
     * little-endian DOUBLE layout matches the target, making the copy a raw byte copy with no heap {@code double[]}.
     * Otherwise the page decoded into the heap {@link #pageDoubles}, which the copy reads element by element.
     */
    private DoubleVector sliceDouble(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Double.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageLiveValues != null) {
            MemorySegment.copy(pageLiveValues, (long) start * Double.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageDoubles, start, dst, DOUBLE, 0L, n);
        }
        return DoubleVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Copies the page's int values for the slice into a buffer the decode valve hands out (a native segment while the
     * off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch closes the buffer on
     * {@link io.tileverse.parquetry.batch.ParquetRecordBatch#close()}; the resulting vector holds no heap value array.
     *
     * <p>For a PLAIN all-valid page the bytes come straight from the live page segment ({@link #pageLiveValues}), whose
     * little-endian INT32 layout matches the target, making the copy a raw byte copy with no heap {@code int[]}.
     * Otherwise the page decoded into the heap {@link #pageInts}, which the copy reads element by element.
     */
    private IntVector sliceInt(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Integer.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageLiveValues != null) {
            MemorySegment.copy(pageLiveValues, (long) start * Integer.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageInts, start, dst, INT32, 0L, n);
        }
        return IntVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Copies the page's long values for the slice into a buffer the decode valve hands out. For a PLAIN all-valid page
     * the bytes come straight from the live page segment ({@link #pageLiveValues}), whose little-endian INT64 layout
     * matches the target, making the copy a raw byte copy with no heap {@code long[]}. Otherwise the page decoded into
     * the heap {@link #pageLongs}, which the copy reads element by element.
     */
    private LongVector sliceLong(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Long.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageLiveValues != null) {
            MemorySegment.copy(pageLiveValues, (long) start * Long.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageLongs, start, dst, INT64, 0L, n);
        }
        return LongVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Copies the page's float values for the slice into a buffer the decode valve hands out. For a PLAIN all-valid page
     * the bytes come straight from the live page segment ({@link #pageLiveValues}), whose little-endian FLOAT layout
     * matches the target, making the copy a raw byte copy with no heap {@code float[]}. Otherwise the page decoded into
     * the heap {@link #pageFloats}, which the copy reads element by element.
     */
    private FloatVector sliceFloat(int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        long byteSize = (long) n * Float.BYTES;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(byteSize);
        acquiredBuffers.add(pooled);
        MemorySegment dst = pooled.segment();
        if (pageLiveValues != null) {
            MemorySegment.copy(pageLiveValues, (long) start * Float.BYTES, dst, 0L, byteSize);
        } else {
            MemorySegment.copy(pageFloats, start, dst, FLOAT, 0L, n);
        }
        return FloatVector.segmentBacked(dst, sliceValidity);
    }

    /**
     * Packs the page's boolean values for the slice into an LSB-first bit-packed buffer the decode valve hands out (a
     * native segment while the off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch
     * closes the buffer on {@link io.tileverse.parquetry.batch.ParquetRecordBatch#close()}; the resulting vector holds
     * no heap value array. The bit layout matches the Arrow validity bitmap convention.
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
            int[] sliceIndices = Arrays.copyOfRange(pageIndices, start, start + n);
            return BinaryVector.dictionary(dictEntries, sliceIndices, sliceValidity);
        }
        return sliceConsolidatedBinary(start, n, sliceValidity, acquiredBuffers);
    }

    /**
     * Copies the slice's variable-length value bytes into a buffer the decode valve hands out (a native segment while
     * the off-heap decode budget has room, an mmap of an on-disk file otherwise) and rebases the row offsets to that
     * window. The bytes come straight from the live decompressed page for the direct path, or in one contiguous run
     * from the frozen page backing for DELTA_BYTE_ARRAY and surviving-rows pages. The owning batch closes the buffer on
     * {@link io.tileverse.parquetry.batch.ParquetRecordBatch#close()}; the resulting vector holds no heap value bytes,
     * only the offsets and validity.
     */
    private BinaryVector sliceConsolidatedBinary(
            int start, int n, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        int base = pageBinaryOffsets[start];
        long windowBytes = (long) pageBinaryOffsets[start + n] - base;
        SegmentPool.Pooled pooled = decodeBufferAllocator.acquireMandatory(Math.max(1L, windowBytes));
        acquiredBuffers.add(pooled);
        // The valve never hands out a zero-length buffer; slice back to the true window so an all-null slice is empty.
        MemorySegment dst = pooled.segment().asSlice(0L, windowBytes);
        if (pageBackingIsLivePage) {
            copyRowsFromLivePage(start, n, dst);
        } else if (windowBytes > 0) {
            MemorySegment.copy(pageBinaryBacking, base, dst, 0L, windowBytes);
        }
        int[] rebasedOffsets = new int[n + 1];
        for (int i = 0; i <= n; i++) {
            rebasedOffsets[i] = pageBinaryOffsets[start + i] - base;
        }
        return BinaryVector.of(dst, rebasedOffsets, sliceValidity);
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
            int len = pageBinaryOffsets[row + 1] - pageBinaryOffsets[row];
            if (len > 0) {
                MemorySegment.copy(pageBinaryBacking, pageValuePos[row], dst, writePos, len);
                writePos += len;
            }
        }
    }

    /**
     * Copies the page's fixed-width binary slots for the slice into a buffer the decode valve hands out (a native
     * segment while the off-heap decode budget has room, an mmap of an on-disk file otherwise). The owning batch closes
     * the buffer on {@link io.tileverse.parquetry.batch.ParquetRecordBatch#close()}. The dictionary branch reuses
     * shared heap entries and acquires nothing.
     */
    private FixedLenBinaryVector sliceFixedBinary(
            int start, int n, int width, Validity sliceValidity, List<AutoCloseable> acquiredBuffers) {
        if (isDictionaryBinaryPage()) {
            int[] sliceIndices = Arrays.copyOfRange(pageIndices, start, start + n);
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
            int[] sliceIndices = Arrays.copyOfRange(pageIndices, start, start + n);
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
