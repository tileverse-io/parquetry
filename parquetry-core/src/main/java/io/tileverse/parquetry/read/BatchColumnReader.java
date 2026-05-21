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
package io.tileverse.parquetry.read;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Optional;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.codec.Codec;
import io.tileverse.parquetry.codec.CodecRegistry;
import io.tileverse.parquetry.format.Encoding;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.page.Dictionary;
import io.tileverse.parquetry.page.LevelDecoder;
import io.tileverse.parquetry.read.LevelMaximaResolver.LevelMaxima;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.Field;

import lombok.NonNull;

/**
 * Per-column reader for the batch API. Fills one {@link ColumnVector} per call to {@link #readBatch(int)} from a
 * {@link FetchedColumnChunk}'s compressed byte stream.
 *
 * <p>Each call to {@link #readBatch(int)} is <em>page-bounded</em>: the returned vector contains at most
 * {@code min(maxRows, remainingInCurrentPage)} rows. The caller is responsible for aligning batches across columns by
 * first asking {@link #rowsRemainingInCurrentPage()} and capping the {@code maxRows} argument accordingly. This is the
 * contract {@code BatchRowGroupReader} enforces.
 *
 * <p>Pages are decoded lazily: the current page is not decompressed until the first {@link #readBatch} or
 * {@link #rowsRemainingInCurrentPage} call that requires it. Each page is decompressed into a fresh
 * {@link Arena#ofConfined() confined Arena} that this reader owns. The previous page's Arena is closed when the next
 * page is loaded (in {@link #loadNextPage()}), at which point callers should no longer hold unmaterialized vector
 * references from the previous page. Callers that need values to outlive the page should call
 * {@link ColumnVector#materialize()} to copy values onto the heap before the reader advances.
 *
 * <p>The constructor's {@code arena} parameter is accepted for API compatibility but is not used for page allocation;
 * the reader always allocates its own per-page Arena. Use {@link #close()} when done to release any Arenas still held.
 *
 * <p>Dictionary-encoded columns require a {@code Dictionary<?>} wired into each {@code ColumnVector}'s factory method.
 * Dictionary-encoded pages are decoded via {@link io.tileverse.parquetry.page.RleDictionaryPageDecoder}.
 */
final class BatchColumnReader {

    private final ColumnPath columnPath;
    private final Field.Primitive leaf;
    private final LevelMaxima maxLevels;
    private final Codec codec;
    private final long totalValues;
    private final PageCursor pageCursor;
    private final int defBitWidth;
    private final Optional<Dictionary<?>> dictionary;

    private DecodedPage currentPage;
    // The Arena that owns the current page's decompressed bytes. Created per page in loadNextPage().
    // Closed when the next page is loaded (at which point all vectors from this page should be consumed).
    private Arena currentPageArenaRef;
    // Arena for the previous page; held until the next loadNextPage() call closes it.
    private Arena previousPageArenaRef;
    private int rowsConsumedInCurrentPage;
    private long rowsConsumedTotal;

    /**
     * Constructs a {@code BatchColumnReader} from a pre-fetched column chunk.
     *
     * <p>{@code leaf} is required separately because {@link FetchedColumnChunk} does not carry the schema leaf; the
     * caller resolves it from the file schema before constructing the reader.
     *
     * @param chunk the fetched, compressed column chunk to read
     * @param leaf the file-schema leaf for this column; used to determine the primitive kind and optional type length
     * @param arena unused; kept for API compatibility with existing tests. The reader allocates its own Arena per page.
     */
    BatchColumnReader(@NonNull FetchedColumnChunk chunk, @NonNull Field.Primitive leaf, @NonNull Arena arena) {
        this.columnPath = chunk.path();
        this.leaf = leaf;
        this.maxLevels = new LevelMaxima(chunk.maxRepetitionLevel(), chunk.maxDefinitionLevel());
        this.codec = CodecRegistry.lookup(chunk.metadata().codec());
        this.totalValues = chunk.metadata().numValues();
        this.pageCursor = new PageCursor(chunk.compressedBuffer().buffer(), columnPath);
        this.defBitWidth = LevelDecoder.computeBitWidth(maxLevels.maxDefinitionLevel());
        this.dictionary = chunk.dictionary();
    }

    /**
     * Returns {@code true} while there are rows remaining in the current page or the page cursor has more pages. A
     * {@code true} result means the next {@link #readBatch} call will return a non-empty vector.
     */
    boolean hasMore() {
        if (rowsConsumedTotal < totalValues) {
            return true;
        }
        // If a page is loaded and has unconsumed rows, we still have more (handles the case where totalValues
        // is not set precisely but the page stream has data).
        return currentPage != null && rowsConsumedInCurrentPage < currentPage.valueCount();
    }

    /**
     * Returns the number of rows remaining in the current page. Loads the page if it has not been loaded yet.
     *
     * <p>Used by {@code BatchRowGroupReader} to compute the cross-column page-boundary cap: all columns share the same
     * page-row-count constraint, so the driver calls this on each column and takes the minimum.
     *
     * @throws IllegalStateException if there are no more rows to read
     */
    int rowsRemainingInCurrentPage() {
        ensurePageLoaded();
        return currentPage.valueCount() - rowsConsumedInCurrentPage;
    }

    /**
     * Reads up to {@code maxRows} rows from the current page into a new {@link ColumnVector}. The actual row count is
     * {@code min(maxRows, rowsRemainingInCurrentPage())} so the batch never straddles a page boundary on this column.
     *
     * <p>Callers that want aligned page-boundary batches across all columns should call
     * {@link #rowsRemainingInCurrentPage()} first and pass the minimum as {@code maxRows}.
     *
     * <p>The returned vector holds a lazy view of the decompressed page bytes in the current page's Arena. The Arena is
     * closed when the page is exhausted (all rows consumed) or when the reader advances to the next page. Callers must
     * not hold vector references past the point where the reader advances; for safety, materialize the vector (via
     * {@link ColumnVector#materialize()}) before the batch closes if you need the values to outlive the page.
     *
     * <p><strong>Known limitation:</strong> when {@code maxRows} is less than the current page's remaining row count,
     * subsequent batches that continue reading from the same page will produce incorrect values - the second batch's
     * vector starts decoding from byte 0 of the page again. The page-aligned batch model used by the production caller
     * ({@link BatchRowGroupReader}) avoids this by default: it only emits within-page splits when a
     * {@code batchSizeCap} is explicitly set. A follow-up will add row-offset-aware vector materialization so the cap
     * can split pages safely.
     *
     * @param maxRows maximum number of rows to include in the batch; must be positive
     * @return a {@link ColumnVector} carrying at most {@code maxRows} rows
     * @throws IllegalArgumentException if {@code maxRows} is not positive
     * @throws IllegalStateException if there are no more rows to read
     * @throws UncheckedIOException if a page decompression I/O error occurs
     */
    ColumnVector readBatch(int maxRows) {
        if (maxRows <= 0) {
            throw new IllegalArgumentException("maxRows must be positive; got " + maxRows);
        }
        ensurePageLoaded();
        if (rowsConsumedInCurrentPage >= currentPage.valueCount()) {
            throw new IllegalStateException(
                    "No more rows in column " + columnPath.dot() + " (totalValues=" + totalValues + ")");
        }
        int rowsThisBatch = Math.min(maxRows, currentPage.valueCount() - rowsConsumedInCurrentPage);
        BitSet validity = decodeValidity(rowsThisBatch);
        ColumnVector vec = buildVector(rowsThisBatch, validity);
        rowsConsumedInCurrentPage += rowsThisBatch;
        rowsConsumedTotal += rowsThisBatch;
        if (rowsConsumedInCurrentPage >= currentPage.valueCount()) {
            advancePastCurrentPage();
        }
        return vec;
    }

    // --- page management ---

    private void ensurePageLoaded() {
        if (currentPage == null) {
            loadNextPage();
        }
    }

    private void loadNextPage() {
        // Close the previous page's Arena if it is still open. At this point, all batches from the
        // previous page should have been consumed by the caller (we're advancing to the next page because
        // the reader has no more rows in the current page). Any vectors from the previous page that the
        // caller still holds will have stale MemorySegment references after this point.
        if (previousPageArenaRef != null) {
            previousPageArenaRef.close();
            previousPageArenaRef = null;
        }
        // Each page gets its own confined Arena so its decompressed bytes outlive individual batch Arenas.
        Arena pageArena = Arena.ofConfined();
        DecodedPage next;
        try {
            next = pageCursor.nextDataPage(maxLevels, codec, pageArena);
        } catch (IOException e) {
            pageArena.close();
            throw new UncheckedIOException("Failed to read next page for column " + columnPath.dot(), e);
        }
        if (next == null) {
            pageArena.close();
            throw new MalformedFileException("Column " + columnPath.dot() + " exhausted page stream after "
                    + rowsConsumedTotal + " values; expected " + totalValues);
        }
        currentPage = next;
        currentPageArenaRef = pageArena;
        rowsConsumedInCurrentPage = 0;
    }

    /**
     * Records that the current page is fully consumed and saves the page Arena reference for deferred closure. The
     * Arena is closed when the NEXT page is loaded (in {@link #loadNextPage()}), at which point the caller should have
     * stopped using vectors from this page. Sets {@code currentPage} to {@code null} so that the next
     * {@link #ensurePageLoaded} call loads the following page.
     */
    private void advancePastCurrentPage() {
        // Park the old Arena so loadNextPage() closes it when moving to the next page.
        previousPageArenaRef = currentPageArenaRef;
        currentPage = null;
        currentPageArenaRef = null;
        rowsConsumedInCurrentPage = 0;
    }

    // --- def-level decoding ---

    /**
     * Decodes {@code rows} definition level values from the current page and returns a validity {@link BitSet} where
     * bit {@code i} is set iff row {@code i} is non-null (i.e. its def level equals {@code maxDefinitionLevel}).
     *
     * <p>When {@code maxDefinitionLevel == 0} (required / non-nullable column), all bits are set without reading any
     * bytes, matching the {@code MemorySegment.NULL} def-level segment that {@link DecodedPage} uses for required
     * columns.
     */
    private BitSet decodeValidity(int rows) {
        BitSet validity = new BitSet(rows);
        int maxDef = maxLevels.maxDefinitionLevel();
        if (maxDef == 0) {
            validity.set(0, rows);
            return validity;
        }
        LevelDecoder defDecoder = new LevelDecoder(defBitWidth);
        MemorySegment defBytes = currentPage.defLevelBytes();
        ByteBuffer defBuf = (defBytes == MemorySegment.NULL)
                ? ByteBuffer.allocate(0).order(LITTLE_ENDIAN)
                : defBytes.asByteBuffer().order(LITTLE_ENDIAN);
        defDecoder.load(defBuf);
        int[] defLevels = new int[rows];
        defDecoder.decode(rows, defLevels, 0);
        for (int i = 0; i < rows; i++) {
            if (defLevels[i] == maxDef) {
                validity.set(i);
            }
        }
        return validity;
    }

    // --- vector construction ---

    /**
     * Builds the typed {@link ColumnVector} for the current page, dispatching on the leaf's primitive kind. The
     * {@code valueBytes} segment is a view into the Arena-allocated decompressed page; it remains valid as long as the
     * Arena is open.
     *
     * <p>The chunk's dictionary (if present) is forwarded to every vector factory so that dictionary-encoded pages can
     * be materialized via {@link io.tileverse.parquetry.page.RleDictionaryPageDecoder}.
     */
    private ColumnVector buildVector(int rows, BitSet validity) {
        MemorySegment valueBytes = currentPage.valueBytes();
        Encoding encoding = currentPage.valuesEncoding();
        Dictionary<?> dict = dictionary.orElse(null);
        return switch (leaf.kind()) {
            case INT32 -> IntVector.raw(valueBytes, encoding, rows, validity, dict);
            case INT64 -> LongVector.raw(valueBytes, encoding, rows, validity, dict);
            case FLOAT -> FloatVector.raw(valueBytes, encoding, rows, validity, dict);
            case DOUBLE -> DoubleVector.raw(valueBytes, encoding, rows, validity, dict);
            case BOOLEAN -> BooleanVector.raw(valueBytes, encoding, rows, validity);
            case BYTE_ARRAY -> BinaryVector.raw(valueBytes, encoding, rows, validity, dict);
            case FIXED_LEN_BYTE_ARRAY -> buildFixedLenBinaryVector(valueBytes, encoding, rows, validity, dict);
            case INT96 -> Int96Vector.raw(valueBytes, encoding, rows, validity, dict);
        };
    }

    private ColumnVector buildFixedLenBinaryVector(
            MemorySegment valueBytes, Encoding encoding, int rows, BitSet validity, Dictionary<?> dict) {
        int byteWidth = leaf.typeLength()
                .orElseThrow(() -> new IllegalStateException(
                        "FIXED_LEN_BYTE_ARRAY column " + columnPath.dot() + " is missing typeLength in schema"));
        return FixedLenBinaryVector.raw(valueBytes, encoding, rows, byteWidth, validity, dict);
    }

    /**
     * Releases any page Arenas still held by this reader. Called by {@link BatchRowGroupReader} when the row group is
     * fully consumed (no more batches). After this call, any vectors still held by the caller that reference
     * Arena-backed memory will be invalid.
     */
    void close() {
        if (currentPageArenaRef != null) {
            currentPageArenaRef.close();
            currentPageArenaRef = null;
        }
        if (previousPageArenaRef != null) {
            previousPageArenaRef.close();
            previousPageArenaRef = null;
        }
        currentPage = null;
    }

    /** Returns the column path this reader serves. Used for diagnostics and tests. */
    ColumnPath columnPath() {
        return columnPath;
    }

    /** Returns the optional dictionary for this chunk. */
    Optional<Dictionary<?>> dictionary() {
        return dictionary;
    }
}
