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

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Selection;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.read.ProjectedLeaves.ProjectedLeaf;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Per-row-group driver that emits {@link ParquetRecordBatch} instances.
 *
 * <p>Owns one {@link BatchColumnReader} per projected leaf. Each {@link #nextBatch()} call:
 *
 * <ol>
 *   <li>Computes the batch's logical row count as {@code min(logicalRowsRemainingInCurrentPage across all columns,
 *       batchSizeCap)}.
 *   <li>For each column derives the per-leaf value count covering that logical-row range and asks the reader for a
 *       vector at that value count, capturing the matching slices of the rep-level and def-level streams.
 *   <li>Feeds the per-leaf vectors and rep-/def-level slices to
 *       {@link NestedVectorAssembler#assembleNested(ParquetSchema, Map, Map, Map, int)} which produces
 *       {@link io.tileverse.parquetry.columnar.ListVector} / {@link io.tileverse.parquetry.columnar.MapVector} /
 *       {@link io.tileverse.parquetry.columnar.StructVector} wrappers at the LIST / MAP / STRUCT group paths.
 * </ol>
 *
 * <p>Most vectors are heap-backed at construction (see {@link BatchColumnReader#readBatch}); DOUBLE columns are decoded
 * into batch-owned off-heap buffers acquired through the decode allocator. The batch holds no Arena state across thread
 * boundaries.
 */
public final class BatchRowGroupReader implements AutoCloseable {

    private final ParquetSchema projectedSchema;
    private final ParquetSchema batchSchema;
    private final OptionalInt batchSizeCap;
    private final List<ProjectedLeaf> projectedLeaves;
    private final Optional<RowMask> rowMask;
    private final BatchForm batchForm;
    private final DecodeBufferAllocator decodeBufferAllocator;
    private final Optional<RowPositionSynthesis> rowPosition;
    private final LevelAssemblyPlans levelAssemblyPlans;

    // Lazily built on first nextBatch() call; keyed by column path in declaration order.
    private Map<ColumnPath, BatchColumnReader> columnReaders;

    private long rowsProducedTotal;

    public BatchRowGroupReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull List<FetchedColumnChunk> chunks,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Optional<RowMask> rowMask,
            @NonNull BatchForm batchForm) {
        this(
                decodeBufferAllocator,
                chunks,
                projectedSchema,
                fileSchema,
                batchSizeCap,
                rowMask,
                batchForm,
                Optional.empty());
    }

    /**
     * Builds a reader that synthesizes the absolute row-position column when {@code rowPosition} is present, the row
     * group's file row offset paired with the caller-chosen column name(s). Each emitted batch then includes a non-null
     * {@code long} at each named path; the value reports the row's true position in the file even when the row mask
     * dropped earlier rows.
     */
    @SuppressWarnings("java:S107") // the decode inputs plus the batch form and the row-position synthesis; a parameter
    // object would only relocate the arity
    public BatchRowGroupReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull List<FetchedColumnChunk> chunks,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Optional<RowMask> rowMask,
            @NonNull BatchForm batchForm,
            @NonNull Optional<RowPositionSynthesis> rowPosition) {
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.projectedSchema = projectedSchema;
        this.batchSizeCap = batchSizeCap;
        this.projectedLeaves = ProjectedLeaves.resolve(chunks, fileSchema);
        this.rowMask = rowMask;
        this.batchForm = batchForm;
        this.rowPosition = rowPosition;
        this.levelAssemblyPlans = new LevelAssemblyPlans(projectedSchema);
        this.batchSchema = rowPosition
                .map(synthesis -> projectedSchema.withAppendedLeaves(RowPositionVectors.leaves(synthesis.columns())))
                .orElse(projectedSchema);
    }

    /**
     * Returns true while any column reader still has rows. Before the first batch is read, returns true if there are
     * projected columns to read from.
     */
    public boolean hasMore() {
        if (columnReaders == null) {
            return !projectedLeaves.isEmpty();
        }
        for (BatchColumnReader reader : columnReaders.values()) {
            if (reader.hasMore()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Builds and returns the next batch. The row count is the minimum of
     * {@link BatchColumnReader#logicalRowsRemainingInCurrentPage()} across all columns (further capped by
     * {@code batchSizeCap} if present). Each column reader emits a vector at the corresponding leaf-value count; those
     * vectors and their rep-level slices feed {@link NestedVectorAssembler#assembleNested} which produces
     * {@link io.tileverse.parquetry.columnar.ListVector} / {@link io.tileverse.parquetry.columnar.MapVector} /
     * {@link io.tileverse.parquetry.columnar.StructVector} wrappers at LIST / MAP / STRUCT group paths. Leaf vectors
     * descended from a LIST or MAP group are removed from the top-level columns map because they are sized at element
     * granularity, not logical-row granularity.
     *
     * @throws IllegalStateException if called when {@link #hasMore()} returns false
     */
    @MustBeClosed
    public ParquetRecordBatch nextBatch() {
        if (!hasMore()) {
            throw new IllegalStateException("No more rows to read");
        }
        // Shared arena: vectors are heap-backed, but the batch may be decoded on a worker thread
        // and closed by the consumer thread, so its arena must not be confined to a single thread.
        Arena batchArena = Arena.ofShared();
        List<AutoCloseable> acquiredBuffers = new ArrayList<>();
        try {
            ensureColumnReadersBuilt();
            int batchRows = computeBatchRows();
            // Zero rows while hasMore() means a repeated column's page holds no complete row: its last-started
            // row's values continue in the next page. That row becomes a one-row batch read across the boundary.
            boolean spanningRow = batchRows == 0 && rowMask.isEmpty();
            if (spanningRow) {
                batchRows = 1;
            }
            long firstRowDenseIndex = rowsProducedTotal;
            rowsProducedTotal += batchRows;
            Map<ColumnPath, LevelSlice> repLevelsByLeaf = new HashMap<>();
            Map<ColumnPath, LevelSlice> defLevelsByLeaf = new HashMap<>();
            Map<ColumnPath, ColumnVector> leafVectors = spanningRow
                    ? readSpanningVectors(repLevelsByLeaf, defLevelsByLeaf, acquiredBuffers)
                    : readVectors(batchRows, repLevelsByLeaf, defLevelsByLeaf, acquiredBuffers);
            Map<ColumnPath, ColumnVector> assembled =
                    assembleGroups(leafVectors, repLevelsByLeaf, defLevelsByLeaf, batchRows, acquiredBuffers);
            Map<ColumnPath, ColumnVector> vectors = withRowPosition(assembled, firstRowDenseIndex, batchRows);
            DefaultParquetRecordBatch batch =
                    new DefaultParquetRecordBatch(batchSchema, vectors, batchRows, batchArena);
            for (AutoCloseable buffer : acquiredBuffers) {
                batch.registerBuffer(buffer);
            }
            return batch;
        } catch (RuntimeException e) {
            closeQuietly(acquiredBuffers);
            batchArena.close();
            throw e;
        }
    }

    // --- synthesized row-position column ---

    /**
     * Adds the synthesized absolute row-position column when row-position synthesis is on, leaving {@code vectors}
     * unchanged otherwise. The added vector reports each row's true file position even when the row mask dropped
     * earlier rows: it maps the batch's {@code batchRows} logical rows, beginning at the row group's
     * {@code firstRowDenseIndex}-th emitted row, to their row-group-relative positions and offsets each by the row
     * group's file base.
     */
    private Map<ColumnPath, ColumnVector> withRowPosition(
            Map<ColumnPath, ColumnVector> vectors, long firstRowDenseIndex, int batchRows) {
        if (rowPosition.isEmpty()) {
            return vectors;
        }
        Selection positions = RowPositionVectors.window(rowMask, firstRowDenseIndex, batchRows);
        return RowPositionVectors.add(vectors, rowPosition.orElseThrow(), positions, batchRows);
    }

    /**
     * Wraps the batch's row-aligned LIST and MAP groups in this read's {@link BatchForm}, assembling from the metadata
     * this reader resolved for the batch's leaves.
     */
    private Map<ColumnPath, ColumnVector> assembleGroups(
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevelsByLeaf,
            Map<ColumnPath, LevelSlice> defLevelsByLeaf,
            int batchRows,
            List<AutoCloseable> acquiredBuffers) {
        return BatchAssembly.assemble(
                batchForm,
                levelAssemblyPlans,
                projectedSchema,
                leafVectors,
                repLevelsByLeaf,
                defLevelsByLeaf,
                batchRows,
                decodeBufferAllocator,
                acquiredBuffers);
    }

    /** Closes the buffers acquired for a batch that failed to assemble, in reverse order, best-effort. */
    private static void closeQuietly(List<AutoCloseable> acquiredBuffers) {
        for (int i = acquiredBuffers.size() - 1; i >= 0; i--) {
            try {
                acquiredBuffers.get(i).close();
            } catch (Exception _) {
                // best-effort release; one failure must not strand the others
            }
        }
    }

    /**
     * Closes all column readers, releasing any page Arenas they still hold.
     *
     * <p>Must be called when done with the row group, whether rows were fully consumed or the consumer broke early.
     * Without this call, the last page's Arena in each column reader would leak (it is only closed on the next
     * {@code loadNextPage} call, which never comes if the reader is abandoned).
     *
     * <p>Each column reader's {@link BatchColumnReader#close()} is invoked inside a try/catch so that one failing close
     * does not leave the remaining readers unclosed.
     */
    @Override
    public void close() {
        if (columnReaders == null) {
            return;
        }
        RuntimeException firstFailure = null;
        for (BatchColumnReader reader : columnReaders.values()) {
            try {
                reader.close();
            } catch (RuntimeException e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
            }
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
    }

    // --- page-count summary ---

    /**
     * Sums the data pages decoded and skipped across every column reader, for the read observability callback. Returns
     * zeros before the first batch is pulled (no column reader has been built yet).
     */
    public PageCounts pageCounts() {
        if (columnReaders == null) {
            return PageCounts.ZERO;
        }
        int decoded = 0;
        int skipped = 0;
        for (BatchColumnReader reader : columnReaders.values()) {
            decoded += reader.decodedDataPageCount();
            skipped += reader.skippedDataPageCount();
        }
        return new PageCounts(decoded, skipped);
    }

    /** Data pages decoded and skipped across a row group's column readers. */
    public record PageCounts(int decoded, int skipped) {
        static final PageCounts ZERO = new PageCounts(0, 0);
    }

    /**
     * Logical rows this reader has decoded into batches, for the read observability callback. Counts batch rows as they
     * are produced, never the row group's metadata row count; an abandoned reader reports only what it decoded.
     */
    public long rowsProduced() {
        return rowsProducedTotal;
    }

    // --- column reader construction ---

    private void ensureColumnReadersBuilt() {
        if (columnReaders != null) {
            return;
        }
        Map<ColumnPath, BatchColumnReader> readers = new HashMap<>();
        for (ProjectedLeaf leaf : projectedLeaves) {
            readers.put(leaf.path(), buildColumnReader(leaf));
        }
        columnReaders = readers;
    }

    private BatchColumnReader buildColumnReader(ProjectedLeaf leaf) {
        if (rowMask.isEmpty()) {
            return new BatchColumnReader(decodeBufferAllocator, leaf.chunk(), leaf.leaf());
        }
        RowMask mask = rowMask.orElseThrow();
        OffsetIndex offsetIndex = mask.offsetIndexes().get(leaf.path());
        return new BatchColumnReader(
                decodeBufferAllocator, leaf.chunk(), leaf.leaf(), mask.survivingRows(), offsetIndex);
    }

    // --- batch row count computation ---

    /**
     * Computes the logical row count for the next batch as the minimum of
     * {@link BatchColumnReader#logicalRowsRemainingInCurrentPage()} across all columns, further capped by
     * {@code batchSizeCap} if present. For repeated columns the leaf-value count exceeds the logical-row count by the
     * number of list / map elements within each row; the driver reasons in logical rows rather than leaf values. A zero
     * result while rows remain means some repeated column deferred its page's last-started row (the row may continue in
     * the next page); {@link #nextBatch()} then reads that row as a one-row page-spanning batch.
     */
    private int computeBatchRows() {
        int min = Integer.MAX_VALUE;
        for (BatchColumnReader reader : columnReaders.values()) {
            min = Math.min(min, reader.logicalRowsRemainingInCurrentPage());
        }
        if (batchSizeCap.isPresent()) {
            min = Math.min(min, batchSizeCap.getAsInt());
        }
        return min;
    }

    // --- vector reads ---

    /**
     * Asks each column reader for a vector covering {@code batchLogicalRows} logical rows. For repeated columns the
     * actual leaf-value count is derived via {@link BatchColumnReader#valuesForLogicalRows(int)} and the matching
     * slices of the rep-level and def-level streams are windowed into {@code repLevelsByLeafOut} /
     * {@code defLevelsByLeafOut} as views over the page level sequences before the reader advances.
     */
    private Map<ColumnPath, ColumnVector> readVectors(
            int batchLogicalRows,
            Map<ColumnPath, LevelSlice> repLevelsByLeafOut,
            Map<ColumnPath, LevelSlice> defLevelsByLeafOut,
            List<AutoCloseable> acquiredBuffers) {
        Map<ColumnPath, ColumnVector> vectors = new HashMap<>();
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : columnReaders.entrySet()) {
            BatchColumnReader reader = entry.getValue();
            int valuesThisBatch = reader.valuesForLogicalRows(batchLogicalRows);
            int start = reader.valuesConsumedInCurrentPage();
            Levels pageRepLevels = reader.currentPageRepLevels();
            if (pageRepLevels != null) {
                repLevelsByLeafOut.put(entry.getKey(), new LevelSlice(pageRepLevels, start, valuesThisBatch));
            }
            Levels pageDefLevels = reader.currentPageDefLevels();
            if (pageDefLevels != null) {
                defLevelsByLeafOut.put(entry.getKey(), new LevelSlice(pageDefLevels, start, valuesThisBatch));
            }
            ColumnVector vec = reader.readBatch(valuesThisBatch, acquiredBuffers);
            vectors.put(entry.getKey(), vec);
        }
        return vectors;
    }

    /**
     * Reads exactly one logical row from every column for the batch that crosses a data-page boundary: at least one
     * repeated column's remaining page window holds no complete row, and every column advances one row - across pages
     * where needed - through {@link BatchColumnReader#readSpanningRow} to stay in lockstep. The level windows come back
     * as reader-owned copies (a cross-page read decodes the next page, overwriting the scratch the normal windows
     * view), wrapped whole for the assemblers.
     */
    private Map<ColumnPath, ColumnVector> readSpanningVectors(
            Map<ColumnPath, LevelSlice> repLevelsByLeafOut,
            Map<ColumnPath, LevelSlice> defLevelsByLeafOut,
            List<AutoCloseable> acquiredBuffers) {
        Map<ColumnPath, ColumnVector> vectors = new HashMap<>();
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : columnReaders.entrySet()) {
            BatchColumnReader.SpanningRow row = entry.getValue().readSpanningRow(acquiredBuffers);
            if (row.repLevels() != null) {
                repLevelsByLeafOut.put(entry.getKey(), LevelSlice.ofWhole(row.repLevels()));
            }
            if (row.defLevels() != null) {
                defLevelsByLeafOut.put(entry.getKey(), LevelSlice.ofWhole(row.defLevels()));
            }
            vectors.put(entry.getKey(), row.vector());
        }
        return vectors;
    }
}
