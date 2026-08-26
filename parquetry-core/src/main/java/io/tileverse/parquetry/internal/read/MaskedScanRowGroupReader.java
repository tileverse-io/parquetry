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
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.Compaction;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.VectorizedPredicateEvaluator;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.format.OffsetIndex;
import io.tileverse.parquetry.internal.read.ProjectedLeaves.ProjectedLeaf;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Single-pass filtering reader for one row group: it walks the row group in windows, evaluates the predicate over each
 * window's filter columns, and materializes the output columns only for the rows that survive.
 *
 * <p>A window is the run of rows every column reader can serve from the page it stands on, capped by the read's batch
 * size. Within a window the two column groups play different parts:
 *
 * <ul>
 *   <li>The predicate's leaf columns decode their window whole, feed the predicate evaluator, and - where the output
 *       projects them too - are gathered to the surviving rows, never decoded twice.
 *   <li>An output-only leaf decodes only the value slots the surviving rows cover, stepping its page decoder over the
 *       rest.
 * </ul>
 *
 * <p>A window whose rows all fail the predicate emits nothing: its output columns are stepped over without decoding a
 * value. Each emitted batch holds the window's survivors, dense and in row order, over the caller's output schema.
 */
final class MaskedScanRowGroupReader implements AutoCloseable {

    private final DecodeBufferAllocator decodeBufferAllocator;
    private final ParquetSchema filterSchema;
    private final ParquetSchema outputSchema;
    private final Predicate predicate;
    private final OptionalInt batchSizeCap;
    private final Optional<RowMask> rowMask;
    private final BatchForm outputForm;
    private final long expectedRows;
    private final LevelAssemblyPlans filterPlans;
    private final LevelAssemblyPlans outputPlans;

    private final Map<ColumnPath, BatchColumnReader> filterReaders = new HashMap<>();
    private final Map<ColumnPath, BatchColumnReader> outputReaders = new HashMap<>();
    // Both groups in one list, for the walks that treat every scanned column alike: window sizing, the drain check,
    // the page tally and close.
    private final List<BatchColumnReader> scanReaders = new ArrayList<>();
    // The leaves the predicate and the output share, gathered from the window rather than read a second time.
    private final List<ColumnPath> sharedLeaves = new ArrayList<>();

    private ParquetRecordBatch pending;
    private long rowsDecodedTotal;
    private long rowsMatchedTotal;
    private long recordFilterNanos;

    /**
     * @param filterLeaves the physical leaf columns the predicate reads
     * @param expectedRows the rows this scan must cover: the row group's row count, or the count of surviving rows when
     *     a row mask narrows the readers' row space
     */
    @SuppressWarnings("java:S107") // the decode inputs plus the scan's predicate and form; a parameter object would
    // only relocate the arity
    MaskedScanRowGroupReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull List<FetchedColumnChunk> chunks,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema outputSchema,
            @NonNull Set<ColumnPath> filterLeaves,
            @NonNull Predicate predicate,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Optional<RowMask> rowMask,
            @NonNull BatchForm outputForm,
            long expectedRows) {
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.outputSchema = outputSchema;
        this.predicate = predicate;
        this.batchSizeCap = batchSizeCap;
        this.rowMask = rowMask;
        this.outputForm = outputForm;
        this.expectedRows = expectedRows;
        this.filterSchema = fileSchema.project(filterLeaves);
        this.filterPlans = new LevelAssemblyPlans(filterSchema);
        this.outputPlans = new LevelAssemblyPlans(outputSchema);
        buildReaders(ProjectedLeaves.resolve(chunks, fileSchema), filterLeaves, outputSchema.leafColumns());
    }

    /**
     * Builds one reader per scanned leaf. A predicate leaf materializes its pages as they load, an output-only leaf
     * defers its values to the windows that keep rows, and a leaf the read fetched for neither is never decoded. Under
     * a row mask both groups take the same surviving rows, which leaves them serving one row space: row {@code j} of a
     * filter reader and row {@code j} of an output reader are the same row of the row group.
     */
    private void buildReaders(List<ProjectedLeaf> leaves, Set<ColumnPath> filterLeaves, List<ColumnPath> outputLeaves) {
        for (ProjectedLeaf leaf : leaves) {
            if (filterLeaves.contains(leaf.path())) {
                filterReaders.put(leaf.path(), buildReader(leaf, ValueDecode.EAGER));
                if (outputLeaves.contains(leaf.path())) {
                    sharedLeaves.add(leaf.path());
                }
            } else if (outputLeaves.contains(leaf.path())) {
                outputReaders.put(leaf.path(), buildReader(leaf, ValueDecode.WINDOWED_MASK));
            }
        }
        scanReaders.addAll(filterReaders.values());
        scanReaders.addAll(outputReaders.values());
    }

    private BatchColumnReader buildReader(ProjectedLeaf leaf, ValueDecode valueDecode) {
        RowRanges survivingRows = rowMask.isPresent() ? rowMask.get().survivingRows() : null;
        OffsetIndex offsetIndex =
                rowMask.isPresent() ? rowMask.get().offsetIndexes().get(leaf.path()) : null;
        return new BatchColumnReader(
                decodeBufferAllocator, leaf.chunk(), leaf.leaf(), survivingRows, offsetIndex, valueDecode);
    }

    /**
     * Returns true while a surviving batch is available, computing it on demand. Windows whose rows all fail the
     * predicate are consumed here and emit nothing, which keeps empty batches out of the hand-off.
     */
    boolean hasMore() {
        if (pending != null) {
            return true;
        }
        pending = computeNextSurvivingBatch();
        return pending != null;
    }

    @MustBeClosed
    ParquetRecordBatch nextBatch() {
        if (!hasMore()) {
            throw new IllegalStateException("No more rows to read");
        }
        ParquetRecordBatch batch = pending;
        pending = null;
        return batch;
    }

    /** Pulls windows until one has survivors, or returns null once the row group is drained. */
    private ParquetRecordBatch computeNextSurvivingBatch() {
        while (moreRowsRemain()) {
            Window window = readAndEvaluateWindow();
            if (window.survivorCount() > 0) {
                return emit(window);
            }
            try {
                skipOutputs(window);
            } finally {
                window.release();
            }
        }
        verifyRowsScanned();
        return null;
    }

    /** True while any scanned column still has rows to serve. */
    private boolean moreRowsRemain() {
        for (BatchColumnReader reader : scanReaders) {
            if (reader.hasMore()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Reads one window's filter columns and evaluates the predicate over them. The window is the rows every reader can
     * serve from its current page, capped by the batch size; a window of zero rows while rows remain means a repeated
     * column deferred its page's last-started row, which advances one row across the page boundary instead.
     */
    private Window readAndEvaluateWindow() {
        int windowRows = computeWindowRows();
        if (windowRows == 0) {
            return readAndEvaluateSpanningRow();
        }
        List<AutoCloseable> acquired = new ArrayList<>();
        Map<ColumnPath, ColumnVector> leafVectors = new HashMap<>();
        Map<ColumnPath, LevelSlice> repLevels = new HashMap<>();
        Map<ColumnPath, LevelSlice> defLevels = new HashMap<>();
        try {
            readFilterVectors(windowRows, leafVectors, repLevels, defLevels, acquired);
            BitSet survivors = evaluate(leafVectors, repLevels, defLevels, windowRows, acquired);
            rowsDecodedTotal += windowRows;
            return new Window(windowRows, Survivors.of(survivors), leafVectors, repLevels, defLevels, acquired, false);
        } catch (RuntimeException e) {
            closeQuietly(acquired);
            throw e;
        }
    }

    /** The window every reader can serve from its current page, capped by the read's batch size. */
    private int computeWindowRows() {
        int min = Integer.MAX_VALUE;
        for (BatchColumnReader reader : scanReaders) {
            min = Math.min(min, reader.logicalRowsRemainingInCurrentPage());
        }
        if (batchSizeCap.isPresent()) {
            min = Math.min(min, batchSizeCap.getAsInt());
        }
        return min;
    }

    /**
     * Reads the window's rows from every filter column, capturing each leaf's rep- and def-level windows as views over
     * the page level sequences before the reader advances.
     */
    private void readFilterVectors(
            int windowRows,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            List<AutoCloseable> acquired) {
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : filterReaders.entrySet()) {
            BatchColumnReader reader = entry.getValue();
            int windowValues = reader.valuesForLogicalRows(windowRows);
            int start = reader.valuesConsumedInCurrentPage();
            Levels pageRepLevels = reader.currentPageRepLevels();
            if (pageRepLevels != null) {
                repLevels.put(entry.getKey(), new LevelSlice(pageRepLevels, start, windowValues));
            }
            Levels pageDefLevels = reader.currentPageDefLevels();
            if (pageDefLevels != null) {
                defLevels.put(entry.getKey(), new LevelSlice(pageDefLevels, start, windowValues));
            }
            leafVectors.put(entry.getKey(), reader.readBatch(windowValues, acquired));
        }
    }

    /**
     * Reads the one row whose values outrun the page some column stands on, and evaluates the predicate over it. Every
     * column advances that row - across pages where needed - to stay in lockstep.
     */
    private Window readAndEvaluateSpanningRow() {
        requireUnmaskedSpanningRow();
        List<AutoCloseable> acquired = new ArrayList<>();
        Map<ColumnPath, ColumnVector> leafVectors = new HashMap<>();
        Map<ColumnPath, LevelSlice> repLevels = new HashMap<>();
        Map<ColumnPath, LevelSlice> defLevels = new HashMap<>();
        try {
            readFilterSpanningRow(leafVectors, repLevels, defLevels, acquired);
            BitSet survivors = evaluate(leafVectors, repLevels, defLevels, 1, acquired);
            rowsDecodedTotal++;
            return new Window(1, Survivors.of(survivors), leafVectors, repLevels, defLevels, acquired, true);
        } catch (RuntimeException e) {
            closeQuietly(acquired);
            throw e;
        }
    }

    /**
     * Fails a page-spanning row under a row mask. A row mask requires an offset index, a page index implies row-aligned
     * pages, and a masked reader therefore never defers a row past its page: a zero-row window there means the readers
     * disagree on how many rows the row group holds.
     */
    private void requireUnmaskedSpanningRow() {
        if (rowMask.isPresent()) {
            throw new MalformedFileException("A masked scan over row-aligned pages served a window of no rows after "
                    + rowsDecodedTotal + " rows");
        }
    }

    /**
     * Reads exactly one logical row from every filter column, following it across data-page boundaries. The level
     * windows come back as reader-owned copies, wrapped whole for the assemblers.
     */
    private void readFilterSpanningRow(
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            List<AutoCloseable> acquired) {
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : filterReaders.entrySet()) {
            BatchColumnReader.SpanningRow row = entry.getValue().readSpanningRow(acquired);
            if (row.repLevels() != null) {
                repLevels.put(entry.getKey(), LevelSlice.ofWhole(row.repLevels()));
            }
            if (row.defLevels() != null) {
                defLevels.put(entry.getKey(), LevelSlice.ofWhole(row.defLevels()));
            }
            leafVectors.put(entry.getKey(), row.vector());
        }
    }

    /** Evaluates the predicate over the window's filter batch, timed into the row group's record-filter tally. */
    private BitSet evaluate(
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            int windowRows,
            List<AutoCloseable> acquired) {
        long start = System.nanoTime();
        try (ParquetRecordBatch filterBatch = filterBatch(leafVectors, repLevels, defLevels, windowRows, acquired)) {
            return VectorizedPredicateEvaluator.eval(predicate, filterBatch);
        } finally {
            recordFilterNanos += System.nanoTime() - start;
        }
    }

    /**
     * The window's filter columns as a batch the predicate evaluator reads. It registers no buffer: the window owns the
     * decode buffers its vectors read, and closing this batch releases only its own arena.
     */
    @MustBeClosed
    private ParquetRecordBatch filterBatch(
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            int windowRows,
            List<AutoCloseable> acquired) {
        // Confined is enough: this batch never leaves the decode worker that builds it, and the same call closes it.
        // A shared arena would only add a close handshake per window.
        Arena batchArena = Arena.ofConfined();
        try {
            Map<ColumnPath, ColumnVector> vectors = BatchAssembly.assemble(
                    BatchForm.LEVELS,
                    filterPlans,
                    filterSchema,
                    leafVectors,
                    repLevels,
                    defLevels,
                    windowRows,
                    decodeBufferAllocator,
                    acquired);
            return new DefaultParquetRecordBatch(filterSchema, vectors, windowRows, batchArena);
        } catch (RuntimeException e) {
            batchArena.close();
            throw e;
        }
    }

    /** Closes decode buffers in reverse order, best-effort: one failing close must not strand the others. */
    private static void closeQuietly(List<AutoCloseable> buffers) {
        for (int i = buffers.size() - 1; i >= 0; i--) {
            try {
                buffers.get(i).close();
            } catch (Exception _) {
                // best-effort release; one failure must not strand the others
            }
        }
    }

    /** Builds the window's emitted batch: survivor-dense output leaves, in the read's batch form. */
    private ParquetRecordBatch emit(Window window) {
        Arena batchArena = Arena.ofShared();
        List<AutoCloseable> acquired = window.acquiredBuffers();
        try {
            int survivors = window.survivorCount();
            Map<ColumnPath, ColumnVector> leafVectors = new HashMap<>();
            Map<ColumnPath, LevelSlice> repLevels = new HashMap<>();
            Map<ColumnPath, LevelSlice> defLevels = new HashMap<>();
            gatherFilterLeaves(window, leafVectors, repLevels, defLevels);
            readOutputLeaves(window, leafVectors, repLevels, defLevels, acquired);
            Map<ColumnPath, ColumnVector> vectors = BatchAssembly.assemble(
                    outputForm,
                    outputPlans,
                    outputSchema,
                    leafVectors,
                    repLevels,
                    defLevels,
                    survivors,
                    decodeBufferAllocator,
                    acquired);
            DefaultParquetRecordBatch batch =
                    new DefaultParquetRecordBatch(outputSchema, vectors, survivors, batchArena);
            for (AutoCloseable buffer : acquired) {
                batch.registerBuffer(buffer);
            }
            rowsMatchedTotal += survivors;
            return batch;
        } catch (RuntimeException e) {
            window.release();
            batchArena.close();
            throw e;
        }
    }

    /**
     * Gathers the leaves the predicate and the output share to survivor space. Their values are already decoded for the
     * window, hence a surviving row's value is copied, never decoded a second time.
     */
    private void gatherFilterLeaves(
            Window window,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels) {
        BitSet survivors = window.survivors().bits();
        for (ColumnPath path : sharedLeaves) {
            LevelSlice repWindow = window.repLevels().get(path);
            int[] keep = (repWindow == null)
                    ? MaskedValues.flatSlots(0, window.rows(), survivors)
                    : MaskedValues.repeatedSlots(
                            0, repWindow.levels(), repWindow.from(), repWindow.length(), survivors);
            leafVectors.put(path, Compaction.compact(window.leafVectors().get(path), keep));
            if (repWindow != null) {
                repLevels.put(path, LevelSlice.ofWhole(repWindow.gather(keep)));
            }
            LevelSlice defWindow = window.defLevels().get(path);
            if (defWindow != null) {
                defLevels.put(path, LevelSlice.ofWhole(defWindow.gather(keep)));
            }
        }
    }

    /** Decodes each output-only leaf's surviving values for the window, straight into survivor space. */
    private void readOutputLeaves(
            Window window,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            List<AutoCloseable> acquired) {
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : outputReaders.entrySet()) {
            BatchColumnReader.MaskedWindow masked = readOutputWindow(entry.getValue(), window, acquired);
            leafVectors.put(entry.getKey(), masked.vector());
            if (masked.repLevels() != null) {
                repLevels.put(entry.getKey(), LevelSlice.ofWhole(masked.repLevels()));
            }
            if (masked.defLevels() != null) {
                defLevels.put(entry.getKey(), LevelSlice.ofWhole(masked.defLevels()));
            }
        }
    }

    /** One output column's surviving values for the window, read across the page boundary for a spanning row. */
    private BatchColumnReader.MaskedWindow readOutputWindow(
            BatchColumnReader reader, Window window, List<AutoCloseable> acquired) {
        BitSet survivors = window.survivors().bits();
        if (window.spansPages()) {
            return reader.readMaskedSpanningRow(survivors.get(0), acquired);
        }
        return reader.readMaskedWindow(window.rows(), survivors, acquired);
    }

    /** Advances every output column past a window no row survived, decoding none of its values. */
    private void skipOutputs(Window window) {
        for (BatchColumnReader reader : outputReaders.values()) {
            if (window.spansPages()) {
                reader.readMaskedSpanningRow(false, window.acquiredBuffers());
            } else {
                reader.skipMaskedWindow(window.rows());
            }
        }
    }

    /**
     * Proves the walk covered the row group. A masked scan that ends short of the expected rows has misread a page
     * boundary, which would silently drop rows.
     */
    private void verifyRowsScanned() {
        if (rowsDecodedTotal != expectedRows) {
            throw new MalformedFileException("Masked scan covered " + rowsDecodedTotal + " rows of a row group with "
                    + expectedRows + " rows to read");
        }
    }

    // --- tallies ---

    /** The data pages this scan decoded and skipped across its filter and output columns, for read observability. */
    BatchRowGroupReader.PageCounts pageCounts() {
        int decoded = 0;
        int skipped = 0;
        for (BatchColumnReader reader : scanReaders) {
            decoded += reader.decodedDataPageCount();
            skipped += reader.skippedDataPageCount();
        }
        return new BatchRowGroupReader.PageCounts(decoded, skipped);
    }

    /** The rows this scan ran through decode: every filter-column row of every window it evaluated. */
    long rowsProduced() {
        return rowsDecodedTotal;
    }

    /** The rows that satisfied the predicate, which is the row count of the emitted batches. */
    long rowsMatched() {
        return rowsMatchedTotal;
    }

    /** Time spent evaluating the predicate over the windows, in nanoseconds. */
    long recordFilterNanos() {
        return recordFilterNanos;
    }

    /**
     * Releases the scan: the batch computed ahead of the consumer and not taken, then every column reader's page Arena.
     * One failing close does not leave the remaining readers unclosed.
     */
    @Override
    public void close() {
        try {
            releasePending();
        } finally {
            closeReaders();
        }
    }

    private void releasePending() {
        if (pending != null) {
            ParquetRecordBatch batch = pending;
            pending = null;
            batch.close();
        }
    }

    private void closeReaders() {
        RuntimeException firstFailure = null;
        for (BatchColumnReader reader : scanReaders) {
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

    /**
     * One window's decoded filter columns and the rows of it the predicate kept. The vectors and level windows cover
     * the window's own rows, indexed from zero; {@code acquiredBuffers} holds the decode buffers they read, which the
     * emitted batch takes over and a dropped window releases. {@code spansPages} marks the one-row window that follows
     * a row past the page it started in.
     */
    private record Window(
            int rows,
            Survivors survivors,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            List<AutoCloseable> acquiredBuffers,
            boolean spansPages) {

        int survivorCount() {
            return survivors.count();
        }

        /** Closes the window's decode buffers in reverse order, best-effort. */
        void release() {
            closeQuietly(acquiredBuffers);
        }
    }

    /** The window rows the predicate kept, paired with their count taken once as the window is built. */
    private record Survivors(BitSet bits, int count) {

        static Survivors of(BitSet bits) {
            return new Survivors(bits, bits.cardinality());
        }
    }
}
