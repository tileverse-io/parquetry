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

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
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
import io.tileverse.parquetry.columnar.Selection;
import io.tileverse.parquetry.columnar.VectorizedPredicateEvaluator;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RowRanges;
import io.tileverse.parquetry.format.MalformedFileException;
import io.tileverse.parquetry.internal.filter.PageIndex;
import io.tileverse.parquetry.internal.read.ProjectedLeaves.ProjectedLeaf;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Single-pass filtering reader for one row group: it walks the row group in windows, runs the stages of the predicate
 * over each window, and materializes the output columns only for the rows that survive.
 *
 * <p>A window is the run of rows served by the current page of each column reader, capped by the read's batch size.
 * Within a window the column groups play different parts:
 *
 * <ul>
 *   <li>The leaves of the first stage decode their window whole and feed the predicate evaluator.
 *   <li>A leaf first read by a later stage decodes its window whole only when the stages before it left rows, and the
 *       stage evaluates those rows alone. With no row left the leaf steps over the window, and a page with all its rows
 *       stepped over is discarded without decompression. {@link ScanStages} sets the order of the stages.
 *   <li>A filter leaf projected by the output too is gathered to the surviving rows, never decoded twice.
 *   <li>An output-only leaf decodes only the value slots the surviving rows cover, stepping its page decoder over the
 *       rest.
 * </ul>
 *
 * <p>A window whose rows all fail the predicate emits nothing: its output columns are stepped over without decoding a
 * value. Each emitted batch holds the window's survivors, dense and in row order, over the caller's output schema.
 */
final class MaskedScanRowGroupReader implements RowGroupReader {

    private final DecodeBufferAllocator decodeBufferAllocator;
    private final ParquetSchema firstStageBatchSchema;
    private final ParquetSchema outputSchema;
    private final ParquetSchema outputDecodeSchema;
    private final OptionalInt batchSizeCap;
    private final Optional<RowMask> rowMask;
    private final BatchForm outputForm;
    private final long expectedRows;
    private final LevelAssemblyPlans outputPlans;
    // The synthesized columns read by the first stage, and those presented by the output; each with the row group's
    // own base offset.
    private final Optional<RowPositionSynthesis> filterPositions;
    private final Optional<RowPositionSynthesis> outputPositions;

    // The stages in evaluation order, and the same list without the first one.
    private final List<RowGroupStage> stages;
    private final List<RowGroupStage> laterStages;
    private final Map<ColumnPath, BatchColumnReader> outputReaders = new HashMap<>();
    // The readers of the stages and the output in one list. Window sizing, the drain check, the page tally and
    // close() walk over it.
    private final List<BatchColumnReader> scanReaders = new ArrayList<>();
    // The leaves shared by the stages and the output, gathered from the window and never read a second time.
    private final List<ColumnPath> sharedLeaves = new ArrayList<>();

    private ParquetRecordBatch pending;
    private long rowsDecodedTotal;
    private long rowsMatchedTotal;
    private long recordFilterNanos;

    /**
     * @param outputSchema the schema each emitted batch exposes, synthesized row-position leaves included; an output
     *     schema with no leaf at all emits row-counted batches with no columns, which is what a count folds
     * @param stages the conditions of the predicate in evaluation order
     * @param rowPosition the row group's row-position synthesis inputs when the read synthesizes any column
     * @param expectedRows the rows this scan must cover: the row group's row count, or the count of surviving rows when
     *     a row mask narrows the readers' row space
     */
    @SuppressWarnings("java:S107") // the decode inputs plus the scan's stages and form; a parameter object would
    // only relocate the arity
    MaskedScanRowGroupReader(
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull List<FetchedColumnChunk> chunks,
            @NonNull ParquetSchema fileSchema,
            @NonNull ParquetSchema outputSchema,
            @NonNull ScanStages stages,
            @NonNull OptionalInt batchSizeCap,
            @NonNull Optional<RowMask> rowMask,
            @NonNull BatchForm outputForm,
            @NonNull Optional<RowPositionSynthesis> rowPosition,
            long expectedRows) {
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.outputSchema = outputSchema;
        this.batchSizeCap = batchSizeCap;
        this.rowMask = rowMask;
        this.outputForm = outputForm;
        this.expectedRows = expectedRows;
        this.stages = overRowGroup(stages);
        this.laterStages = this.stages.subList(1, this.stages.size());
        ScanStages.Stage firstStage = this.stages.getFirst().conditions();
        this.filterPositions = positionsRead(rowPosition, firstStage.predicate());
        this.outputPositions = positionsPresented(rowPosition, outputSchema);
        this.firstStageBatchSchema = withPositionLeaves(firstStage.schema(), filterPositions);
        this.outputDecodeSchema = withoutPositionLeaves(outputSchema, outputPositions);
        this.outputPlans = new LevelAssemblyPlans(outputDecodeSchema);
        buildReaders(ProjectedLeaves.resolve(chunks, fileSchema), outputDecodeSchema.leafColumns());
    }

    /** The stages of the scan over this row group. The first stage reads eagerly, and the others on demand. */
    private static List<RowGroupStage> overRowGroup(ScanStages stages) {
        List<RowGroupStage> rowGroupStages = new ArrayList<>(stages.stages().size());
        for (ScanStages.Stage stage : stages.stages()) {
            ValueDecode valueDecode = rowGroupStages.isEmpty() ? ValueDecode.EAGER : ValueDecode.EAGER_ON_DEMAND;
            LevelAssemblyPlans plans = new LevelAssemblyPlans(stage.schema());
            rowGroupStages.add(new RowGroupStage(stage, valueDecode, plans, new HashMap<>()));
        }
        return rowGroupStages;
    }

    /**
     * The synthesized columns read by the first stage, which holds each row-position condition. The batch of that stage
     * must present them. A positional test asks where a row sits in the file, and the row-group tiers prune by that
     * same true position. A column also presented by the projection as a coalesce therefore reaches the predicate as
     * that position, never as the coalesced lineage value.
     */
    private static Optional<RowPositionSynthesis> positionsRead(
            Optional<RowPositionSynthesis> rowPosition, Predicate predicate) {
        Set<ColumnPath> namedPositionally = Set.copyOf(Predicate.rowPositionColumns(predicate));
        return narrowTo(rowPosition, namedPositionally).map(MaskedScanRowGroupReader::asWithinFilePositions);
    }

    /** {@code synthesis} with every column a pure within-file position, which is a first row id of zero. */
    private static RowPositionSynthesis asWithinFilePositions(RowPositionSynthesis synthesis) {
        List<RowPositionColumn> positions = new ArrayList<>(synthesis.columns().size());
        for (RowPositionColumn column : synthesis.columns()) {
            positions.add(RowPositionColumn.position(column.name(), 0L));
        }
        return new RowPositionSynthesis(synthesis.base(), positions);
    }

    /** The synthesized columns the caller's projection asks for, which the emitted batch must present. */
    private static Optional<RowPositionSynthesis> positionsPresented(
            Optional<RowPositionSynthesis> rowPosition, ParquetSchema outputSchema) {
        return narrowTo(rowPosition, Set.copyOf(outputSchema.leafColumns()));
    }

    /** The row group's synthesis restricted to the columns named in {@code names}, or empty when none is. */
    private static Optional<RowPositionSynthesis> narrowTo(
            Optional<RowPositionSynthesis> rowPosition, Set<ColumnPath> names) {
        if (rowPosition.isEmpty()) {
            return Optional.empty();
        }
        RowPositionSynthesis synthesis = rowPosition.orElseThrow();
        List<RowPositionColumn> kept = new ArrayList<>(synthesis.columns().size());
        for (RowPositionColumn column : synthesis.columns()) {
            if (names.contains(column.name())) {
                kept.add(column);
            }
        }
        if (kept.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new RowPositionSynthesis(synthesis.base(), kept));
    }

    /** {@code schema} plus the leaves {@code positions} synthesizes, which no column chunk holds. */
    private static ParquetSchema withPositionLeaves(ParquetSchema schema, Optional<RowPositionSynthesis> positions) {
        return positions
                .map(synthesis -> schema.withAppendedLeaves(RowPositionVectors.leaves(synthesis.columns())))
                .orElse(schema);
    }

    /**
     * {@code schema} without the leaves {@code positions} synthesizes: the decoded shape the column readers and the
     * batch assembly cover. A coalesce presented under its source name keeps that leaf, which the read does decode.
     */
    private static ParquetSchema withoutPositionLeaves(ParquetSchema schema, Optional<RowPositionSynthesis> positions) {
        if (positions.isEmpty()) {
            return schema;
        }
        Set<ColumnPath> synthesized = new HashSet<>();
        for (RowPositionColumn column : positions.orElseThrow().columns()) {
            if (!column.reusesSourceLeaf()) {
                synthesized.add(column.name());
            }
        }
        Set<ColumnPath> decoded = new HashSet<>(schema.leafColumns());
        decoded.removeAll(synthesized);
        return schema.project(decoded);
    }

    /**
     * Builds one reader per scanned leaf. A filter leaf belongs to the first stage reading it, and takes the value
     * decode of that stage. An output-only leaf defers its values to the windows that keep rows, and a leaf fetched for
     * neither is never decoded. Under a row mask the filter readers and the output readers take the same surviving
     * rows, and they serve one row space. Row {@code j} of a filter reader and row {@code j} of an output reader are
     * the same row of the row group.
     */
    private void buildReaders(List<ProjectedLeaf> leaves, List<ColumnPath> outputLeaves) {
        for (ProjectedLeaf leaf : leaves) {
            boolean projected = outputLeaves.contains(leaf.path());
            Optional<RowGroupStage> owner = stageOwning(leaf.path());
            if (owner.isPresent()) {
                addOwnReader(owner.orElseThrow(), leaf);
                if (projected) {
                    sharedLeaves.add(leaf.path());
                }
            } else if (projected) {
                outputReaders.put(leaf.path(), buildReader(leaf, ValueDecode.WINDOWED_MASK));
            }
        }
        for (RowGroupStage stage : stages) {
            scanReaders.addAll(stage.ownReaders().values());
        }
        scanReaders.addAll(outputReaders.values());
    }

    private Optional<RowGroupStage> stageOwning(ColumnPath leaf) {
        for (RowGroupStage stage : stages) {
            if (stage.conditions().ownLeaves().contains(leaf)) {
                return Optional.of(stage);
            }
        }
        return Optional.empty();
    }

    private void addOwnReader(RowGroupStage stage, ProjectedLeaf leaf) {
        stage.ownReaders().put(leaf.path(), buildReader(leaf, stage.valueDecode()));
    }

    private BatchColumnReader buildReader(ProjectedLeaf leaf, ValueDecode valueDecode) {
        RowRanges survivingRows = rowMask.isPresent() ? rowMask.get().survivingRows() : null;
        PageIndex pageIndex = rowMask.isPresent() ? rowMask.get().pageIndexes().get(leaf.path()) : null;
        return new BatchColumnReader(
                decodeBufferAllocator, leaf.chunk(), leaf.leaf(), survivingRows, pageIndex, valueDecode);
    }

    /**
     * Returns true while a surviving batch is available, computing it on demand. Windows whose rows all fail the
     * predicate are consumed here and emit nothing, which keeps empty batches out of the hand-off.
     */
    @Override
    public boolean hasMore() {
        if (pending != null) {
            return true;
        }
        pending = computeNextSurvivingBatch();
        return pending != null;
    }

    @Override
    @MustBeClosed
    public ParquetRecordBatch nextBatch() {
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
     * Runs the stages over one window. The window is the run of rows served by the current page of each reader, capped
     * by the batch size. A window of zero rows while rows remain means that a repeated column deferred the last-started
     * row of its page, or that a column stands on a page with no rows. The scan then advances one row across the page
     * boundary.
     */
    private Window readAndEvaluateWindow() {
        int windowRows = computeWindowRows();
        if (windowRows == 0) {
            return readAndEvaluateSpanningRow();
        }
        return runStages(windowRows, false);
    }

    /** The window served by the current page of each reader, capped by the read's batch size. */
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
     * Runs the stages over the one row at a page boundary of some column. Each column advances that row, across pages
     * where needed, to stay in lockstep.
     */
    private Window readAndEvaluateSpanningRow() {
        requireUnmaskedSpanningRow();
        return runStages(1, true);
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
     * Runs the stages in order over the next {@code windowRows} rows. The stages add their vectors to one window. A
     * later stage reuses the leaves of an earlier stage from it, and the emitted batch gathers a shared leaf from it.
     */
    private Window runStages(int windowRows, boolean spansPages) {
        Window window = Window.open(rowsDecodedTotal, windowRows, new BatchBuffers(decodeBufferAllocator), spansPages);
        try {
            BitSet rowsLeft = runFirstStage(window);
            for (RowGroupStage stage : laterStages) {
                rowsLeft = runLaterStage(stage, window, rowsLeft);
            }
            rowsDecodedTotal += windowRows;
            return window.withSurvivors(rowsLeft);
        } catch (RuntimeException e) {
            window.release();
            throw e;
        }
    }

    /** Reads the leaves of the first stage over the window and evaluates each of its rows. */
    private BitSet runFirstStage(Window window) {
        RowGroupStage first = stages.getFirst();
        readOwnLeaves(first, window);
        long start = System.nanoTime();
        try (ParquetRecordBatch batch = firstStageBatch(first, window)) {
            return VectorizedPredicateEvaluator.eval(first.conditions().predicate(), batch);
        } finally {
            recordFilterNanos += System.nanoTime() - start;
        }
    }

    /**
     * Narrows {@code rowsLeft} by one later stage. The readers of the stage are read only after {@code rowsLeft} is
     * known to hold a row, because a read loads the page held compressed by the reader. With no row left they step over
     * the window, and the stage evaluates nothing.
     */
    private BitSet runLaterStage(RowGroupStage stage, Window window, BitSet rowsLeft) {
        if (rowsLeft.isEmpty()) {
            stepOverWindow(stage, window);
            return rowsLeft;
        }
        readOwnLeaves(stage, window);
        long start = System.nanoTime();
        try (ParquetRecordBatch batch = laterStageBatch(stage, window)) {
            return VectorizedPredicateEvaluator.evalWithin(stage.conditions().predicate(), batch, rowsLeft);
        } finally {
            recordFilterNanos += System.nanoTime() - start;
        }
    }

    private static void stepOverWindow(RowGroupStage stage, Window window) {
        for (BatchColumnReader reader : stage.ownReaders().values()) {
            if (window.spansPages()) {
                reader.skipSpanningRow();
            } else {
                reader.skipWindow(window.rows());
            }
        }
    }

    private static void readOwnLeaves(RowGroupStage stage, Window window) {
        if (window.spansPages()) {
            readFilterSpanningRow(stage.ownReaders(), window);
        } else {
            readFilterVectors(stage.ownReaders(), window);
        }
    }

    /**
     * Reads the window's rows from {@code readers}, capturing each leaf's rep- and def-level windows as views over the
     * page level sequences before the reader advances.
     */
    private static void readFilterVectors(Map<ColumnPath, BatchColumnReader> readers, Window window) {
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : readers.entrySet()) {
            BatchColumnReader reader = entry.getValue();
            int windowValues = reader.valuesForLogicalRows(window.rows());
            int start = reader.valuesConsumedInCurrentPage();
            Levels pageRepLevels = reader.currentPageRepLevels();
            if (pageRepLevels != null) {
                window.repLevels().put(entry.getKey(), new LevelSlice(pageRepLevels, start, windowValues));
            }
            Levels pageDefLevels = reader.currentPageDefLevels();
            if (pageDefLevels != null) {
                window.defLevels().put(entry.getKey(), new LevelSlice(pageDefLevels, start, windowValues));
            }
            window.leafVectors().put(entry.getKey(), reader.readBatch(windowValues, window.batchBuffers()));
        }
    }

    /**
     * Reads exactly one logical row from {@code readers}, following it across data-page boundaries. The level windows
     * come back as reader-owned copies, wrapped whole for the assemblers.
     */
    private static void readFilterSpanningRow(Map<ColumnPath, BatchColumnReader> readers, Window window) {
        for (Map.Entry<ColumnPath, BatchColumnReader> entry : readers.entrySet()) {
            BatchColumnReader.SpanningRow row = entry.getValue().readSpanningRow(window.batchBuffers());
            if (row.repLevels() != null) {
                window.repLevels().put(entry.getKey(), LevelSlice.ofWhole(row.repLevels()));
            }
            if (row.defLevels() != null) {
                window.defLevels().put(entry.getKey(), LevelSlice.ofWhole(row.defLevels()));
            }
            window.leafVectors().put(entry.getKey(), row.vector());
        }
    }

    /**
     * The leaves of the first stage as a batch for the predicate evaluator, plus the row-position columns named by the
     * stage. The window holds no other leaf yet. The batch registers no buffer. The window owns the decode buffers read
     * by the vectors, and closing this batch releases only its own arena.
     */
    @MustBeClosed
    private ParquetRecordBatch firstStageBatch(RowGroupStage first, Window window) {
        Map<ColumnPath, ColumnVector> vectors =
                assembleStage(first, window.leafVectors(), window.repLevels(), window.defLevels(), window);
        Map<ColumnPath, ColumnVector> withPositions = addFilterPositions(vectors, window);
        return new DefaultParquetRecordBatch(firstStageBatchSchema, withPositions, window.rows());
    }

    /**
     * The leaves read by {@code stage} as a batch for the predicate evaluator, its own and those reused from earlier
     * stages. Like the batch of the first stage, it registers no buffer.
     */
    @MustBeClosed
    private ParquetRecordBatch laterStageBatch(RowGroupStage stage, Window window) {
        Set<ColumnPath> leaves = stage.conditions().leaves();
        Map<ColumnPath, ColumnVector> vectors = assembleStage(
                stage,
                restrictedTo(leaves, window.leafVectors()),
                restrictedTo(leaves, window.repLevels()),
                restrictedTo(leaves, window.defLevels()),
                window);
        return new DefaultParquetRecordBatch(stage.conditions().schema(), vectors, window.rows());
    }

    /** The entries of {@code windowEntries} for {@code leaves}. A leaf with no level stream has no level entry. */
    private static <T> Map<ColumnPath, T> restrictedTo(Set<ColumnPath> leaves, Map<ColumnPath, T> windowEntries) {
        Map<ColumnPath, T> restricted = HashMap.newHashMap(leaves.size());
        for (ColumnPath leaf : leaves) {
            T entry = windowEntries.get(leaf);
            if (entry != null) {
                restricted.put(leaf, entry);
            }
        }
        return restricted;
    }

    private Map<ColumnPath, ColumnVector> assembleStage(
            RowGroupStage stage,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            Window window) {
        return BatchAssembly.assemble(
                BatchForm.LEVELS,
                stage.plans(),
                stage.conditions().schema(),
                leafVectors,
                repLevels,
                defLevels,
                window.rows(),
                decodeBufferAllocator,
                window.batchBuffers());
    }

    /** The window's synthesized columns over its own rows, which the predicate reads beside the decoded ones. */
    private Map<ColumnPath, ColumnVector> addFilterPositions(Map<ColumnPath, ColumnVector> vectors, Window window) {
        if (filterPositions.isEmpty()) {
            return vectors;
        }
        Selection positions = RowPositionVectors.window(rowMask, window.firstDenseRow(), window.rows());
        return RowPositionVectors.add(vectors, filterPositions.orElseThrow(), positions, window.rows());
    }

    /** Builds the window's emitted batch: survivor-dense output leaves, in the read's batch form. */
    private ParquetRecordBatch emit(Window window) {
        BatchBuffers acquired = window.batchBuffers();
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
                    outputDecodeSchema,
                    leafVectors,
                    repLevels,
                    defLevels,
                    survivors,
                    decodeBufferAllocator,
                    acquired);
            Map<ColumnPath, ColumnVector> withPositions = addOutputPositions(vectors, window);
            DefaultParquetRecordBatch batch = new DefaultParquetRecordBatch(outputSchema, withPositions, survivors);
            batch.registerBuffer(acquired);
            rowsMatchedTotal += survivors;
            return batch;
        } catch (RuntimeException e) {
            window.release();
            throw e;
        }
    }

    /**
     * The emitted batch's synthesized columns, reporting each surviving row's true position: the window's positions
     * restricted to the rows the predicate kept.
     */
    private Map<ColumnPath, ColumnVector> addOutputPositions(Map<ColumnPath, ColumnVector> vectors, Window window) {
        if (outputPositions.isEmpty()) {
            return vectors;
        }
        Selection positions = RowPositionVectors.survivors(
                rowMask,
                window.firstDenseRow(),
                window.rows(),
                window.survivors().bits());
        return RowPositionVectors.add(vectors, outputPositions.orElseThrow(), positions, window.survivorCount());
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
            BatchBuffers acquired) {
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
            BatchColumnReader reader, Window window, BatchBuffers acquired) {
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
                reader.readMaskedSpanningRow(false, window.batchBuffers());
            } else {
                reader.skipWindow(window.rows());
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
    @Override
    public BatchRowGroupReader.PageCounts pageCounts() {
        int decoded = 0;
        int skipped = 0;
        for (BatchColumnReader reader : scanReaders) {
            decoded += reader.decodedDataPageCount();
            skipped += reader.skippedDataPageCount();
        }
        return new BatchRowGroupReader.PageCounts(decoded, skipped);
    }

    /** The rows of the windows evaluated by this scan, whether or not a later stage read them. */
    @Override
    public long rowsProduced() {
        return rowsDecodedTotal;
    }

    /** The rows that satisfied the predicate, which is the row count of the emitted batches. */
    long rowsMatched() {
        return rowsMatchedTotal;
    }

    /** Time spent evaluating the stages over the windows, in nanoseconds. */
    @Override
    public long recordFilterNanos() {
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
     * One stage of the scan over this row group.
     *
     * @param conditions the conditions of the stage and the leaves read by them
     * @param valueDecode how the own leaves of the stage materialize a page
     * @param plans the plans assembling the stage's batches
     * @param ownReaders the readers of the leaves first read by this stage; a stage holds none when each of its leaves
     *     was read by an earlier stage
     */
    private record RowGroupStage(
            ScanStages.Stage conditions,
            ValueDecode valueDecode,
            LevelAssemblyPlans plans,
            Map<ColumnPath, BatchColumnReader> ownReaders) {}

    /**
     * One window's decoded filter columns and the rows of it kept by the predicate. The vectors and level windows cover
     * the window's own rows, indexed from zero; {@code firstDenseRow} places those rows in the row group, counting the
     * rows walked by the scan before them. A leaf of a later stage has no entry when no row reached that stage, and
     * such a window keeps no row. {@code batchBuffers} holds the decode buffers read by those vectors; the emitted
     * batch takes them over, and a dropped window releases them. {@code spansPages} marks the one-row window at a page
     * boundary of some column.
     */
    private record Window(
            long firstDenseRow,
            int rows,
            Survivors survivors,
            Map<ColumnPath, ColumnVector> leafVectors,
            Map<ColumnPath, LevelSlice> repLevels,
            Map<ColumnPath, LevelSlice> defLevels,
            BatchBuffers batchBuffers,
            boolean spansPages) {

        /** A window with no column read and no survivor yet. The stages fill it, and {@link #withSurvivors} ends it. */
        static Window open(long firstDenseRow, int rows, BatchBuffers batchBuffers, boolean spansPages) {
            return new Window(
                    firstDenseRow,
                    rows,
                    Survivors.of(new BitSet()),
                    new HashMap<>(),
                    new HashMap<>(),
                    new HashMap<>(),
                    batchBuffers,
                    spansPages);
        }

        Window withSurvivors(BitSet rowsLeft) {
            return new Window(
                    firstDenseRow,
                    rows,
                    Survivors.of(rowsLeft),
                    leafVectors,
                    repLevels,
                    defLevels,
                    batchBuffers,
                    spansPages);
        }

        int survivorCount() {
            return survivors.count();
        }

        /** Closes the window's decode buffers in reverse order, best-effort. */
        void release() {
            batchBuffers.close();
        }
    }

    /** The window rows the predicate kept, paired with their count taken once as the window is built. */
    private record Survivors(BitSet bits, int count) {

        static Survivors of(BitSet bits) {
            return new Survivors(bits, bits.cardinality());
        }
    }
}
