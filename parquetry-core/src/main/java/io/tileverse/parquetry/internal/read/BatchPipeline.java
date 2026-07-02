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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.columnar.FilteredRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.VectorizedPredicateEvaluator;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Batch pipeline driven by a {@link ParallelDecodeCoordinator}. Pulls fully decoded row groups from the coordinator in
 * strict file order, drains each one's batches into the returned stream, and closes the row group's undelivered batches
 * when it is exhausted (or the stream is closed early).
 *
 * <p>The coordinator decodes upcoming row groups in parallel on a shared decode pool while the consumer drains the
 * current one, and reorders the worker completions so emission stays in file order. Closing the stream cascades to the
 * coordinator, which drains any in-flight decodes and closes the prefetcher.
 */
public final class BatchPipeline {

    private BatchPipeline() {}

    /**
     * Returns a closeable stream that yields one {@link ParquetRecordBatch} at a time across the coordinator's row
     * groups, in strict file order. Closing the stream cascades to the coordinator, which drains in-flight decodes and
     * closes the prefetcher.
     */
    @MustBeClosed
    public static Stream<ParquetRecordBatch> batches(@NonNull ParallelDecodeCoordinator coordinator) {
        return stream(new BatchIterator(coordinator));
    }

    /**
     * Returns a closeable stream of batches with {@code recordFilter} applied exactly: each surviving batch is narrowed
     * to {@code outputSchema} and compacted to its predicate-matching rows. A row group that statistics already proved
     * matches in full skips evaluation and only narrows. Fully filtered-out batches are dropped (never an empty batch).
     * A {@code null} {@code recordFilter} narrows without evaluating (the pushdown-only shape). When {@code gate} is
     * present, each batch's survivors are decimated through it after the filter, or, with no filter, starting from
     * every row.
     */
    @MustBeClosed
    public static Stream<ParquetRecordBatch> batches(
            @NonNull ParallelDecodeCoordinator coordinator,
            Predicate recordFilter,
            @NonNull ParquetSchema outputSchema,
            @NonNull Optional<SpatialDecimationGate> gate) {
        return stream(new FilteredBatchIterator(coordinator, recordFilter, outputSchema, gate.orElse(null)));
    }

    @MustBeClosed
    private static Stream<ParquetRecordBatch> stream(CloseableBatchIterator iterator) {
        Spliterator<ParquetRecordBatch> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /** A batch iterator whose {@code close()} throws no checked exception, fit for {@link Stream#onClose(Runnable)}. */
    private interface CloseableBatchIterator extends Iterator<ParquetRecordBatch> {
        void close();
    }

    /**
     * Returns a closeable stream of rows materialized through {@code materializer}. Iterates the coordinator's row
     * groups in file order; each group's batches are iterated row-by-row and closed when its row stream ends. Closing
     * the outer stream cascades to the underlying row-group iterator.
     *
     * <p>A row group whose statistics already proved every row matches (the MATCHED outcome) skips per-row evaluation:
     * its rows are passed through without testing them against {@code recordFilter}. Every other surviving row group
     * applies {@code recordFilter} when it is non-null. A null {@code recordFilter} passes every row through (the
     * pushdown-only path). When {@code gate} is present, each batch's survivors are decimated through it after the
     * filter, or, with no filter, starting from every row.
     */
    @MustBeClosed
    public static <T> Stream<T> rows(
            @NonNull ParallelDecodeCoordinator coordinator,
            @NonNull Materializer<T> materializer,
            @NonNull ParquetSchema outputSchema,
            Predicate recordFilter,
            boolean observe,
            boolean wantsTimings,
            @NonNull Optional<SpatialDecimationGate> gate) {
        return rows(coordinator, materializer, outputSchema, recordFilter, observe, wantsTimings, gate, batch -> {});
    }

    /**
     * Same as {@link #rows(ParallelDecodeCoordinator, Materializer, ParquetSchema, Predicate, boolean, boolean,
     * Optional)}, plus a {@code batchObserver} notified the moment each freshly pulled batch becomes the one live
     * batch. Tests use it to assert that at most one batch is resident at a time.
     */
    @MustBeClosed
    @SuppressWarnings("java:S107") // cohesive row-scan collaborators; a parameter object would only relocate the arity
    static <T> Stream<T> rows(
            @NonNull ParallelDecodeCoordinator coordinator,
            @NonNull Materializer<T> materializer,
            @NonNull ParquetSchema outputSchema,
            Predicate recordFilter,
            boolean observe,
            boolean wantsTimings,
            @NonNull Optional<SpatialDecimationGate> gate,
            @NonNull Consumer<ParquetRecordBatch> batchObserver) {
        RowIterator<T> iterator = new RowIterator<>(
                coordinator,
                materializer,
                outputSchema,
                recordFilter,
                batchObserver,
                observe,
                wantsTimings,
                gate.orElse(null));
        Spliterator<T> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /**
     * Counts rows matching {@code predicate} across the coordinator's batches with no materialization. Each batch is
     * evaluated columnar-style ({@link VectorizedPredicateEvaluator}) and its matching-row bitset cardinality is
     * summed. Every batch is closed as it is consumed; closing the stream cascades to the coordinator. When
     * {@code gate} is present, the matching-row bitset is decimated through it before counting.
     */
    public static long countMatching(
            @NonNull ParallelDecodeCoordinator coordinator,
            @NonNull Predicate predicate,
            boolean observe,
            @NonNull Optional<SpatialDecimationGate> gate) {
        SpatialDecimationGate decimationGate = gate.orElse(null);
        long total = 0L;
        BatchIterator iterator = new BatchIterator(coordinator);
        try (Stream<ParquetRecordBatch> batches = stream(iterator)) {
            Iterator<ParquetRecordBatch> it = batches.iterator();
            while (it.hasNext()) {
                try (ParquetRecordBatch batch = it.next()) {
                    long matched = countMatchingRows(predicate, batch, decimationGate);
                    total += matched;
                    if (observe) {
                        iterator.addMatchedToCurrentRowGroup(matched);
                    }
                }
            }
        }
        return total;
    }

    /** The matching-row count for one batch, decimated through {@code gate} when one is present. */
    private static long countMatchingRows(Predicate predicate, ParquetRecordBatch batch, SpatialDecimationGate gate) {
        BitSet matches = VectorizedPredicateEvaluator.eval(predicate, batch);
        if (gate != null) {
            gate.narrow(batch, matches);
        }
        return matches.cardinality();
    }

    // ---- iterators ----

    /**
     * Pulls one {@link ParquetRecordBatch} at a time. Advances to the next decoded row group when the current one is
     * drained, closing the previous row group's undelivered batches before pulling the next one from the coordinator.
     */
    private static final class BatchIterator implements CloseableBatchIterator {

        private final ParallelDecodeCoordinator coordinator;
        private DecodedRowGroup currentRowGroup;

        BatchIterator(ParallelDecodeCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public boolean hasNext() {
            while (currentRowGroup == null || !currentRowGroup.hasNext()) {
                closeCurrentRowGroup();
                DecodedRowGroup next;
                try {
                    next = coordinator.next();
                } catch (IOException e) {
                    throw new UncheckedIOException("Row group decode failed", e);
                }
                if (next == null) {
                    return false;
                }
                currentRowGroup = next;
            }
            return true;
        }

        @Override
        public ParquetRecordBatch next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return currentRowGroup.next();
        }

        /**
         * Attributes {@code matched} rows to the row group that produced the batch just returned by {@link #next()}.
         * Only the count path calls it, and only when observing.
         */
        void addMatchedToCurrentRowGroup(long matched) {
            currentRowGroup.addMatchedRows(matched);
        }

        @Override
        public void close() {
            closeCurrentRowGroup();
            coordinator.close();
        }

        private void closeCurrentRowGroup() {
            DecodedRowGroup rowGroup = currentRowGroup;
            currentRowGroup = null;
            if (rowGroup != null) {
                rowGroup.close();
            }
        }
    }

    /**
     * Pulls one {@link ParquetRecordBatch} at a time with the record filter applied exactly. Each pulled batch is
     * narrowed to the output leaves and compacted to its matching rows; a batch with no matches is closed and skipped,
     * never emitted. The per-group filter is resolved once when a group is taken (the row group's statistics may
     * already prove every row matches, which skips evaluation and only narrows). It uses a compute-next pattern because
     * a pulled batch may be skipped.
     */
    private static final class FilteredBatchIterator implements CloseableBatchIterator {

        private final ParallelDecodeCoordinator coordinator;
        private final Predicate recordFilter;
        private final ParquetSchema outputSchema;
        private final SpatialDecimationGate gate;

        private DecodedRowGroup currentRowGroup;
        private Predicate currentFilter;
        private boolean hasComputedNext;
        private ParquetRecordBatch next;

        FilteredBatchIterator(
                ParallelDecodeCoordinator coordinator,
                Predicate recordFilter,
                ParquetSchema outputSchema,
                SpatialDecimationGate gate) {
            this.coordinator = coordinator;
            this.recordFilter = recordFilter;
            this.outputSchema = outputSchema;
            this.gate = gate;
        }

        @Override
        public boolean hasNext() {
            if (!hasComputedNext) {
                next = advance();
                hasComputedNext = true;
            }
            return next != null;
        }

        @Override
        public ParquetRecordBatch next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ParquetRecordBatch result = next;
            next = null;
            hasComputedNext = false;
            return result;
        }

        private ParquetRecordBatch advance() {
            while (true) {
                ParquetRecordBatch source = pullNextSourceBatch();
                if (source == null) {
                    return null;
                }
                ParquetRecordBatch survivor = filter(source);
                if (survivor != null) {
                    return survivor;
                }
            }
        }

        // Once pullNextSourceBatch hands a batch here, the row group no longer owns it (the same semantics as
        // BatchIterator, where a pulled batch becomes the caller's). This method either closes the source (no match),
        // returns a survivor that owns the source (closed downstream), or returns the source itself when it is already
        // output-shaped.
        private ParquetRecordBatch filter(ParquetRecordBatch source) {
            if (gate == null && currentFilter == null) {
                return FilteredRecordBatch.narrowed(source, outputSchema);
            }
            BitSet matches = survivorsOf(source);
            int matched = matches.cardinality();
            if (matched == 0) {
                source.close();
                return null;
            }
            if (matched == source.rowCount()) {
                return FilteredRecordBatch.narrowed(source, outputSchema);
            }
            return FilteredRecordBatch.filtered(source, matches, outputSchema);
        }

        /**
         * The surviving rows of {@code source}: the predicate matches when a filter is present, every row otherwise,
         * decimated through the gate when one is present.
         */
        private BitSet survivorsOf(ParquetRecordBatch source) {
            BitSet survivors = currentFilter == null
                    ? allRows(source.rowCount())
                    : VectorizedPredicateEvaluator.eval(currentFilter, source);
            if (gate != null) {
                gate.narrow(source, survivors);
            }
            return survivors;
        }

        private ParquetRecordBatch pullNextSourceBatch() {
            while (true) {
                if (currentRowGroup != null && currentRowGroup.hasNext()) {
                    return currentRowGroup.next();
                }
                if (!takeNextRowGroup()) {
                    return null;
                }
            }
        }

        private boolean takeNextRowGroup() {
            closeCurrentRowGroup();
            DecodedRowGroup nextRowGroup;
            try {
                nextRowGroup = coordinator.next();
            } catch (IOException e) {
                throw new UncheckedIOException("Row group decode failed", e);
            }
            if (nextRowGroup == null) {
                return false;
            }
            currentRowGroup = nextRowGroup;
            currentFilter = nextRowGroup.recordEvalRequired() ? recordFilter : null;
            return true;
        }

        @Override
        public void close() {
            closeBestEffort(this::closeBufferedNext);
            closeBestEffort(this::closeCurrentRowGroup);
            closeBestEffort(coordinator::close);
        }

        // A survivor computed by hasNext() but not yet taken by next() owns the source's off-heap buffers; closing the
        // row group only releases the queue's undelivered batches, never this one. Release it explicitly.
        private void closeBufferedNext() {
            ParquetRecordBatch buffered = next;
            next = null;
            hasComputedNext = false;
            if (buffered != null) {
                buffered.close();
            }
        }

        // Closes only the group's undelivered batches; a batch already handed to filter() is owned elsewhere.
        private void closeCurrentRowGroup() {
            DecodedRowGroup rowGroup = currentRowGroup;
            currentRowGroup = null;
            currentFilter = null;
            if (rowGroup != null) {
                rowGroup.close();
            }
        }
    }

    /**
     * Pulls materialized rows one at a time, keeping exactly one decoded batch live. It scans the current batch row by
     * row; when that batch is drained it is closed before the next one is pulled, and when the current row group is
     * drained it is closed before the next is taken from the coordinator. This three-level walk (coordinator -> row
     * group -> batch) holds only one batch in memory at a time, unlike a {@code flatMap} chain that buffers emitted
     * rows (and the batches they pin) when pulled lazily.
     *
     * <p>The per-group filter is resolved once when a group is taken: {@code recordFilter} when the group still needs
     * per-row evaluation, or {@code null} when its statistics already proved every row matches (the MATCHED outcome). A
     * {@code null} filter passes every row through (the pushdown-only path); a non-null filter skips rows it rejects,
     * neither materializing nor emitting them.
     *
     * <p>{@link #close()} closes the current batch, the current row group, and the coordinator, best-effort and
     * idempotent, mirroring {@link BatchIterator}.
     */
    private static final class RowIterator<T> implements Iterator<T>, AutoCloseable {

        private final ParallelDecodeCoordinator coordinator;
        private final Materializer<T> materializer;
        private final ParquetSchema outputSchema;
        private final Predicate recordFilter;
        private final Consumer<ParquetRecordBatch> batchObserver;
        private final boolean observe;
        private final boolean wantsTimings;
        private final SpatialDecimationGate gate;

        private DecodedRowGroup currentRowGroup;
        private Predicate currentFilter;
        private ParquetRecordBatch currentBatch;
        // A zero-copy view of currentBatch narrowed to the output leaves, used to materialize rows. The predicate runs
        // against the wider currentBatch (which may hold predicate-only or synthesized columns the output excludes),
        // and the records the caller sees expose exactly the output columns.
        private ParquetRecordBatch outputBatch;
        private BitSet currentMask;
        private boolean maskReady;
        private int rowIndex;
        private int batchRowCount;
        private long matchedInCurrentRowGroup;
        private long recordFilterNanosInCurrentRowGroup;

        private boolean hasComputedNext;
        private T next;

        @SuppressWarnings(
                "java:S107") // cohesive row-scan collaborators; a parameter object would only relocate the arity
        RowIterator(
                ParallelDecodeCoordinator coordinator,
                Materializer<T> materializer,
                ParquetSchema outputSchema,
                Predicate recordFilter,
                Consumer<ParquetRecordBatch> batchObserver,
                boolean observe,
                boolean wantsTimings,
                SpatialDecimationGate gate) {
            this.coordinator = coordinator;
            this.materializer = materializer;
            this.outputSchema = outputSchema;
            this.recordFilter = recordFilter;
            this.batchObserver = batchObserver;
            this.observe = observe;
            this.wantsTimings = wantsTimings;
            this.gate = gate;
        }

        @Override
        public boolean hasNext() {
            if (!hasComputedNext) {
                next = advance();
                hasComputedNext = true;
            }
            return next != null;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            T result = next;
            next = null;
            hasComputedNext = false;
            return result;
        }

        private T advance() {
            while (true) {
                if (rowIndex >= batchRowCount && !pullNextBatch()) {
                    return null;
                }
                T row = scanCurrentBatch();
                if (row != null) {
                    return row;
                }
            }
        }

        /**
         * Materializes the next surviving row in the current batch, or {@code null} once its rows are exhausted. When
         * the observer opted into timings, each scan resumption is bracketed and the elapsed time accumulates into the
         * current row group's record-filter tally; the figure covers predicate evaluation plus the per-row
         * materialization interleaved with it.
         */
        private T scanCurrentBatch() {
            if (!wantsTimings) {
                return scanRows();
            }
            long start = System.nanoTime();
            try {
                return scanRows();
            } finally {
                recordFilterNanosInCurrentRowGroup += System.nanoTime() - start;
            }
        }

        private T scanRows() {
            ensureMask();
            int row = (currentMask == null) ? rowIndex : currentMask.nextSetBit(rowIndex);
            if (row < 0 || row >= batchRowCount) {
                rowIndex = batchRowCount;
                return null;
            }
            rowIndex = row + 1;
            if (observe) {
                matchedInCurrentRowGroup++;
            }
            ParquetRecord rec = outputBatch.materialize(row);
            return materializer.materialize(outputSchema, rec);
        }

        /**
         * Computes the surviving-rows mask once per batch; {@code null} when neither a filter nor a gate is in play and
         * hence every row passes. With a gate present, a {@code null} filter still yields an explicit all-rows mask to
         * narrow through the gate.
         */
        private void ensureMask() {
            if (maskReady) {
                return;
            }
            currentMask = computeMask();
            maskReady = true;
        }

        private BitSet computeMask() {
            if (gate == null && currentFilter == null) {
                return null;
            }
            BitSet survivors = currentFilter == null
                    ? allRows(batchRowCount)
                    : VectorizedPredicateEvaluator.eval(currentFilter, currentBatch);
            if (gate != null) {
                gate.narrow(currentBatch, survivors);
            }
            return survivors;
        }

        /** Closes the drained batch and makes the next non-empty batch current, or returns {@code false} at the end. */
        private boolean pullNextBatch() {
            closeCurrentBatch();
            while (true) {
                if (currentRowGroup != null && currentRowGroup.hasNext()) {
                    currentBatch = currentRowGroup.next();
                    outputBatch = FilteredRecordBatch.narrowed(currentBatch, outputSchema);
                    batchObserver.accept(currentBatch);
                    rowIndex = 0;
                    batchRowCount = currentBatch.rowCount();
                    if (batchRowCount > 0) {
                        currentMask = null;
                        maskReady = false;
                        return true;
                    }
                    closeCurrentBatch();
                    continue;
                }
                if (!takeNextRowGroup()) {
                    return false;
                }
            }
        }

        /**
         * Closes the drained row group and takes the next from the coordinator, resolving its per-group filter once.
         */
        private boolean takeNextRowGroup() {
            closeCurrentRowGroup();
            DecodedRowGroup nextRowGroup;
            try {
                nextRowGroup = coordinator.next();
            } catch (IOException e) {
                throw new UncheckedIOException("Row group decode failed", e);
            }
            if (nextRowGroup == null) {
                return false;
            }
            currentRowGroup = nextRowGroup;
            currentFilter = nextRowGroup.recordEvalRequired() ? recordFilter : null;
            return true;
        }

        @Override
        public void close() {
            closeBestEffort(this::closeCurrentBatch);
            closeBestEffort(this::closeCurrentRowGroup);
            closeBestEffort(coordinator::close);
        }

        private void closeCurrentBatch() {
            ParquetRecordBatch batch = currentBatch;
            currentBatch = null;
            // outputBatch is a zero-copy view sharing batch's resources; clear the reference but never close it
            // separately, closing batch releases the shared backing.
            outputBatch = null;
            rowIndex = 0;
            batchRowCount = 0;
            if (batch != null) {
                batch.close();
            }
        }

        private void closeCurrentRowGroup() {
            DecodedRowGroup rowGroup = currentRowGroup;
            currentRowGroup = null;
            currentFilter = null;
            if (rowGroup != null) {
                if (observe) {
                    rowGroup.addMatchedRows(matchedInCurrentRowGroup);
                    matchedInCurrentRowGroup = 0L;
                }
                if (wantsTimings) {
                    rowGroup.addRecordFilterNanos(recordFilterNanosInCurrentRowGroup);
                    recordFilterNanosInCurrentRowGroup = 0L;
                }
                rowGroup.close();
            }
        }
    }

    /** A bitset with every row in {@code [0, rowCount)} set, the all-rows start for a gate narrow with no filter. */
    private static BitSet allRows(int rowCount) {
        BitSet rows = new BitSet(rowCount);
        rows.set(0, rowCount);
        return rows;
    }

    /** Runs a close step, swallowing a {@link RuntimeException} when one failed close must not skip the rest. */
    private static void closeBestEffort(Runnable close) {
        try {
            close.run();
        } catch (RuntimeException _) {
            // best-effort; one failed close must not skip the rest
        }
    }
}
