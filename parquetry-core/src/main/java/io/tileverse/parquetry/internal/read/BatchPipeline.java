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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.VectorizedPredicateEvaluator;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.RecordLevelEvaluator;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.BatchRowAccessor;
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
        BatchIterator iterator = new BatchIterator(coordinator);
        Spliterator<ParquetRecordBatch> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /**
     * Returns a closeable stream of rows materialized through {@code materializer}. Iterates the coordinator's row
     * groups in file order; each group's batches are iterated row-by-row and closed when its row stream ends. Closing
     * the outer stream cascades to the underlying row-group iterator.
     *
     * <p>A row group whose statistics already proved every row matches (the MATCHED outcome) skips per-row evaluation:
     * its rows are passed through without testing them against {@code recordFilter}. Every other surviving row group
     * applies {@code recordFilter} when it is non-null. A null {@code recordFilter} passes every row through (the
     * pushdown-only path).
     */
    @MustBeClosed
    public static <T> Stream<T> rows(
            @NonNull ParallelDecodeCoordinator coordinator,
            @NonNull Materializer<T> materializer,
            @NonNull ParquetSchema outputSchema,
            Predicate recordFilter) {
        return rows(coordinator, materializer, outputSchema, recordFilter, batch -> {});
    }

    /**
     * Same as {@link #rows(ParallelDecodeCoordinator, Materializer, ParquetSchema, Predicate)}, plus a
     * {@code batchObserver} notified the moment each freshly pulled batch becomes the one live batch. Tests use it to
     * assert that at most one batch is resident at a time.
     */
    @MustBeClosed
    static <T> Stream<T> rows(
            @NonNull ParallelDecodeCoordinator coordinator,
            @NonNull Materializer<T> materializer,
            @NonNull ParquetSchema outputSchema,
            Predicate recordFilter,
            @NonNull Consumer<ParquetRecordBatch> batchObserver) {
        RowIterator<T> iterator =
                new RowIterator<>(coordinator, materializer, outputSchema, recordFilter, batchObserver);
        Spliterator<T> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /**
     * Counts rows matching {@code predicate} across the coordinator's batches with no materialization. Each batch is
     * evaluated columnar-style ({@link VectorizedPredicateEvaluator}) and its matching-row bitset cardinality is
     * summed. Every batch is closed as it is consumed; closing the stream cascades to the coordinator.
     */
    public static long countMatching(@NonNull ParallelDecodeCoordinator coordinator, @NonNull Predicate predicate) {
        long total = 0L;
        try (Stream<ParquetRecordBatch> batches = batches(coordinator)) {
            Iterator<ParquetRecordBatch> it = batches.iterator();
            while (it.hasNext()) {
                try (ParquetRecordBatch batch = it.next()) {
                    total += VectorizedPredicateEvaluator.eval(predicate, batch).cardinality();
                }
            }
        }
        return total;
    }

    // ---- iterators ----

    /**
     * Pulls one {@link ParquetRecordBatch} at a time. Advances to the next decoded row group when the current one is
     * drained, closing the previous row group's undelivered batches before pulling the next one from the coordinator.
     */
    private static final class BatchIterator implements Iterator<ParquetRecordBatch>, AutoCloseable {

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

        private DecodedRowGroup currentRowGroup;
        private Predicate currentFilter;
        private ParquetRecordBatch currentBatch;
        private int rowIndex;
        private int batchRowCount;

        private boolean hasComputedNext;
        private T next;

        RowIterator(
                ParallelDecodeCoordinator coordinator,
                Materializer<T> materializer,
                ParquetSchema outputSchema,
                Predicate recordFilter,
                Consumer<ParquetRecordBatch> batchObserver) {
            this.coordinator = coordinator;
            this.materializer = materializer;
            this.outputSchema = outputSchema;
            this.recordFilter = recordFilter;
            this.batchObserver = batchObserver;
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

        /** Materializes the next surviving row in the current batch, or {@code null} once its rows are exhausted. */
        private T scanCurrentBatch() {
            while (rowIndex < batchRowCount) {
                BatchRowAccessor accessor = new BatchRowAccessor(currentBatch, rowIndex);
                rowIndex++;
                if (currentFilter == null || RecordLevelEvaluator.test(currentFilter, accessor::get)) {
                    return materializer.materialize(outputSchema, accessor);
                }
            }
            return null;
        }

        /** Closes the drained batch and makes the next non-empty batch current, or returns {@code false} at the end. */
        private boolean pullNextBatch() {
            closeCurrentBatch();
            while (true) {
                if (currentRowGroup != null && currentRowGroup.hasNext()) {
                    currentBatch = currentRowGroup.next();
                    batchObserver.accept(currentBatch);
                    rowIndex = 0;
                    batchRowCount = currentBatch.rowCount();
                    if (batchRowCount > 0) {
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
                rowGroup.close();
            }
        }

        private static void closeBestEffort(Runnable close) {
            try {
                close.run();
            } catch (RuntimeException _) {
                // best-effort; one failed close must not skip the rest
            }
        }
    }
}
