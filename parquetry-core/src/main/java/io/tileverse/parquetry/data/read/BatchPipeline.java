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
package io.tileverse.parquetry.data.read;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
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
        Stream<DecodedRowGroup> rowGroupStream = rowGroups(coordinator);
        return rowGroupStream.flatMap(rowGroup -> rowsFromRowGroup(rowGroup, materializer, outputSchema, recordFilter));
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

    /**
     * Streams the coordinator's decoded row groups in strict file order. Closing the returned stream cascades to the
     * coordinator, which drains in-flight decodes and closes the prefetcher.
     */
    @MustBeClosed
    private static Stream<DecodedRowGroup> rowGroups(ParallelDecodeCoordinator coordinator) {
        RowGroupIterator iterator = new RowGroupIterator(coordinator);
        Spliterator<DecodedRowGroup> spliterator =
                Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(iterator::close);
    }

    /**
     * Iterates one row group's batches row by row. The effective per-row filter is resolved once for the whole group:
     * {@code recordFilter} when the group still needs per-row evaluation, or no filter when its statistics already
     * proved every row matches.
     */
    private static <T> Stream<T> rowsFromRowGroup(
            DecodedRowGroup rowGroup,
            Materializer<T> materializer,
            ParquetSchema outputSchema,
            Predicate recordFilter) {
        Predicate effectiveFilter = rowGroup.recordEvalRequired() ? recordFilter : null;
        return drainBatches(rowGroup)
                .flatMap(batch -> rowsFromBatch(batch, materializer, outputSchema, effectiveFilter));
    }

    /** Streams a row group's batches in order; closing the stream closes any batches not yet drained. */
    private static Stream<ParquetRecordBatch> drainBatches(DecodedRowGroup rowGroup) {
        Iterator<ParquetRecordBatch> batchIterator = rowGroupBatchIterator(rowGroup);
        Spliterator<ParquetRecordBatch> spliterator =
                Spliterators.spliteratorUnknownSize(batchIterator, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(rowGroup::close);
    }

    private static Iterator<ParquetRecordBatch> rowGroupBatchIterator(DecodedRowGroup rowGroup) {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return rowGroup.hasNext();
            }

            @Override
            public ParquetRecordBatch next() {
                if (!rowGroup.hasNext()) {
                    throw new NoSuchElementException();
                }
                return rowGroup.next();
            }
        };
    }

    /**
     * Iterates one batch row by row. When {@code recordFilter} is non-null, each row is tested against it before
     * materialization; non-matching rows are neither materialized nor emitted. A null filter passes every row through
     * (the pushdown-only path). Rows are materialized through {@code outputSchema}, which may be a subset of the
     * decoded batch schema when the predicate references columns outside the caller's projection.
     */
    private static <T> Stream<T> rowsFromBatch(
            ParquetRecordBatch batch,
            Materializer<T> materializer,
            ParquetSchema outputSchema,
            Predicate recordFilter) {
        Stream<T> rows = IntStream.range(0, batch.rowCount())
                .mapToObj(rowIndex -> new BatchRowAccessor(batch, rowIndex))
                .filter(accessor -> recordFilter == null || RecordLevelEvaluator.test(recordFilter, accessor::get))
                .map(accessor -> materializer.materialize(outputSchema, accessor));
        return rows.onClose(batch::close);
    }

    // ---- iterator ----

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
     * Pulls one whole {@link DecodedRowGroup} at a time, in strict file order. The row path consumes a group's batches
     * with the per-group filter resolved once, hence iterating by group rather than by batch. Each yielded group's
     * batches are drained (and its undelivered batches closed) by the consuming stream; {@link #close()} closes the
     * last yielded group as a safety net for early abandonment and then closes the coordinator.
     */
    private static final class RowGroupIterator implements Iterator<DecodedRowGroup>, AutoCloseable {

        private final ParallelDecodeCoordinator coordinator;
        private DecodedRowGroup lastYielded;
        private DecodedRowGroup pending;

        RowGroupIterator(ParallelDecodeCoordinator coordinator) {
            this.coordinator = coordinator;
        }

        @Override
        public boolean hasNext() {
            if (pending != null) {
                return true;
            }
            DecodedRowGroup next;
            try {
                next = coordinator.next();
            } catch (IOException e) {
                throw new UncheckedIOException("Row group decode failed", e);
            }
            if (next == null) {
                return false;
            }
            pending = next;
            return true;
        }

        @Override
        public DecodedRowGroup next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            DecodedRowGroup rowGroup = pending;
            pending = null;
            lastYielded = rowGroup;
            return rowGroup;
        }

        @Override
        public void close() {
            closeIfPresent(pending);
            closeIfPresent(lastYielded);
            pending = null;
            lastYielded = null;
            coordinator.close();
        }

        private static void closeIfPresent(DecodedRowGroup rowGroup) {
            if (rowGroup != null) {
                rowGroup.close();
            }
        }
    }
}
