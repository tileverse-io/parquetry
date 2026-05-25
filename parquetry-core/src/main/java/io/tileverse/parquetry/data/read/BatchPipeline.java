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
     * Returns a closeable stream of rows materialized through {@code materializer}. Each batch is iterated row-by-row
     * and closed when its row stream ends; closing the outer stream cascades to the underlying batch iterator.
     */
    @MustBeClosed
    public static <T> Stream<T> rows(
            @NonNull ParallelDecodeCoordinator coordinator,
            @NonNull Materializer<T> materializer,
            @NonNull ParquetSchema outputSchema,
            Predicate recordFilter) {
        Stream<ParquetRecordBatch> batchStream = batches(coordinator);
        return batchStream.flatMap(batch -> rowsFromBatch(batch, materializer, outputSchema, recordFilter));
    }

    /**
     * Iterates one batch row by row. When {@code recordFilter} is non-null, each row is tested against it before
     * materialization, so non-matching rows are neither materialized nor emitted. A null filter passes every row
     * through (the pushdown-only path). Rows are materialized through {@code outputSchema}, which may be a subset of
     * the decoded batch schema when the predicate references columns outside the caller's projection.
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
}
