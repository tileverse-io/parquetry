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
package io.tileverse.parquetry.columnar;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.errorprone.annotations.MustBeClosed;

import io.tileverse.parquetry.materializer.Materializer;

/**
 * The lazy batch-to-row flatten every row-shaped read composes over its batch stream. A row handed out here is a
 * flyweight view over its batch's memory, valid until the stream advances past it; the flatten therefore closes a batch
 * only when it moves on to the next one (or when the returned stream closes), never earlier.
 *
 * <p>{@code Stream.flatMap} cannot provide that ordering: pulled through {@code Stream.iterator()}, it drains a whole
 * inner stream into a buffer and closes the batch before the buffered row views are read. A closed batch's
 * segment-backed vectors return to the buffer pool, and any concurrent decode may recycle them, turning the buffered
 * views into reads of foreign memory.
 */
public final class BatchRows {

    private BatchRows() {}

    /** Materializes one row of a batch; the generic seam that lets the flatten's ordering be tested in isolation. */
    @FunctionalInterface
    interface BatchRow<B, T> {
        T materialize(B batch, int rowIndex);
    }

    /**
     * Flattens the batches to rows via {@code materializer}, closing each batch when the stream advances past it and
     * the upstream {@code batches} stream when the returned stream closes.
     */
    @MustBeClosed
    public static <T> Stream<T> rows(Stream<ParquetRecordBatch> batches, Materializer<T> materializer) {
        return flatten(
                batches,
                ParquetRecordBatch::rowCount,
                (batch, row) -> materializer.materialize(batch.projectedSchema(), batch.materialize(row)),
                ParquetRecordBatch::close);
    }

    /**
     * The generic flatten engine. Emits {@code materialize(batch, row)} for every row of every batch in order;
     * {@code closeBatch} runs for a batch exactly once, when the flatten advances past its last row, skips it for
     * having no rows, or the returned stream closes while it is current. Batches the flatten never pulled are not its
     * to close; the upstream stream's own close hook owns them.
     */
    static <B, T> Stream<T> flatten(
            Stream<B> batches, ToIntFunction<B> rowCount, BatchRow<B, T> materialize, Consumer<B> closeBatch) {
        FlattenSpliterator<B, T> spliterator =
                new FlattenSpliterator<>(batches.iterator(), rowCount, materialize, closeBatch);
        return StreamSupport.stream(spliterator, /* parallel */ false).onClose(() -> {
            try {
                spliterator.closeCurrent();
            } finally {
                batches.close();
            }
        });
    }

    private static final class FlattenSpliterator<B, T> extends Spliterators.AbstractSpliterator<T> {

        private final Iterator<B> batches;
        private final ToIntFunction<B> rowCount;
        private final BatchRow<B, T> materialize;
        private final Consumer<B> closeBatch;

        private B current;
        private int row;
        private int rows;

        FlattenSpliterator(
                Iterator<B> batches, ToIntFunction<B> rowCount, BatchRow<B, T> materialize, Consumer<B> closeBatch) {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.batches = batches;
            this.rowCount = rowCount;
            this.materialize = materialize;
            this.closeBatch = closeBatch;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            while (true) {
                if (current != null && row >= rows) {
                    closeCurrent();
                }
                if (current == null) {
                    if (!batches.hasNext()) {
                        return false;
                    }
                    current = batches.next();
                    row = 0;
                    rows = rowCount.applyAsInt(current);
                    continue;
                }
                T value = materialize.materialize(current, row);
                row++;
                action.accept(value);
                return true;
            }
        }

        void closeCurrent() {
            B closing = current;
            current = null;
            if (closing != null) {
                closeBatch.accept(closing);
            }
        }
    }
}
