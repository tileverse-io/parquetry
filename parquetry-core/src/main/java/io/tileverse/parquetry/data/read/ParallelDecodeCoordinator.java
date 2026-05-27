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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Decodes row groups in file order, overlapping the decode of upcoming row groups on a shared {@link DecodeExecutor}
 * while the consumer drains the current one. Pulls fetched row groups from the {@link RowGroupPrefetcher} (fetch path
 * unchanged) and reorders out-of-order worker completions via an index-keyed window.
 *
 * <p>The single consumer thread calls {@link #next()} in order. Up to {@code maxDecodeAheadPerRead} decode tasks are in
 * flight per read (the window bound); the shared executor bounds total decodes across reads. When no decode slot is
 * free, the row group is decoded inline on the consumer thread (serial fallback). {@code maxDecodeAheadPerRead == 0}
 * decodes every row group inline - the serial path.
 */
public final class ParallelDecodeCoordinator implements AutoCloseable {

    private final RowGroupPrefetcher prefetcher;
    private final DecodeExecutor decodeExecutor;
    private final int maxDecodeAheadPerRead;
    private final ParquetSchema projectedSchema;
    private final ParquetSchema fileSchema;
    private final OptionalInt batchSizeCap;
    private final List<Optional<RowMask>> rowMasks;
    private final Optional<LateMaterialization> lateMat;

    private final Map<Integer, Future<DecodedRowGroup>> window = new HashMap<>();
    private final int size;
    private int nextToConsume;
    private int nextToSubmit;

    // S107: aggregates the parallel-decode collaborators; a parameter object would only relocate the arity.
    @SuppressWarnings("java:S107")
    public ParallelDecodeCoordinator(
            @NonNull RowGroupPrefetcher prefetcher,
            @NonNull DecodeExecutor decodeExecutor,
            int maxDecodeAheadPerRead,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap,
            @NonNull List<Optional<RowMask>> rowMasks,
            @NonNull Optional<LateMaterialization> lateMat) {
        this.prefetcher = prefetcher;
        this.decodeExecutor = decodeExecutor;
        this.maxDecodeAheadPerRead = maxDecodeAheadPerRead;
        this.projectedSchema = projectedSchema;
        this.fileSchema = fileSchema;
        this.batchSizeCap = batchSizeCap;
        this.rowMasks = List.copyOf(rowMasks);
        this.lateMat = lateMat;
        this.size = prefetcher.size();
    }

    /** Returns the next decoded row group in file order, or {@code null} when all row groups have been consumed. */
    DecodedRowGroup next() throws IOException {
        if (nextToConsume >= size) {
            return null;
        }
        submitAhead();
        int index = nextToConsume;
        nextToConsume++;
        Future<DecodedRowGroup> future = window.remove(index);
        if (future != null) {
            return join(future);
        }
        RowGroupFetch fetch = prefetcher.take(index);
        nextToSubmit = Math.max(nextToSubmit, index + 1);
        return decode(fetch, rowMasks.get(index), index);
    }

    private void submitAhead() throws IOException {
        while (window.size() < maxDecodeAheadPerRead && nextToSubmit < size) {
            if (!decodeExecutor.tryAcquire()) {
                return;
            }
            RowGroupFetch fetch;
            try {
                fetch = prefetcher.take(nextToSubmit);
            } catch (IOException | RuntimeException e) {
                decodeExecutor.release();
                throw e;
            }
            window.put(nextToSubmit, submitDecode(fetch, rowMasks.get(nextToSubmit), nextToSubmit));
            nextToSubmit++;
        }
    }

    private Future<DecodedRowGroup> submitDecode(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        try {
            return decodeExecutor.submitAcquired(() -> decode(fetch, mask, index));
        } catch (RejectedExecutionException _) {
            // The slot was released by submitAcquired; decode inline now and hand back a completed future.
            return CompletableFuture.completedFuture(decode(fetch, mask, index));
        }
    }

    @SuppressWarnings("MustBeClosed")
    private DecodedRowGroup decode(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        try (fetch) {
            if (lateMat.isPresent()) {
                return decodeLateMaterialized(fetch, mask, index);
            }
            List<ParquetRecordBatch> batches = new ArrayList<>();
            BatchRowGroupReader reader =
                    new BatchRowGroupReader(fetch.columns(), projectedSchema, fileSchema, batchSizeCap, mask);
            try {
                while (reader.hasMore()) {
                    batches.add(reader.nextBatch());
                }
            } catch (RuntimeException e) {
                closeAll(batches);
                throw e;
            } finally {
                reader.close();
            }
            return new DecodedRowGroup(batches);
        }
    }

    /**
     * Decodes one row group two-phase: evaluate the predicate over its columns, then materialize the output columns
     * only for the matching rows. The returned batches are already filtered, hence the row pipeline applies no further
     * record filter on this path.
     */
    private DecodedRowGroup decodeLateMaterialized(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        LateMaterialization lm = lateMat.orElseThrow();
        LateMaterialization.PerRowGroup perRg = lm.perRowGroup().get(index);
        LateMaterializingRowGroupReader reader = new LateMaterializingRowGroupReader(
                fetch.columns(),
                fileSchema,
                lm.outputSchema(),
                lm.predicateLeaves(),
                lm.predicate(),
                batchSizeCap,
                mask,
                perRg.outputOffsetIndexes(),
                perRg.numRows());
        return new DecodedRowGroup(reader.decodeAll());
    }

    private DecodedRowGroup join(Future<DecodedRowGroup> future) throws IOException {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while awaiting a decoded row group", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            if (cause instanceof UncheckedIOException uio) {
                throw uio;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new IOException("Row group decode failed", cause);
        }
    }

    @Override
    public void close() {
        for (Future<DecodedRowGroup> future : window.values()) {
            drainAndClose(future);
        }
        window.clear();
        prefetcher.close();
    }

    private static void drainAndClose(Future<DecodedRowGroup> future) {
        try {
            DecodedRowGroup rowGroup = future.get();
            rowGroup.close();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException _) {
            // The decode failed and already released its own batches and fetch.
        }
    }

    private static void closeAll(List<ParquetRecordBatch> batches) {
        for (ParquetRecordBatch batch : batches) {
            try {
                batch.close();
            } catch (RuntimeException _) {
                // best-effort
            }
        }
    }
}
