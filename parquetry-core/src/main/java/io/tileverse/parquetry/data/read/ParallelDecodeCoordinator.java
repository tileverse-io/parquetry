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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Decodes row groups in file order, overlapping the decode of upcoming row groups on a shared {@link DecodeExecutor}
 * while the consumer drains the current one. Pulls fetched row groups from the {@link RowGroupPrefetcher} (fetch path
 * unchanged) and reorders out-of-order worker completions via an index-keyed window.
 *
 * <p>The single consumer thread calls {@link #next()} in order. Up to {@code maxDecodeAheadPerRead} speculative decodes
 * run on worker threads, each draining into a bounded hand-off whose live batches a shared {@link DecodeBudget}
 * controls; the shared executor bounds total decodes across reads. When no decode slot is free, the row group is
 * decoded inline on the consumer thread (serial fallback). {@code maxDecodeAheadPerRead == 0} decodes every row group
 * inline - the serial path.
 *
 * <p>The in-order current row group is never budget-controlled: when the consumer reaches a speculatively decoded row
 * group, the coordinator promotes it off the budget, which releases a worker parked on a reservation. That promotion is
 * what keeps the read live even under a budget too small for a single batch.
 */
public final class ParallelDecodeCoordinator implements AutoCloseable {

    /** Live batches a single row group may hold in its hand-off; the per-row-group memory bound (internal). */
    private static final int HANDOFF_CAPACITY = 2;

    private final RowGroupPrefetcher prefetcher;
    private final DecodeExecutor decodeExecutor;
    private final DecodeBudget decodeBudget;
    private final int maxDecodeAheadPerRead;
    private final ParquetSchema projectedSchema;
    private final ParquetSchema fileSchema;
    private final OptionalInt batchSizeCap;
    private final List<Optional<RowMask>> rowMasks;
    private final List<Boolean> recordEvalRequired;
    private final Optional<LateMaterialization> lateMat;

    private final Map<Integer, DecodedRowGroup> window = new HashMap<>();
    private final int size;
    private int nextToConsume;
    private int nextToSubmit;

    // S107: aggregates the parallel-decode collaborators; a parameter object would only relocate the arity.
    @SuppressWarnings("java:S107")
    public ParallelDecodeCoordinator(
            @NonNull RowGroupPrefetcher prefetcher,
            @NonNull DecodeExecutor decodeExecutor,
            @NonNull DecodeBudget decodeBudget,
            int maxDecodeAheadPerRead,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap,
            @NonNull List<Optional<RowMask>> rowMasks,
            @NonNull List<Boolean> recordEvalRequired,
            @NonNull Optional<LateMaterialization> lateMat) {
        this.prefetcher = prefetcher;
        this.decodeExecutor = decodeExecutor;
        this.decodeBudget = decodeBudget;
        this.maxDecodeAheadPerRead = maxDecodeAheadPerRead;
        this.projectedSchema = projectedSchema;
        this.fileSchema = fileSchema;
        this.batchSizeCap = batchSizeCap;
        this.rowMasks = List.copyOf(rowMasks);
        this.recordEvalRequired = List.copyOf(recordEvalRequired);
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
        DecodedRowGroup windowed = window.remove(index);
        if (windowed != null) {
            windowed.promote();
            return windowed;
        }
        return decodeInline(index);
    }

    private void submitAhead() throws IOException {
        while (window.size() < maxDecodeAheadPerRead && nextToSubmit < size) {
            if (!decodeExecutor.tryAcquire()) {
                return;
            }
            int index = nextToSubmit;
            RowGroupFetch fetch;
            try {
                fetch = prefetcher.take(index);
            } catch (IOException | RuntimeException e) {
                decodeExecutor.release();
                throw e;
            }
            window.put(index, submitSpeculative(fetch, index));
            nextToSubmit++;
        }
    }

    // S2095: ownership of the row group transfers to the caller (the window, drained or closed by the coordinator); on
    // rejection nothing was started, the empty hand-off needs no close, and the driver moves to the inline fallback.
    @SuppressWarnings("java:S2095")
    private DecodedRowGroup submitSpeculative(RowGroupFetch fetch, int index) {
        RowGroupBatchDriver driver = buildDriver(fetch, rowMasks.get(index), index);
        BatchHandoff handoff = new BatchHandoff(HANDOFF_CAPACITY);
        StreamingBatchSource source = new StreamingBatchSource(handoff, driver, decodeBudget, /*exempt*/ false);
        DecodedRowGroup rowGroup = new DecodedRowGroup(source, evalRequiredFor(index));
        try {
            Future<Void> task = decodeExecutor.submitAcquired(() -> {
                source.runProducer();
                return null;
            });
            source.attachProducerTask(task);
        } catch (RejectedExecutionException _) {
            // The slot was released by submitAcquired; fall back to inline streaming over the same driver.
            return new DecodedRowGroup(new InlineBatchSource(driver), evalRequiredFor(index));
        }
        return rowGroup;
    }

    private DecodedRowGroup decodeInline(int index) throws IOException {
        RowGroupFetch fetch = prefetcher.take(index);
        nextToSubmit = Math.max(nextToSubmit, index + 1);
        RowGroupBatchDriver driver = buildDriver(fetch, rowMasks.get(index), index);
        return new DecodedRowGroup(new InlineBatchSource(driver), evalRequiredFor(index));
    }

    private RowGroupBatchDriver buildDriver(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        if (lateMat.isPresent()) {
            return buildLateMaterializedDriver(fetch, mask, index);
        }
        return new ClassicRowGroupDriver(fetch, projectedSchema, fileSchema, batchSizeCap, mask);
    }

    private RowGroupBatchDriver buildLateMaterializedDriver(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
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
        return new LateMaterializedRowGroupDriver(fetch, reader);
    }

    private boolean evalRequiredFor(int index) {
        return !lateMat.isPresent() && recordEvalRequired.get(index);
    }

    @Override
    public void close() {
        for (DecodedRowGroup rowGroup : window.values()) {
            try {
                rowGroup.close();
            } catch (RuntimeException _) {
                // best-effort; close the remaining row groups even if one throws
            }
        }
        window.clear();
        prefetcher.close();
    }
}
