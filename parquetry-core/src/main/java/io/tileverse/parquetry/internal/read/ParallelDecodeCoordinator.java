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

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.SpillAccumulator;
import io.tileverse.parquetry.runtime.ComputeExecutor;
import io.tileverse.parquetry.runtime.DecodeBudget;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.schema.ParquetSchema;

import lombok.NonNull;

/**
 * Decodes row groups in file order, overlapping the decode of upcoming row groups on a shared {@link ComputeExecutor}
 * while the consumer drains the current one. Pulls fetched row groups from the {@link RowGroupPrefetcher} (fetch path
 * unchanged) and reorders out-of-order worker completions via an index-keyed window.
 *
 * <p>The single consumer thread calls {@link #next()} in order. Up to {@code maxDecodeAheadPerRead} speculative decodes
 * run on worker threads, each draining into a bounded hand-off whose live batches a shared {@link DecodeBudget}
 * controls; the shared executor bounds total decodes across reads. When no decode slot is free, the row group is
 * decoded inline on the consumer thread (serial fallback). {@code maxDecodeAheadPerRead == 0} decodes every row group
 * inline - the serial path.
 *
 * <p>Every row group honors the budget: a speculative batch that does not fit spills to disk rather than exempting the
 * row group, which keeps the read live even under a budget too small for a single batch. The consumer holds at most one
 * batch at a time, restoring a spilled one when it reaches it.
 */
public final class ParallelDecodeCoordinator implements AutoCloseable {

    /** Live batches a single row group may hold in its hand-off; the per-row-group memory bound (internal). */
    private static final int HANDOFF_CAPACITY = 2;

    private final RowGroupPrefetcher prefetcher;
    private final ComputeExecutor decodeExecutor;
    private final DecodeBudget decodeBudget;
    private final DecodeBufferAllocator decodeBufferAllocator;
    private final DiskBudget diskBudget;
    private final Path spillDir;
    private final boolean spillEnabled;
    private final int maxDecodeAheadPerRead;
    private final ParquetSchema projectedSchema;
    private final ParquetSchema fileSchema;
    private final OptionalInt batchSizeCap;
    private final List<Optional<RowMask>> rowMasks;
    private final List<Boolean> recordEvalRequired;
    private final Optional<LateMaterialization> lateMat;
    private final BatchForm batchForm;
    private final DecodeObservation observation;
    private final List<RowPositionSynthesis> rowPositions;
    private final RowGroupGate rowGroupGate;

    private final Map<Integer, DecodedRowGroup> window = new HashMap<>();
    private final int size;
    private int nextToConsume;
    private int nextToSubmit;

    // S107: aggregates the parallel-decode collaborators; a parameter object would only relocate the arity.
    @SuppressWarnings("java:S107")
    public ParallelDecodeCoordinator(
            @NonNull RowGroupPrefetcher prefetcher,
            @NonNull ComputeExecutor decodeExecutor,
            @NonNull DecodeBudget decodeBudget,
            @NonNull DecodeBufferAllocator decodeBufferAllocator,
            @NonNull DiskBudget diskBudget,
            @NonNull Path spillDir,
            boolean spillEnabled,
            int maxDecodeAheadPerRead,
            @NonNull ParquetSchema projectedSchema,
            @NonNull ParquetSchema fileSchema,
            @NonNull OptionalInt batchSizeCap,
            @NonNull List<Optional<RowMask>> rowMasks,
            @NonNull List<Boolean> recordEvalRequired,
            @NonNull Optional<LateMaterialization> lateMat,
            @NonNull BatchForm batchForm,
            @NonNull DecodeObservation observation,
            @NonNull List<RowPositionSynthesis> rowPositions,
            @NonNull Optional<RowGroupGate> rowGroupGate) {
        this.prefetcher = prefetcher;
        this.decodeExecutor = decodeExecutor;
        this.decodeBudget = decodeBudget;
        this.decodeBufferAllocator = decodeBufferAllocator;
        this.diskBudget = diskBudget;
        this.spillDir = spillDir;
        this.spillEnabled = spillEnabled;
        this.maxDecodeAheadPerRead = maxDecodeAheadPerRead;
        this.projectedSchema = projectedSchema;
        this.fileSchema = fileSchema;
        this.batchSizeCap = batchSizeCap;
        this.rowMasks = List.copyOf(rowMasks);
        this.recordEvalRequired = List.copyOf(recordEvalRequired);
        this.lateMat = lateMat;
        this.batchForm = batchForm;
        this.observation = observation;
        this.rowPositions = List.copyOf(rowPositions);
        this.rowGroupGate = rowGroupGate.orElse(null);
        this.size = prefetcher.size();
    }

    /** Returns the next decoded row group in file order, or {@code null} when all row groups have been consumed. */
    DecodedRowGroup next() throws IOException {
        if (nextToConsume >= size) {
            return null;
        }
        skipPaintedGroups();
        if (nextToConsume >= size) {
            return null;
        }
        submitAhead();
        int index = nextToConsume;
        nextToConsume++;
        DecodedRowGroup windowed = window.remove(index);
        if (windowed != null) {
            windowed.markDelivered();
            return windowed;
        }
        DecodedRowGroup inline = decodeInline(index);
        inline.markDelivered();
        return inline;
    }

    /**
     * Advances past any leading survivor row groups the gate skips, in strict file order on the consumer thread. By the
     * time the gate sees position {@code nextToConsume} every earlier group has been fully drained and painted by the
     * leaf tier, hence its bounds test reads correct accumulated coverage; the skip is monotone, and dropping a group
     * the speculation already decoded is safe. A skipped group performs no fetch and no decode: any speculatively
     * decoded entry is discarded, and {@code nextToSubmit} is advanced past it to keep {@link #submitAhead()} from
     * fetching it. When no gate is wired this is a pure no-op with no allocation.
     */
    private void skipPaintedGroups() {
        if (rowGroupGate == null) {
            return;
        }
        while (nextToConsume < size && rowGroupGate.skip(nextToConsume)) {
            DecodedRowGroup speculativelyDecoded = window.remove(nextToConsume);
            if (speculativelyDecoded != null) {
                speculativelyDecoded.close();
            }
            nextToConsume++;
            nextToSubmit = Math.max(nextToSubmit, nextToConsume);
        }
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
        boolean wantsTimings = observation.wantsTimings();
        RowGroupBatchDriver driver = buildDriver(fetch, rowMasks.get(index), index);
        BatchHandoff handoff = new BatchHandoff(HANDOFF_CAPACITY);
        BatchSpillStore spillStore =
                new BatchSpillStore(spillDir, diskBudget, projectedSchema, observation.spillAccumulator());
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, driver, decodeBudget, spillStore, spillEnabled, wantsTimings);
        DecodedRowGroup rowGroup =
                new DecodedRowGroup(source, evalRequiredFor(index), observationFor(index, fetch.fetchNanos()));
        try {
            Future<Void> task = decodeExecutor.submitAcquired(() -> {
                source.runProducer();
                return null;
            });
            source.attachProducerTask(task);
        } catch (RejectedExecutionException _) {
            // The slot was released by submitAcquired; fall back to inline streaming over the same driver.
            return new DecodedRowGroup(
                    new InlineBatchSource(driver, wantsTimings),
                    evalRequiredFor(index),
                    observationFor(index, fetch.fetchNanos()));
        }
        return rowGroup;
    }

    private DecodedRowGroup decodeInline(int index) throws IOException {
        RowGroupFetch fetch = prefetcher.take(index);
        nextToSubmit = Math.max(nextToSubmit, index + 1);
        RowGroupBatchDriver driver = buildDriver(fetch, rowMasks.get(index), index);
        return new DecodedRowGroup(
                new InlineBatchSource(driver, observation.wantsTimings()),
                evalRequiredFor(index),
                observationFor(index, fetch.fetchNanos()));
    }

    private RowGroupBatchDriver buildDriver(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        if (lateMat.isPresent()) {
            return buildLateMaterializedDriver(fetch, mask, index);
        }
        return new ClassicRowGroupDriver(
                decodeBufferAllocator,
                fetch,
                projectedSchema,
                fileSchema,
                batchSizeCap,
                mask,
                batchForm,
                rowPositionFor(index));
    }

    /**
     * The row group's row-position synthesis inputs when synthesis is on, empty otherwise. The list is parallel to the
     * survivor list, hence indexed by the same survivor position the coordinator decodes in.
     */
    private Optional<RowPositionSynthesis> rowPositionFor(int index) {
        return rowPositions.isEmpty() ? Optional.empty() : Optional.of(rowPositions.get(index));
    }

    private RowGroupBatchDriver buildLateMaterializedDriver(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        LateMaterialization lm = lateMat.orElseThrow();
        LateMaterialization.PerRowGroup perRg = lm.perRowGroup().get(index);
        LateMaterializingRowGroupReader reader = new LateMaterializingRowGroupReader(
                decodeBufferAllocator,
                fetch.columns(),
                fileSchema,
                lm.outputSchema(),
                lm.predicateLeaves(),
                lm.predicate(),
                batchSizeCap,
                mask,
                perRg.outputOffsetIndexes(),
                perRg.numRows(),
                batchForm);
        return new LateMaterializedRowGroupDriver(fetch, reader);
    }

    private boolean evalRequiredFor(int index) {
        return !lateMat.isPresent() && recordEvalRequired.get(index);
    }

    /**
     * The per-row-group observation context for survivor position {@code index}, or {@code null} when no observer is
     * attached. A {@code null} keeps {@link DecodedRowGroup} on its byte-identical decode path. {@code index} is the
     * dense survivor position; the true file ordinal it reports comes from the observation's parallel ordinal list.
     * {@code fetchNanos} is the row group's measured fetch time, zero when timings are off.
     */
    private DecodedRowGroup.ReadObservation observationFor(int index, long fetchNanos) {
        if (observation == DecodeObservation.NONE) {
            return null;
        }
        return new DecodedRowGroup.ReadObservation(
                observation.observer(),
                observation.rowGroupIndices().get(index),
                observation.matchedEqualsDecoded(),
                observation.wantsTimings(),
                fetchNanos);
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

    /**
     * The observation context one read threads into the coordinator: the observer and the true file ordinal per
     * survivor (the list parallel to the survivor list). {@code NONE} is the not-observing sentinel; the coordinator
     * builds no per-row-group observation for it, keeping the decode path byte-identical. {@code matchedEqualsDecoded}
     * is {@code true} for the pushdown-only batches path and {@code false} for the record-filtering read and count
     * paths. {@code wantsTimings} mirrors the observer's opt-in; when off, no decode-side clock is read.
     */
    public record DecodeObservation(
            QueryObserver observer,
            List<Integer> rowGroupIndices,
            boolean matchedEqualsDecoded,
            boolean wantsTimings,
            SpillAccumulator spillAccumulator) {

        public static final DecodeObservation NONE =
                new DecodeObservation(QueryObserver.NONE, List.of(), false, false, SpillAccumulator.NONE);

        public DecodeObservation {
            rowGroupIndices = List.copyOf(rowGroupIndices);
        }
    }
}
