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
 *
 * <p>Decode-ahead is speculation on the fetch budget too: a row group is taken ahead only when its fetch was prefetched
 * or can be reserved now, and the coordinator stops submitting ahead when it cannot. Only the consumer's current row
 * group is fetched through the waiting, mandatory path, at a point where the read holds no other fetch buffer.
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
    private final RowGroupPlans plans;
    private final List<Boolean> recordEvalRequired;
    private final Optional<MaskedScan> maskedScan;
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
            @NonNull RowGroupPlans plans,
            @NonNull List<Boolean> recordEvalRequired,
            @NonNull Optional<MaskedScan> maskedScan,
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
        this.plans = plans;
        this.recordEvalRequired = List.copyOf(recordEvalRequired);
        this.maskedScan = maskedScan;
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
     * Advances past any leading survivor row groups dropped before consumption, in strict file order on the consumer
     * thread. Two questions decide it, in this order. The spatial plan of the group answers first, at the point where
     * its fetch plan would be built. The gate answers second, as the late check: by then every earlier group has been
     * fully drained and painted by the leaf tier, hence its bounds test reads correct accumulated coverage.
     *
     * <p>Both answers are monotone, and discarding a group already decoded by the speculation is safe. A skipped group
     * performs no fetch and no decode: any speculatively decoded entry is closed, and {@code nextToSubmit} is advanced
     * past it to keep {@link #submitAhead()} from fetching it. With unplanned plans and no gate this is a pure no-op
     * with no allocation.
     */
    private void skipPaintedGroups() {
        while (nextToConsume < size && (plans.dropped(nextToConsume) || gateSkips(nextToConsume))) {
            DecodedRowGroup speculativelyDecoded = window.remove(nextToConsume);
            if (speculativelyDecoded != null) {
                speculativelyDecoded.close();
            }
            prefetcher.discard(nextToConsume);
            nextToConsume++;
            nextToSubmit = Math.max(nextToSubmit, nextToConsume);
        }
    }

    private boolean gateSkips(int position) {
        return rowGroupGate != null && rowGroupGate.skip(position);
    }

    private void submitAhead() throws IOException {
        while (window.size() < maxDecodeAheadPerRead && nextToSubmit < size) {
            // Speculating over a dropped row group would fetch the very bytes that its plan decided not to read.
            if (plans.dropped(nextToSubmit)) {
                nextToSubmit++;
                continue;
            }
            if (!decodeExecutor.tryAcquire()) {
                return;
            }
            int index = nextToSubmit;
            Optional<RowGroupFetch> fetch;
            try {
                fetch = prefetcher.tryTake(index);
            } catch (IOException | RuntimeException e) {
                decodeExecutor.release();
                throw e;
            }
            if (fetch.isEmpty()) {
                // The fetch budget has no room for this row group now. Fetching it through the waiting path here would
                // hold the row groups already in the window while waiting; the consumer fetches it when it gets there.
                decodeExecutor.release();
                return;
            }
            window.put(index, submitSpeculative(fetch.orElseThrow(), index));
            nextToSubmit++;
        }
    }

    // S2095: ownership of the row group transfers to the caller (the window, drained or closed by the coordinator); on
    // rejection nothing was started, the empty hand-off needs no close, and the decoding source moves to the inline
    // fallback.
    @SuppressWarnings("java:S2095")
    private DecodedRowGroup submitSpeculative(RowGroupFetch fetch, int index) {
        DecodingBatchSource decoding = decodingSource(fetch, plans.decodeMask(index), index);
        BatchHandoff handoff = new BatchHandoff(HANDOFF_CAPACITY);
        BatchSpillStore spillStore =
                new BatchSpillStore(spillDir, diskBudget, projectedSchema, observation.spillAccumulator());
        StreamingBatchSource source =
                new StreamingBatchSource(handoff, decoding, decodeBudget, spillStore, spillEnabled);
        DecodedRowGroup rowGroup =
                new DecodedRowGroup(source, evalRequiredFor(index), observationFor(index, fetch.fetchNanos()));
        try {
            Future<Void> task = decodeExecutor.submitAcquired(() -> {
                source.runProducer();
                return null;
            });
            source.attachProducerTask(task);
        } catch (RejectedExecutionException _) {
            // The slot was released by submitAcquired; fall back to decoding inline through the same source.
            return new DecodedRowGroup(decoding, evalRequiredFor(index), observationFor(index, fetch.fetchNanos()));
        }
        return rowGroup;
    }

    private DecodedRowGroup decodeInline(int index) throws IOException {
        RowGroupFetch fetch = prefetcher.take(index);
        nextToSubmit = Math.max(nextToSubmit, index + 1);
        DecodingBatchSource decoding = decodingSource(fetch, plans.decodeMask(index), index);
        return new DecodedRowGroup(decoding, evalRequiredFor(index), observationFor(index, fetch.fetchNanos()));
    }

    /** The source decoding one fetched row group, which releases the fetch when closed. */
    private DecodingBatchSource decodingSource(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        RowGroupReader reader = readerFor(fetch, mask, index);
        return new DecodingBatchSource(reader, fetch, observation.wantsTimings());
    }

    /**
     * The reader of one fetched row group: the masked scan when the read evaluates the predicate in scan and the row
     * group still needs it, the projected columns over the rows to read otherwise.
     */
    private RowGroupReader readerFor(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        if (maskedScan.isPresent() && Boolean.TRUE.equals(recordEvalRequired.get(index))) {
            return maskedScanReader(fetch, mask, index);
        }
        return new BatchRowGroupReader(
                decodeBufferAllocator,
                fetch.columns(),
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

    /**
     * The masked scan reader for one row group: the predicate columns decode and evaluate per window, and the output
     * columns decode only the rows that survived.
     */
    private MaskedScanRowGroupReader maskedScanReader(RowGroupFetch fetch, Optional<RowMask> mask, int index) {
        MaskedScan scan = maskedScan.orElseThrow();
        return new MaskedScanRowGroupReader(
                decodeBufferAllocator,
                fetch.columns(),
                fileSchema,
                scan.outputSchema(),
                scan.filterLeaves(),
                scan.predicate(),
                batchSizeCap,
                mask,
                batchForm,
                rowPositionFor(index),
                plans.rowsToScan(index));
    }

    /**
     * Whether the pipeline still tests each decoded row against the predicate. A masked scan applied it exactly during
     * decode; a MATCHED row group had it proved by statistics.
     */
    private boolean evalRequiredFor(int index) {
        if (maskedScan.isPresent()) {
            return false;
        }
        return recordEvalRequired.get(index);
    }

    /**
     * The per-row-group observation context for the survivor at {@code position}, or {@code null} when no observer is
     * attached. A {@code null} keeps {@link DecodedRowGroup} on its byte-identical decode path. The events report the
     * position of the survivor in the file. {@code fetchNanos} is the row group's measured fetch time, zero when
     * timings are off.
     */
    private DecodedRowGroup.ReadObservation observationFor(int position, long fetchNanos) {
        if (observation == DecodeObservation.NONE) {
            return null;
        }
        int rowGroupIndex = plans.survivor(position).index();
        return new DecodedRowGroup.ReadObservation(
                observation.observer(),
                rowGroupIndex,
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
     * The observation context one read threads into the coordinator. {@code NONE} is the not-observing sentinel; the
     * coordinator builds no per-row-group observation for it, keeping the decode path byte-identical.
     * {@code matchedEqualsDecoded} is {@code true} for the pushdown-only batches path and {@code false} for the
     * record-filtering read and count paths. {@code wantsTimings} mirrors the observer's opt-in; when off, no
     * decode-side clock is read.
     */
    public record DecodeObservation(
            QueryObserver observer,
            boolean matchedEqualsDecoded,
            boolean wantsTimings,
            SpillAccumulator spillAccumulator) {

        public static final DecodeObservation NONE =
                new DecodeObservation(QueryObserver.NONE, false, false, SpillAccumulator.NONE);
    }
}
