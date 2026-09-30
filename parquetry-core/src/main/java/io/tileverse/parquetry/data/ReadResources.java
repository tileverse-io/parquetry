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
package io.tileverse.parquetry.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.tileverse.parquetry.internal.read.BatchForm;
import io.tileverse.parquetry.internal.read.DecodeBufferAllocator;
import io.tileverse.parquetry.internal.read.FetchBufferAllocator;
import io.tileverse.parquetry.internal.read.FetchSpillStore;
import io.tileverse.parquetry.internal.read.MaskedScan;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.internal.read.RowGroupFetcher;
import io.tileverse.parquetry.internal.read.RowGroupGate;
import io.tileverse.parquetry.internal.read.RowGroupPlanner;
import io.tileverse.parquetry.internal.read.RowGroupPlans;
import io.tileverse.parquetry.internal.read.RowGroupPrefetcher;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.internal.read.RowMasks;
import io.tileverse.parquetry.internal.read.RowPositionSynthesis;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The runtime-bound read machinery for one file: the fetcher, the prefetch pipeline, the parallel decode coordinator,
 * and the decode buffer valve, all built from the reader's {@link ByteRangeSource}, file {@link ParquetSchema}, and
 * {@link ParquetRuntime}. Each {@code read} builds its own collaborators here; closing the returned stream shuts the
 * per-read fetch executor down.
 */
final class ReadResources {

    private final ByteRangeSource source;
    private final ParquetSchema fileSchema;
    private final ParquetRuntime runtime;

    ReadResources(ByteRangeSource source, ParquetSchema fileSchema, ParquetRuntime runtime) {
        this.source = source;
        this.fileSchema = fileSchema;
        this.runtime = runtime;
    }

    /**
     * Wraps the fetch prefetcher in a {@link ParallelDecodeCoordinator} that decodes row groups in parallel on the
     * shared decode pool while preserving file order. Closing the returned coordinator drains in-flight decodes and
     * cascades to {@link RowGroupPrefetcher#close()}; the per-read fetch executor still shuts down with the read.
     */
    @SuppressWarnings("java:S107") // decode-coordinator wiring needs every input; splitting it would obscure, not help
    ParallelDecodeCoordinator newDecodeCoordinator(
            List<RowGroupSurvivor> survivors,
            ParquetSchema projectedSchema,
            List<Optional<RowMask>> decodeMasks,
            ReadOptions options,
            Optional<MaskedScan> maskedScan,
            BatchForm batchForm,
            FetchAccumulator accumulator,
            DecodeObservation observation,
            List<RowPositionSynthesis> rowPositions,
            Optional<RowGroupGate> rowGroupGate,
            Optional<RowGroupPlanner> planner) {
        RowGroupPlans plans = newPlans(survivors, projectedSchema, decodeMasks, options, planner);
        RowGroupPrefetcher prefetcher =
                newPrefetcher(survivors, plans, projectedSchema, accumulator, observation.wantsTimings());
        List<Boolean> recordEvalRequired = recordEvalFlagsFor(survivors);
        DecodeBufferAllocator decodeBufferAllocator = newDecodeBufferAllocator();
        return new ParallelDecodeCoordinator(
                prefetcher,
                runtime.computeExecutor(),
                runtime.decodeBudget(),
                decodeBufferAllocator,
                runtime.diskBudget(),
                runtime.spillDir(),
                runtime.spillEnabled(),
                runtime.maxDecodeAhead(),
                projectedSchema,
                fileSchema,
                options.batchSize(),
                plans,
                recordEvalRequired,
                maskedScan,
                batchForm,
                observation,
                rowPositions,
                rowGroupGate);
    }

    /**
     * The read's per-row-group plans, shared by the prefetcher and the decode coordinator. Without a spatial planner
     * they are the filter pipeline's decode masks and nothing is dropped.
     */
    private RowGroupPlans newPlans(
            List<RowGroupSurvivor> survivors,
            ParquetSchema projectedSchema,
            List<Optional<RowMask>> decodeMasks,
            ReadOptions options,
            Optional<RowGroupPlanner> planner) {
        boolean pageNarrowedFetch = options.usePageNarrowedFetch();
        if (planner.isEmpty()) {
            return RowGroupPlans.unplanned(survivors, decodeMasks, pageNarrowedFetch);
        }
        RowGroupPlans.NarrowedMaskFactory maskFactory = narrowedMaskFactory(projectedSchema);
        return RowGroupPlans.planned(survivors, decodeMasks, pageNarrowedFetch, planner.orElseThrow(), maskFactory);
    }

    /**
     * Builds the mask that narrows a row group to the rows decided by its plan. The schema is the read's scan schema,
     * which covers the filter columns as well as the output ones: a filter column is fetched and decoded with the rest,
     * hence its leaves must be masked to the same rows as theirs. Only a flat scan whose every leaf exposes an offset
     * index can be masked; any other scan gets no mask, and its row group reads as the filter pipeline left it.
     */
    private RowGroupPlans.NarrowedMaskFactory narrowedMaskFactory(ParquetSchema scanSchema) {
        List<ColumnPath> scanLeaves = scanSchema.leafColumns();
        if (!RowMasks.allFlat(fileSchema, scanLeaves)) {
            return (_, _) -> Optional.empty();
        }
        return (survivor, rows) -> RowMasks.maskFor(survivor.chunks(), rows, scanLeaves);
    }

    /**
     * Builds the fetcher and the prefetch pipeline for one read. The prefetcher owns a fresh per-read virtual-thread
     * executor; closing the returned stream cascades to {@link RowGroupPrefetcher#close()}, which shuts the executor
     * down. No executor outlives the read.
     */
    private RowGroupPrefetcher newPrefetcher(
            List<RowGroupSurvivor> survivors,
            RowGroupPlans plans,
            ParquetSchema projectedSchema,
            FetchAccumulator accumulator,
            boolean wantsTimings) {
        FetchSpillStore spillStore = new FetchSpillStore(runtime.spillDir(), runtime.diskBudget());
        FetchBufferAllocator mandatoryAllocator =
                new FetchBufferAllocator(runtime.segmentPool(), runtime.fetchBudget(), spillStore);
        RowGroupFetcher fetcher = new RowGroupFetcher(
                source, fileSchema, projectedSchema, runtime.segmentPool(), mandatoryAllocator, accumulator);
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("parquetry-fetch-", 0).factory());
        try {
            return new RowGroupPrefetcher(
                    survivors,
                    plans,
                    fetcher,
                    runtime.fetchBudget(),
                    executor,
                    runtime.prefetchDepth(),
                    runtime.maxConcurrentFetchesPerRead(),
                    wantsTimings);
        } catch (RuntimeException e) {
            executor.shutdownNow();
            throw e;
        }
    }

    /**
     * The RAM-or-mmap valve a decode reserves its off-heap value buffers through. The RAM path reserves against the
     * runtime's off-heap decode budget; the spill path maps an on-disk file under the runtime's disk budget while the
     * native decode budget has no room.
     */
    private DecodeBufferAllocator newDecodeBufferAllocator() {
        FetchSpillStore decodeSpillStore = new FetchSpillStore(runtime.spillDir(), runtime.diskBudget());
        return new DecodeBufferAllocator(runtime.segmentPool(), runtime.offHeapDecodeBudget(), decodeSpillStore);
    }

    /**
     * Returns one flag per survivor, in survivor order: whether the read still has to test each decoded row against the
     * predicate. A MATCHED survivor reports {@code false}; its statistics already proved every row matches, hence the
     * row pipeline skips per-row evaluation for it.
     */
    private static List<Boolean> recordEvalFlagsFor(List<RowGroupSurvivor> survivors) {
        List<Boolean> flags = new ArrayList<>(survivors.size());
        for (RowGroupSurvivor survivor : survivors) {
            flags.add(survivor.recordEvalRequired());
        }
        return flags;
    }
}
