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
import io.tileverse.parquetry.internal.read.LateMaterialization;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator;
import io.tileverse.parquetry.internal.read.ParallelDecodeCoordinator.DecodeObservation;
import io.tileverse.parquetry.internal.read.RowGroupFetcher;
import io.tileverse.parquetry.internal.read.RowGroupPrefetcher;
import io.tileverse.parquetry.internal.read.RowGroupSurvivor;
import io.tileverse.parquetry.internal.read.RowMask;
import io.tileverse.parquetry.internal.read.RowPositionSynthesis;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.observe.FetchAccumulator;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * The runtime-bound read machinery for one file: the coalescing fetcher, the prefetch pipeline, the parallel decode
 * coordinator, and the decode buffer valve, all built from the reader's {@link ByteRangeSource}, file
 * {@link ParquetSchema}, and {@link ParquetRuntime}. Each {@code read} builds its own collaborators here; closing the
 * returned stream shuts the per-read fetch executor down.
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
            Optional<LateMaterialization> lateMat,
            BatchForm batchForm,
            FetchAccumulator accumulator,
            DecodeObservation observation,
            List<RowPositionSynthesis> rowPositions) {
        RowGroupPrefetcher prefetcher =
                newPrefetcher(survivors, projectedSchema, accumulator, observation.wantsTimings());
        List<Boolean> recordEvalRequired = recordEvalFlagsFor(survivors);
        DecodeBufferAllocator decodeBufferAllocator = newDecodeBufferAllocator();
        return new ParallelDecodeCoordinator(
                prefetcher,
                runtime.decodeExecutor(),
                runtime.decodeBudget(),
                decodeBufferAllocator,
                runtime.diskBudget(),
                runtime.spillDir(),
                runtime.spillEnabled(),
                runtime.maxDecodeAhead(),
                projectedSchema,
                fileSchema,
                options.batchSize(),
                decodeMasks,
                recordEvalRequired,
                lateMat,
                batchForm,
                observation,
                rowPositions);
    }

    /**
     * Builds the coalescing fetcher and the prefetch pipeline for one read. The prefetcher owns a fresh per-read
     * virtual-thread executor; closing the returned stream cascades to {@link RowGroupPrefetcher#close()}, which shuts
     * the executor down. No executor outlives the read.
     */
    private RowGroupPrefetcher newPrefetcher(
            List<RowGroupSurvivor> survivors,
            ParquetSchema projectedSchema,
            FetchAccumulator accumulator,
            boolean wantsTimings) {
        FetchSpillStore spillStore = new FetchSpillStore(runtime.spillDir(), runtime.diskBudget());
        FetchBufferAllocator mandatoryAllocator =
                new FetchBufferAllocator(runtime.segmentPool(), runtime.fetchBudget(), spillStore);
        RowGroupFetcher fetcher = new RowGroupFetcher(
                source,
                fileSchema,
                projectedSchema,
                runtime.segmentPool(),
                mandatoryAllocator,
                runtime.maxCoalesceGap(),
                runtime.maxCoalescedSpan(),
                accumulator);
        ExecutorService executor = Executors.newThreadPerTaskExecutor(
                Thread.ofVirtual().name("parquetry-fetch-", 0).factory());
        try {
            return new RowGroupPrefetcher(
                    survivors,
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
