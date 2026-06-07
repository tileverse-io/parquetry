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

import java.nio.file.Path;

import io.tileverse.parquetry.internal.read.DecodeBudget;
import io.tileverse.parquetry.internal.read.DecodeExecutor;
import io.tileverse.parquetry.internal.read.DiskBudget;
import io.tileverse.parquetry.internal.read.FetchBudget;
import io.tileverse.parquetry.internal.read.FetchSpillStore;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.io.limits.IoLimits;
import io.tileverse.parquetry.io.limits.ResourceLimits;

import lombok.NonNull;

/**
 * The process-wide read resources and I/O tuning a {@link ParquetDataset} reads against, bound once at
 * {@code ParquetDataset.open(...)} and shared across datasets on a pod. {@link #defaultRuntime()} is elastic: every
 * size derives from the container's measured limits (heap, cores, free disk), and one runtime fits a small or a large
 * pod while the read-path invariants hold at either size. Per-call query policy lives in {@link ReadOptions}, not here.
 *
 * <p>Immutable and composable: {@link #withPrefetchDepth(int)} and the other {@code withX} methods return a new runtime
 * that shares the same pool, executor, and budget instances and changes only the named scalar. The heavy resources are
 * never replaced on a live runtime.
 */
public record ParquetRuntime(
        @NonNull SegmentPool segmentPool,
        @NonNull FetchBudget fetchBudget,
        @NonNull DecodeBudget decodeBudget,
        @NonNull DiskBudget diskBudget,
        @NonNull Path spillDir,
        boolean spillEnabled,
        @NonNull DecodeExecutor decodeExecutor,
        int maxDecodeAhead,
        int maxCoalesceGap,
        int maxCoalescedSpan,
        int prefetchDepth,
        int maxConcurrentFetchesPerRead) {

    private static final long NOMINAL_AHEAD_SLOT_BYTES = 32L << 20;

    public ParquetRuntime {
        if (maxDecodeAhead < 0) {
            throw new IllegalArgumentException("maxDecodeAhead must be >= 0, got " + maxDecodeAhead);
        }
        if (maxCoalesceGap < 0) {
            throw new IllegalArgumentException("maxCoalesceGap must be >= 0, got " + maxCoalesceGap);
        }
        if (maxCoalescedSpan <= 0) {
            throw new IllegalArgumentException("maxCoalescedSpan must be > 0, got " + maxCoalescedSpan);
        }
        if (prefetchDepth < 0) {
            throw new IllegalArgumentException("prefetchDepth must be >= 0, got " + prefetchDepth);
        }
        if (maxConcurrentFetchesPerRead <= 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentFetchesPerRead must be > 0, got " + maxConcurrentFetchesPerRead);
        }
    }

    /** The elastic default runtime, sized to this pod's heap, cores, and free disk. */
    public static ParquetRuntime defaultRuntime() {
        return DefaultHolder.INSTANCE;
    }

    public static Builder builder() {
        return new Builder();
    }

    /** A copy sharing every resource, with {@code prefetchDepth} replaced. */
    public ParquetRuntime withPrefetchDepth(int prefetchDepth) {
        return new ParquetRuntime(
                segmentPool,
                fetchBudget,
                decodeBudget,
                diskBudget,
                spillDir,
                spillEnabled,
                decodeExecutor,
                maxDecodeAhead,
                maxCoalesceGap,
                maxCoalescedSpan,
                prefetchDepth,
                maxConcurrentFetchesPerRead);
    }

    /** A copy sharing every resource, with {@code maxConcurrentFetchesPerRead} replaced. */
    public ParquetRuntime withMaxConcurrentFetchesPerRead(int maxConcurrentFetchesPerRead) {
        return new ParquetRuntime(
                segmentPool,
                fetchBudget,
                decodeBudget,
                diskBudget,
                spillDir,
                spillEnabled,
                decodeExecutor,
                maxDecodeAhead,
                maxCoalesceGap,
                maxCoalescedSpan,
                prefetchDepth,
                maxConcurrentFetchesPerRead);
    }

    /** A copy sharing every resource, with {@code maxDecodeAhead} replaced. */
    public ParquetRuntime withMaxDecodeAhead(int maxDecodeAhead) {
        return new ParquetRuntime(
                segmentPool,
                fetchBudget,
                decodeBudget,
                diskBudget,
                spillDir,
                spillEnabled,
                decodeExecutor,
                maxDecodeAhead,
                maxCoalesceGap,
                maxCoalescedSpan,
                prefetchDepth,
                maxConcurrentFetchesPerRead);
    }

    static int decodeAheadDefault(int availableProcessors, long decodeBudgetCapacity) {
        long heapTerm = Math.max(2L, decodeBudgetCapacity / NOMINAL_AHEAD_SLOT_BYTES);
        long clamped = Math.clamp(availableProcessors, 2L, heapTerm);
        return (int) clamped;
    }

    /** Fluent builder; unset fields take the elastic pod-sized defaults. */
    public static final class Builder {

        private static final int UNSET_DECODE_AHEAD = -1;

        private SegmentPool segmentPool = SegmentPool.getDefault();
        private ResourceLimits resourceLimits = ResourceLimits.getDefault();
        private FetchBudget fetchBudget;
        private DecodeBudget decodeBudget = DecodeBudget.defaultBudget();
        private DiskBudget diskBudget;
        private Path spillDir;
        private boolean spillEnabled = true;
        private DecodeExecutor decodeExecutor = DecodeExecutor.shared();
        private int maxDecodeAhead = UNSET_DECODE_AHEAD;
        private int maxCoalesceGap = 1 << 20;
        private int maxCoalescedSpan = 8 << 20;
        private int prefetchDepth = 2;
        private int maxConcurrentFetchesPerRead = 4;

        private Builder() {}

        public Builder segmentPool(@NonNull SegmentPool segmentPool) {
            this.segmentPool = segmentPool;
            return this;
        }

        /**
         * The raw machine facts the unset fetch budget, disk budget, and spill directory derive from. An explicit
         * {@link #fetchBudget}, {@link #diskBudget}, or {@link #spillDir} overrides its derived counterpart.
         */
        public Builder resourceLimits(@NonNull ResourceLimits resourceLimits) {
            this.resourceLimits = resourceLimits;
            return this;
        }

        public Builder fetchBudget(@NonNull FetchBudget fetchBudget) {
            this.fetchBudget = fetchBudget;
            return this;
        }

        public Builder decodeBudget(@NonNull DecodeBudget decodeBudget) {
            this.decodeBudget = decodeBudget;
            return this;
        }

        public Builder diskBudget(@NonNull DiskBudget diskBudget) {
            this.diskBudget = diskBudget;
            return this;
        }

        public Builder spillDir(@NonNull Path spillDir) {
            this.spillDir = spillDir;
            return this;
        }

        public Builder spillEnabled(boolean spillEnabled) {
            this.spillEnabled = spillEnabled;
            return this;
        }

        public Builder decodeExecutor(@NonNull DecodeExecutor decodeExecutor) {
            this.decodeExecutor = decodeExecutor;
            return this;
        }

        public Builder maxDecodeAhead(int maxDecodeAhead) {
            if (maxDecodeAhead < 0) {
                throw new IllegalArgumentException("maxDecodeAhead must be >= 0, got " + maxDecodeAhead);
            }
            this.maxDecodeAhead = maxDecodeAhead;
            return this;
        }

        public Builder maxCoalesceGap(int maxCoalesceGap) {
            this.maxCoalesceGap = maxCoalesceGap;
            return this;
        }

        public Builder maxCoalescedSpan(int maxCoalescedSpan) {
            this.maxCoalescedSpan = maxCoalescedSpan;
            return this;
        }

        public Builder prefetchDepth(int prefetchDepth) {
            this.prefetchDepth = prefetchDepth;
            return this;
        }

        public Builder maxConcurrentFetchesPerRead(int maxConcurrentFetchesPerRead) {
            this.maxConcurrentFetchesPerRead = maxConcurrentFetchesPerRead;
            return this;
        }

        public ParquetRuntime build() {
            IoLimits limits = IoLimits.from(resourceLimits);
            FetchBudget resolvedFetchBudget =
                    fetchBudget != null ? fetchBudget : FetchBudget.ofBytes(limits.maxOffHeapBytes());
            DiskBudget resolvedDiskBudget =
                    diskBudget != null ? diskBudget : DiskBudget.ofBytes(limits.maxSpillBytes());
            Path resolvedSpillDir = spillDir != null ? spillDir : limits.spillDir();
            int resolvedDecodeAhead = maxDecodeAhead == UNSET_DECODE_AHEAD
                    ? decodeAheadDefault(Runtime.getRuntime().availableProcessors(), decodeBudget.capacity())
                    : maxDecodeAhead;
            return new ParquetRuntime(
                    segmentPool,
                    resolvedFetchBudget,
                    decodeBudget,
                    resolvedDiskBudget,
                    resolvedSpillDir,
                    spillEnabled,
                    decodeExecutor,
                    resolvedDecodeAhead,
                    maxCoalesceGap,
                    maxCoalescedSpan,
                    prefetchDepth,
                    maxConcurrentFetchesPerRead);
        }
    }

    /**
     * Builds the elastic default runtime once and, at that single point, removes fetch spill directories left behind by
     * processes that have since died. A crashed read can orphan a mapped spill file; sweeping here reclaims that disk
     * exactly once per JVM rather than on every read.
     */
    private static final class DefaultHolder {
        private static final ParquetRuntime INSTANCE = buildAndSweepOrphanSpills();

        private static ParquetRuntime buildAndSweepOrphanSpills() {
            ParquetRuntime runtime = builder().build();
            FetchSpillStore.sweepOrphans(runtime.spillDir());
            return runtime;
        }
    }
}
