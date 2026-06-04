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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

import io.tileverse.parquetry.filter.PruningDecision;
import io.tileverse.parquetry.internal.read.DecodeBudget;
import io.tileverse.parquetry.internal.read.DecodeExecutor;
import io.tileverse.parquetry.internal.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.internal.read.FetchBudget;
import io.tileverse.parquetry.io.SegmentPool;

import lombok.NonNull;

/**
 * Tunables for a single {@code ParquetDataset.read()} call.
 *
 * <p>Filter toggles default to ON; turn them off to bypass a tier (useful for measuring effectiveness or working around
 * a known-bad statistic). The {@code pruningDecisionListener} receives one {@link PruningDecision} per (row group,
 * tier) pair as the pipeline runs - the same vocabulary as {@code ExplainPlan}.
 *
 * <p>The {@code segmentPool} backs every column-chunk fetch and every per-page decompression buffer; defaults to
 * {@link SegmentPool#getDefault()}. See the package documentation for the streaming memory contract that motivates the
 * pool.
 *
 * <p>Two memory dials bound a read. {@code fetchBudget} caps the off-heap bytes a read may speculatively prefetch
 * (counted against the container's memory limit, outside {@code -Xmx}); {@code decodeBudget} caps the on-heap bytes a
 * read may hold in speculatively decoded batches (inside {@code -Xmx}). The in-order current row group is exempt from
 * both; a read therefore always makes progress, and only decode-ahead is bounded. Peak process memory is approximately
 * {@code maxHeap + fetchBudget + retained pool + JVM native baseline}. The recommended way to set both on a container
 * is {@link DecodeBudget#ofMaxMemoryFraction(double)} / {@link FetchBudget#ofMaxMemoryFraction(double)}: one fraction
 * policy auto-scales across pods of different sizes.
 *
 * <p>Reads execute synchronously on the caller's thread. Callers that want parallelism issue concurrent
 * {@code read(...)} calls against the dataset (which is thread-safe), or wrap the returned
 * {@link java.util.stream.Stream} themselves. Caching or prefetching of column-chunk bytes belongs to the
 * {@link io.tileverse.parquetry.io.ByteRangeSource} implementation, not the reader.
 *
 * @param useStatsFilter run the STATS tier
 * @param useDictionaryFilter run the DICTIONARY tier
 * @param useColumnIndexFilter run the COLUMN_INDEX tier
 * @param useBloomFilter run the BLOOM_FILTER tier (when loaded)
 * @param useRecordLevelFilter run the RECORD_LEVEL tier inline during record assembly
 * @param useLateMaterialization on the row {@code read(...)} path over flat columns, decode the output columns only for
 *     rows that match the predicate (two-phase decode); when off, decode every surviving row's output columns and drop
 *     non-matches at materialization. Honored only when {@code useRecordLevelFilter} is on and the predicate is
 *     non-trivial; otherwise it has no effect.
 * @param pruningDecisionListener called once per per-row-group tier outcome; never {@code null} (defaults to no-op)
 * @param decryptionKeyRetriever supplied by the encryption module; empty when the file isn't encrypted
 * @param segmentPool source of pooled native segments for column-chunk fetch and per-page decompression
 * @param batchSize maximum row count per emitted batch on the {@code readBatches(...)} path; empty means each batch is
 *     bounded only by the natural page row count
 * @param fetchBudget shared, process-wide cap on the in-flight bytes a read may speculatively prefetch
 * @param decodeBudget shared, process-wide cap on the on-heap bytes a read may hold in speculatively decoded batches
 * @param maxCoalesceGap largest byte gap between adjacent column chunks that the planner will bridge into one read
 * @param maxCoalescedSpan largest byte span a single coalesced range may grow to before the planner opens a new range
 * @param prefetchDepth how many upcoming row groups the prefetcher tries to fetch ahead of the consumed one
 * @param maxConcurrentFetchesPerRead upper bound on row-group fetches running concurrently within one read
 * @param decodeExecutor shared, process-wide CPU pool that decodes row groups in parallel
 * @param maxDecodeAheadPerRead row groups one read may decode ahead of consumption; 0 decodes serially inline
 */
public record ReadOptions(
        boolean useStatsFilter,
        boolean useDictionaryFilter,
        boolean useColumnIndexFilter,
        boolean useBloomFilter,
        boolean useRecordLevelFilter,
        boolean useLateMaterialization,
        @NonNull Consumer<PruningDecision> pruningDecisionListener,
        @NonNull Optional<DecryptionKeyRetriever> decryptionKeyRetriever,
        @NonNull SegmentPool segmentPool,
        @NonNull OptionalInt batchSize,
        @NonNull FetchBudget fetchBudget,
        @NonNull DecodeBudget decodeBudget,
        int maxCoalesceGap,
        int maxCoalescedSpan,
        int prefetchDepth,
        int maxConcurrentFetchesPerRead,
        @NonNull DecodeExecutor decodeExecutor,
        int maxDecodeAheadPerRead) {

    public ReadOptions {
        if (batchSize.isPresent() && batchSize.getAsInt() <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0 when present, got " + batchSize.getAsInt());
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
        if (maxDecodeAheadPerRead < 0) {
            throw new IllegalArgumentException("maxDecodeAheadPerRead must be >= 0, got " + maxDecodeAheadPerRead);
        }
    }

    /** Sensible defaults: all tiers on, no listener, no decryption, shared {@link SegmentPool#getDefault()}. */
    public static final ReadOptions DEFAULTS = builder().build();

    public static Builder builder() {
        return new Builder();
    }

    /** Fluent builder for {@link ReadOptions}. */
    public static final class Builder {

        private boolean useStatsFilter = true;
        private boolean useDictionaryFilter = true;
        private boolean useColumnIndexFilter = true;
        private boolean useBloomFilter = true;
        private boolean useRecordLevelFilter = true;
        private boolean useLateMaterialization = true;
        private Consumer<PruningDecision> pruningDecisionListener = _ -> {};
        private Optional<DecryptionKeyRetriever> decryptionKeyRetriever = Optional.empty();
        private SegmentPool segmentPool = SegmentPool.getDefault();
        private OptionalInt batchSize = OptionalInt.empty();
        private FetchBudget fetchBudget = FetchBudget.defaultBudget();
        private DecodeBudget decodeBudget = DecodeBudget.defaultBudget();
        private int maxCoalesceGap = 1 << 20;
        private int maxCoalescedSpan = 8 << 20;
        private int prefetchDepth = 2;
        private int maxConcurrentFetchesPerRead = 4;
        private DecodeExecutor decodeExecutor = DecodeExecutor.shared();
        private int maxDecodeAheadPerRead = 2;

        private Builder() {}

        public Builder useStatsFilter(boolean v) {
            this.useStatsFilter = v;
            return this;
        }

        public Builder useDictionaryFilter(boolean v) {
            this.useDictionaryFilter = v;
            return this;
        }

        public Builder useColumnIndexFilter(boolean v) {
            this.useColumnIndexFilter = v;
            return this;
        }

        public Builder useBloomFilter(boolean v) {
            this.useBloomFilter = v;
            return this;
        }

        public Builder useRecordLevelFilter(boolean v) {
            this.useRecordLevelFilter = v;
            return this;
        }

        public Builder useLateMaterialization(boolean v) {
            this.useLateMaterialization = v;
            return this;
        }

        public Builder pruningDecisionListener(@NonNull Consumer<PruningDecision> v) {
            this.pruningDecisionListener = v;
            return this;
        }

        public Builder decryptionKeyRetriever(DecryptionKeyRetriever v) {
            this.decryptionKeyRetriever = Optional.ofNullable(v);
            return this;
        }

        public Builder segmentPool(@NonNull SegmentPool v) {
            this.segmentPool = v;
            return this;
        }

        /**
         * Caps the row count of every emitted batch on the {@code readBatches(...)} path. Pass a positive integer to
         * enable; the default leaves batches bounded only by the natural page row count. Has no effect on the row-API
         * {@code read(...)} path - that path flattens batches into rows regardless.
         */
        public Builder batchSize(int maxRows) {
            if (maxRows <= 0) {
                throw new IllegalArgumentException("batchSize must be > 0, got " + maxRows);
            }
            this.batchSize = OptionalInt.of(maxRows);
            return this;
        }

        public Builder fetchBudget(@NonNull FetchBudget v) {
            this.fetchBudget = v;
            return this;
        }

        public Builder decodeBudget(@NonNull DecodeBudget v) {
            this.decodeBudget = v;
            return this;
        }

        public Builder maxCoalesceGap(int v) {
            if (v < 0) {
                throw new IllegalArgumentException("maxCoalesceGap must be >= 0, got " + v);
            }
            this.maxCoalesceGap = v;
            return this;
        }

        public Builder maxCoalescedSpan(int v) {
            if (v <= 0) {
                throw new IllegalArgumentException("maxCoalescedSpan must be > 0, got " + v);
            }
            this.maxCoalescedSpan = v;
            return this;
        }

        public Builder prefetchDepth(int v) {
            if (v < 0) {
                throw new IllegalArgumentException("prefetchDepth must be >= 0, got " + v);
            }
            this.prefetchDepth = v;
            return this;
        }

        public Builder maxConcurrentFetchesPerRead(int v) {
            if (v <= 0) {
                throw new IllegalArgumentException("maxConcurrentFetchesPerRead must be > 0, got " + v);
            }
            this.maxConcurrentFetchesPerRead = v;
            return this;
        }

        public Builder decodeExecutor(@NonNull DecodeExecutor v) {
            this.decodeExecutor = v;
            return this;
        }

        public Builder maxDecodeAheadPerRead(int v) {
            if (v < 0) {
                throw new IllegalArgumentException("maxDecodeAheadPerRead must be >= 0, got " + v);
            }
            this.maxDecodeAheadPerRead = v;
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(
                    useStatsFilter,
                    useDictionaryFilter,
                    useColumnIndexFilter,
                    useBloomFilter,
                    useRecordLevelFilter,
                    useLateMaterialization,
                    pruningDecisionListener,
                    decryptionKeyRetriever,
                    segmentPool,
                    batchSize,
                    fetchBudget,
                    decodeBudget,
                    maxCoalesceGap,
                    maxCoalescedSpan,
                    prefetchDepth,
                    maxConcurrentFetchesPerRead,
                    decodeExecutor,
                    maxDecodeAheadPerRead);
        }
    }
}
