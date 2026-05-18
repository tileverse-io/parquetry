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
package io.tileverse.parquetry.read;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

import io.tileverse.parquetry.filter.PruningDecision;

import io.tileverse.io.ByteBufferPool;

/**
 * Tunables for a single {@code Dataset.read()} call.
 *
 * <p>Filter toggles default to ON; turn them off to bypass a tier (useful for measuring effectiveness or working around
 * a known-bad statistic). Concurrency knobs default to {@link ConcurrencyMode#AUTO}, prefetch window 2, and an
 * unbounded virtual-thread pool capped by {@code maxConcurrency}. The {@code pruningDecisionListener} receives one
 * {@link PruningDecision} per (row group, tier) pair as the pipeline runs - the same vocabulary as {@code ExplainPlan}.
 *
 * <p>The {@code byteBufferPool} backs every column-chunk fetch and every per-page decompression buffer; defaults to
 * {@link ByteBufferPool#getDefault()}. See the package documentation for the streaming memory contract that motivates
 * the pool.
 *
 * @param useStatsFilter run the STATS tier
 * @param useDictionaryFilter run the DICTIONARY tier
 * @param useColumnIndexFilter run the COLUMN_INDEX tier
 * @param useBloomFilter run the BLOOM_FILTER tier (when loaded)
 * @param useRecordLevelFilter run the RECORD_LEVEL tier inline during record assembly
 * @param concurrencyMode selects fan-out / prefetch strategies
 * @param prefetchWindow number of row groups to prefetch ahead of the consumer; 0 disables; default 2
 * @param maxConcurrency upper bound on virtual threads spawned per {@code read()} call; 0 means unbounded
 * @param pruningDecisionListener called once per per-row-group tier outcome; never {@code null} (defaults to no-op)
 * @param decryptionKeyRetriever supplied by the encryption module; empty when the file isn't encrypted
 * @param byteBufferPool source of pooled buffers for column-chunk fetch and per-page decompression
 */
public record ReadOptions(
        boolean useStatsFilter,
        boolean useDictionaryFilter,
        boolean useColumnIndexFilter,
        boolean useBloomFilter,
        boolean useRecordLevelFilter,
        ConcurrencyMode concurrencyMode,
        int prefetchWindow,
        int maxConcurrency,
        Consumer<PruningDecision> pruningDecisionListener,
        Optional<DecryptionKeyRetriever> decryptionKeyRetriever,
        ByteBufferPool byteBufferPool) {

    public ReadOptions {
        Objects.requireNonNull(concurrencyMode, "concurrencyMode");
        Objects.requireNonNull(pruningDecisionListener, "pruningDecisionListener");
        Objects.requireNonNull(decryptionKeyRetriever, "decryptionKeyRetriever");
        Objects.requireNonNull(byteBufferPool, "byteBufferPool");
        if (prefetchWindow < 0) {
            throw new IllegalArgumentException("prefetchWindow must be >= 0, got " + prefetchWindow);
        }
        if (maxConcurrency < 0) {
            throw new IllegalArgumentException("maxConcurrency must be >= 0, got " + maxConcurrency);
        }
    }

    /**
     * Sensible defaults: all tiers on, AUTO concurrency, prefetch=2, unbounded threads, no listener, no decryption,
     * shared {@link ByteBufferPool#getDefault() default pool}.
     */
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
        private ConcurrencyMode concurrencyMode = ConcurrencyMode.AUTO;
        private int prefetchWindow = 2;
        private int maxConcurrency;
        private Consumer<PruningDecision> pruningDecisionListener = _ -> {};
        private Optional<DecryptionKeyRetriever> decryptionKeyRetriever = Optional.empty();
        private ByteBufferPool byteBufferPool = ByteBufferPool.getDefault();

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

        public Builder concurrencyMode(ConcurrencyMode v) {
            this.concurrencyMode = Objects.requireNonNull(v, "concurrencyMode");
            return this;
        }

        public Builder prefetchWindow(int v) {
            this.prefetchWindow = v;
            return this;
        }

        public Builder maxConcurrency(int v) {
            this.maxConcurrency = v;
            return this;
        }

        public Builder pruningDecisionListener(Consumer<PruningDecision> v) {
            this.pruningDecisionListener = Objects.requireNonNull(v, "pruningDecisionListener");
            return this;
        }

        public Builder decryptionKeyRetriever(DecryptionKeyRetriever v) {
            this.decryptionKeyRetriever = Optional.ofNullable(v);
            return this;
        }

        public Builder byteBufferPool(ByteBufferPool v) {
            this.byteBufferPool = Objects.requireNonNull(v, "byteBufferPool");
            return this;
        }

        public ReadOptions build() {
            return new ReadOptions(
                    useStatsFilter,
                    useDictionaryFilter,
                    useColumnIndexFilter,
                    useBloomFilter,
                    useRecordLevelFilter,
                    concurrencyMode,
                    prefetchWindow,
                    maxConcurrency,
                    pruningDecisionListener,
                    decryptionKeyRetriever,
                    byteBufferPool);
        }
    }
}
