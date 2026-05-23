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
package io.tileverse.parquetry.data;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;

import io.tileverse.parquetry.data.read.DecryptionKeyRetriever;
import io.tileverse.parquetry.filter.PruningDecision;

import io.tileverse.io.ByteBufferPool;
import lombok.NonNull;

/**
 * Tunables for a single {@code ParquetDataset.read()} call.
 *
 * <p>Filter toggles default to ON; turn them off to bypass a tier (useful for measuring effectiveness or working around
 * a known-bad statistic). The {@code pruningDecisionListener} receives one {@link PruningDecision} per (row group,
 * tier) pair as the pipeline runs - the same vocabulary as {@code ExplainPlan}.
 *
 * <p>The {@code byteBufferPool} backs every column-chunk fetch and every per-page decompression buffer; defaults to
 * {@link ByteBufferPool#getDefault()}. See the package documentation for the streaming memory contract that motivates
 * the pool.
 *
 * <p>Reads execute synchronously on the caller's thread. Callers that want parallelism issue concurrent
 * {@code read(...)} calls against the dataset (which is thread-safe), or wrap the returned
 * {@link java.util.stream.Stream} themselves. Prefetching of column-chunk bytes is the {@code RangeReader} caching
 * layer's responsibility, not the reader's.
 *
 * @param useStatsFilter run the STATS tier
 * @param useDictionaryFilter run the DICTIONARY tier
 * @param useColumnIndexFilter run the COLUMN_INDEX tier
 * @param useBloomFilter run the BLOOM_FILTER tier (when loaded)
 * @param useRecordLevelFilter run the RECORD_LEVEL tier inline during record assembly
 * @param pruningDecisionListener called once per per-row-group tier outcome; never {@code null} (defaults to no-op)
 * @param decryptionKeyRetriever supplied by the encryption module; empty when the file isn't encrypted
 * @param byteBufferPool source of pooled buffers for column-chunk fetch and per-page decompression
 * @param batchSize maximum row count per emitted batch on the {@code readBatches(...)} path; empty means each batch is
 *     bounded only by the natural page row count
 */
public record ReadOptions(
        boolean useStatsFilter,
        boolean useDictionaryFilter,
        boolean useColumnIndexFilter,
        boolean useBloomFilter,
        boolean useRecordLevelFilter,
        @NonNull Consumer<PruningDecision> pruningDecisionListener,
        @NonNull Optional<DecryptionKeyRetriever> decryptionKeyRetriever,
        @NonNull ByteBufferPool byteBufferPool,
        @NonNull OptionalInt batchSize) {

    public ReadOptions {
        if (batchSize.isPresent() && batchSize.getAsInt() <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0 when present, got " + batchSize.getAsInt());
        }
    }

    /** Sensible defaults: all tiers on, no listener, no decryption, shared {@link ByteBufferPool#getDefault()}. */
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
        private Consumer<PruningDecision> pruningDecisionListener = _ -> {};
        private Optional<DecryptionKeyRetriever> decryptionKeyRetriever = Optional.empty();
        private ByteBufferPool byteBufferPool = ByteBufferPool.getDefault();
        private OptionalInt batchSize = OptionalInt.empty();

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

        public Builder pruningDecisionListener(@NonNull Consumer<PruningDecision> v) {
            this.pruningDecisionListener = v;
            return this;
        }

        public Builder decryptionKeyRetriever(DecryptionKeyRetriever v) {
            this.decryptionKeyRetriever = Optional.ofNullable(v);
            return this;
        }

        public Builder byteBufferPool(@NonNull ByteBufferPool v) {
            this.byteBufferPool = v;
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

        public ReadOptions build() {
            return new ReadOptions(
                    useStatsFilter,
                    useDictionaryFilter,
                    useColumnIndexFilter,
                    useBloomFilter,
                    useRecordLevelFilter,
                    pruningDecisionListener,
                    decryptionKeyRetriever,
                    byteBufferPool,
                    batchSize);
        }
    }
}
