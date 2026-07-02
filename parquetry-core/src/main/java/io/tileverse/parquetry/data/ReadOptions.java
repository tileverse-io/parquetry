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

import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.runtime.ParquetRuntime;

import lombok.NonNull;

/**
 * Per-call query and output policy for a single {@code ParquetSource.read()} call. Read resources and I/O tuning (the
 * segment pool, fetch/decode/disk budgets, decode executor, spill settings, and prefetch tuning) live on
 * {@link ParquetRuntime}, bound once at {@code ParquetSource.open(...)}, not here.
 *
 * <p>Filter toggles default to ON; turn them off to bypass a tier (useful for measuring effectiveness or working around
 * a known-bad statistic). The {@code queryObserver} receives query and row-group boundary callbacks as the read runs;
 * it defaults to {@link QueryObserver#NONE} (no-op).
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
 * @param useLateMaterialization on the {@code read(...)} and {@code readBatches(...)} paths over flat columns, decode
 *     the output columns only for rows that match the predicate (two-phase decode); when off, decode every surviving
 *     row's output columns and drop non-matches at materialization. Honored only when {@code useRecordLevelFilter} is
 *     on and the predicate is non-trivial; otherwise it has no effect.
 * @param queryObserver receives query and row-group boundary callbacks; never {@code null} (defaults to
 *     {@link QueryObserver#NONE})
 * @param batchSize maximum row count per emitted batch on the {@code readBatches(...)} path; empty means each batch is
 *     bounded only by the natural page row count
 * @param spatialReadProbe A stateful, single-use spatial decimation probe consulted per structural unit during the
 *     read; empty by default. Never share a probe-bearing options instance across concurrent reads; {@code DEFAULTS}
 *     never holds one.
 */
public record ReadOptions(
        boolean useStatsFilter,
        boolean useDictionaryFilter,
        boolean useColumnIndexFilter,
        boolean useBloomFilter,
        boolean useRecordLevelFilter,
        boolean useLateMaterialization,
        @NonNull QueryObserver queryObserver,
        @NonNull OptionalInt batchSize,
        @NonNull Optional<SpatialReadProbe> spatialReadProbe) {

    public ReadOptions {
        if (batchSize.isPresent() && batchSize.getAsInt() <= 0) {
            throw new IllegalArgumentException("batchSize must be > 0 when present, got " + batchSize.getAsInt());
        }
    }

    /** Sensible defaults: all tiers on, no observer, natural batch size. */
    public static final ReadOptions DEFAULTS = builder().build();

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A {@link Builder} pre-populated with this instance's field values. Mutating one field and calling {@code build()}
     * yields a copy differing only in that field.
     */
    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.useStatsFilter = useStatsFilter;
        builder.useDictionaryFilter = useDictionaryFilter;
        builder.useColumnIndexFilter = useColumnIndexFilter;
        builder.useBloomFilter = useBloomFilter;
        builder.useRecordLevelFilter = useRecordLevelFilter;
        builder.useLateMaterialization = useLateMaterialization;
        builder.queryObserver = queryObserver;
        builder.batchSize = batchSize;
        builder.spatialReadProbe = spatialReadProbe;
        return builder;
    }

    /** Fluent builder for {@link ReadOptions}. */
    public static final class Builder {

        private boolean useStatsFilter = true;
        private boolean useDictionaryFilter = true;
        private boolean useColumnIndexFilter = true;
        private boolean useBloomFilter = true;
        private boolean useRecordLevelFilter = true;
        private boolean useLateMaterialization = true;
        private QueryObserver queryObserver = QueryObserver.NONE;
        private OptionalInt batchSize = OptionalInt.empty();
        private Optional<SpatialReadProbe> spatialReadProbe = Optional.empty();

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

        public Builder queryObserver(@NonNull QueryObserver v) {
            this.queryObserver = v;
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

        /**
         * Installs a stateful, single-use spatial decimation probe consulted per structural unit during the read. The
         * resulting options instance must not be shared across concurrent reads.
         */
        public Builder spatialReadProbe(@NonNull SpatialReadProbe probe) {
            this.spatialReadProbe = Optional.of(probe);
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
                    queryObserver,
                    batchSize,
                    spatialReadProbe);
        }
    }
}
