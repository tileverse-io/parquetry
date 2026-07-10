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
package io.tileverse.parquetry.probes;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import io.tileverse.parquetry.observe.QueryObserver;
import io.tileverse.parquetry.observe.QueryStats;

/**
 * Aggregates the {@link QueryStats} of every read across one probe run, including the concurrent ones. Each finished
 * read delivers its stats to {@link #onQueryFinished}; the running total is folded with {@link QueryStats#combine}
 * under a lock-free compare-and-set, since concurrent reads finish on different threads. Attached only under
 * {@code --analyze} so a default run keeps the read path free of observer work.
 */
final class ProbeQueryObserver implements QueryObserver {

    private final AtomicReference<QueryStats> aggregate = new AtomicReference<>();

    @Override
    public void onQueryFinished(QueryStats stats) {
        aggregate.accumulateAndGet(
                stats, (running, finished) -> running == null ? finished : running.combine(finished));
    }

    /** The combined stats of every read so far, or empty when no read has finished. */
    Optional<QueryStats> snapshot() {
        return Optional.ofNullable(aggregate.get());
    }
}
