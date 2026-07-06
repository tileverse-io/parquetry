/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.observe;

import io.tileverse.parquetry.filter.explain.PruningDecision;

/**
 * Push-side read observability. Callbacks fire only at query and row-group boundaries with immutable aggregate
 * payloads; no callback ever fires inside a page or batch loop.
 *
 * <p>Threading: {@code onQueryStarted} and {@code onQueryFinished} fire on the calling thread;
 * {@code onRowGroupPlanned} fires on the pipeline thread; {@code onRowGroupRead} fires after each row group completes,
 * on the consuming thread for a single-file read, and may fire concurrently once reads are parallelized across files.
 * Implementations must be thread-safe; the provided {@link QueryStatsCollector} is.
 */
public interface QueryObserver {

    QueryObserver NONE = new QueryObserver() {};

    /** Opt-in to per-phase CPU timing. Default off keeps the read path free of {@code nanoTime} calls. */
    default boolean wantsTimings() {
        return false;
    }

    default void onQueryStarted(QueryStarted event) {}

    default void onRowGroupPlanned(int rowGroup, PruningDecision decision) {}

    default void onRowGroupRead(RowGroupRead event) {}

    default void onQueryFinished(QueryStats stats) {}

    static QueryObserver composite(QueryObserver... observers) {
        return new CompositeQueryObserver(observers);
    }
}
