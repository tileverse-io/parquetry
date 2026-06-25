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
/**
 * Neutral, dependency-free observability vocabulary for parquetry reads and writes.
 *
 * <p>One collection mechanism feeds two views. The pull report ({@code ParquetFileReader.explainAnalyze}) drains a read
 * and annotates the explain plan with measured execution; the push stream delivers the same events live to a
 * {@link io.tileverse.parquetry.observe.QueryObserver} (read) or {@link io.tileverse.parquetry.observe.WriteObserver}
 * (write) attached through the read or write options.
 *
 * <h2>Boundary-only firing</h2>
 *
 * <p>Observer callbacks fire only at query, row-group, and tier boundaries with immutable aggregate payloads
 * ({@link io.tileverse.parquetry.observe.QueryStarted}, {@link io.tileverse.parquetry.observe.RowGroupRead},
 * {@link io.tileverse.parquetry.observe.QueryStats}, and the write-side
 * {@link io.tileverse.parquetry.observe.RowGroupFlushed} / {@link io.tileverse.parquetry.observe.WriteStats}). No
 * callback ever fires inside a page, batch, or value loop. {@code onRowGroupPlanned} fires once per (row group, pruning
 * tier); {@code onRowGroupRead} once per decoded row group; {@code onQueryStarted} and {@code onQueryFinished} once per
 * query. The hot per-row decode loop holds at most one hoisted-boolean-guarded counter increment and calls no observer.
 *
 * <h2>No-observer neutrality</h2>
 *
 * <p>Every interface has a no-op null object: {@link io.tileverse.parquetry.observe.QueryObserver#NONE},
 * {@link io.tileverse.parquetry.observe.WriteObserver#NONE}, and
 * {@link io.tileverse.parquetry.observe.FetchAccumulator} {@code .NONE}. When the options hold {@code NONE} no
 * collector is composed, no payload is allocated, and the decode and encode paths run exactly as they do without
 * observability. Observability is opt-in: a composite observer is built only when a real observer is attached, and its
 * fan-out walks a plain array (the enhanced-for over an array allocates no iterator) at the boundary frequencies above,
 * never per row.
 *
 * <h2>Thread-safety and merging</h2>
 *
 * <p>{@code onRowGroupRead} may arrive concurrently from decode threads; observer implementations must be thread-safe.
 * The provided {@link io.tileverse.parquetry.observe.QueryStatsCollector} is (lock-free adders plus a guarded tier
 * map). Every payload record is immutable and exposes a {@code combine} merge for aggregating per-file results without
 * a shared mutable collector, which a future multi-file reader can use.
 */
package io.tileverse.parquetry.observe;
