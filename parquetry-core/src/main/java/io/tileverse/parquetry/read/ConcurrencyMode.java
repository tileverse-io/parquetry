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

/**
 * Selects which concurrency optimizations the reader applies.
 *
 * <ul>
 *   <li>{@link #AUTO} - resolved at {@code ParquetDataset.open()} time from the {@code RangeReader}'s locality hint:
 *       cloud readers get {@link #FULL}, local/in-memory readers get {@link #SYNC}.
 *   <li>{@link #SYNC} - single-threaded read. Best for local files and tiny datasets.
 *   <li>{@link #FAN_OUT_ONLY} - parallel column fetch within a row group, but row groups are processed sequentially.
 *   <li>{@link #PREFETCH_ONLY} - row group N+1 prefetched while the consumer iterates N; one virtual thread per column
 *       inside the row group is disabled (single-threaded fetch).
 *   <li>{@link #FULL} - both column fan-out and row group prefetch. Default for cloud readers.
 * </ul>
 */
public enum ConcurrencyMode {
    AUTO,
    SYNC,
    FAN_OUT_ONLY,
    PREFETCH_ONLY,
    FULL
}
