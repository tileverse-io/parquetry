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

/**
 * Per-phase CPU nanosecond sums for one read. These are sums, never elapsed wall-clock; fetch and decode overlap across
 * row groups. Present only when {@code QueryObserver.wantsTimings()}.
 *
 * <p>{@code pipelineNanos} is query-level (one filter-pipeline run); it appears in the aggregated
 * {@code QueryStats.cpuTimings} and reads as zero on each per-row-group {@code RowGroupRead.timings}.
 *
 * <p>{@code recordFilterNanos} covers record-level processing as a whole: predicate evaluation plus the per-row
 * materialization interleaved with it, which cannot be cheaply separated. The count and readBatches paths run no
 * record-level filter and report zero.
 */
public record PhaseTimings(long pipelineNanos, long fetchNanos, long decodeNanos, long recordFilterNanos) {

    public static final PhaseTimings ZERO = new PhaseTimings(0, 0, 0, 0);

    public PhaseTimings combine(PhaseTimings other) {
        return new PhaseTimings(
                pipelineNanos + other.pipelineNanos,
                fetchNanos + other.fetchNanos,
                decodeNanos + other.decodeNanos,
                recordFilterNanos + other.recordFilterNanos);
    }
}
