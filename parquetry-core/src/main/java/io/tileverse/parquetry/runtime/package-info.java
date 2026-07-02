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
 * Process-wide runtime resource governance for parquetry reads and writes.
 *
 * <p>{@link io.tileverse.parquetry.runtime.ParquetRuntime} is the single holder of the CPU and memory governors a
 * process shares across all concurrent operations. The elastic default
 * ({@link io.tileverse.parquetry.runtime.ParquetRuntime#defaultRuntime()}) is sized once to the pod's heap, cores, and
 * free disk; a {@code Builder} overrides individual governors for tests and embedding. These are public tuning
 * controls, not engine internals, which is why they live here rather than under {@code internal}.
 *
 * <h2>CPU governor</h2>
 *
 * <p>{@link io.tileverse.parquetry.runtime.ComputeExecutor} is a shared, process-wide pool of platform threads sized to
 * the core count. It is workload-neutral by design: CPU-bound engine work submits to it through a slot semaphore,
 * keeping total concurrency near the core count rather than spreading it over unbounded virtual threads. The pool never
 * queues - a caller that cannot acquire a slot runs the task inline, the never-break fallback. Its consumer today is
 * row-group decode.
 *
 * <h2>Memory and disk governors</h2>
 *
 * <p>Three budgets bound in-flight memory and spill. They are shared across all concurrent operations and additive,
 * keeping any single operation from starving the others: {@link io.tileverse.parquetry.runtime.FetchBudget} bounds
 * off-heap fetch buffers, {@link io.tileverse.parquetry.runtime.DecodeBudget} bounds heap held by decoded batches (a
 * second instance bounds off-heap decode values), and {@link io.tileverse.parquetry.runtime.DiskBudget} bounds spill
 * space. Each is sized once from the pod's limits, and the fetch accounting keeps native memory within
 * {@code -XX:MaxDirectMemorySize}.
 *
 * <p>The lower-level pooled-buffer and machine-fact primitives ({@code SegmentPool}, {@code IoLimits},
 * {@code ResourceLimits}) live in {@code parquetry-io} and are composed here into the runtime aggregate.
 */
package io.tileverse.parquetry.runtime;
