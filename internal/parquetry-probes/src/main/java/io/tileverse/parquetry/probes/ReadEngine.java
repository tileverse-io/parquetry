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
package io.tileverse.parquetry.probes;

import java.util.Optional;

import io.tileverse.parquetry.observe.QueryStats;

/**
 * One engine's read-and-consume arm of the comparison, built from a {@link ReadContext}. The probe drives measurement
 * (warmup, timing, heap, allocation) and calls {@link #read(Scenario)} for each pass; the engine only knows how to read
 * the file and materialize every value the way a real consumer would.
 */
interface ReadEngine extends AutoCloseable {

    String name();

    /** Reads the scenario and consumes (touches) every value of every surviving row, returning the row count. */
    long read(Scenario scenario) throws Exception;

    /** Running total folded from every consumed value, kept so the JIT cannot elide the materialization work. */
    long checksum();

    /**
     * Extra engine-reported metrics (DuckDB's profiler latency and off-heap buffer peak); empty for the JVM engines.
     */
    default Optional<DuckDbProfile> profile() {
        return Optional.empty();
    }

    /**
     * The aggregated {@link QueryStats} when running under {@code --analyze}; empty otherwise and for non-parquetry
     * engines.
     */
    default Optional<QueryStats> analyzeStats() {
        return Optional.empty();
    }

    @Override
    default void close() {}
}
