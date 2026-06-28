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

import java.nio.file.Path;
import java.util.Optional;

import io.tileverse.parquetry.observe.QueryStats;

/**
 * One engine's columnar full-scan arm: read the file column-major and touch every leaf value without assembling
 * records. The probe drives measurement; the engine only knows how to scan and consume.
 */
interface ColumnarEngine extends AutoCloseable {

    String name();

    /** Scans the whole file and consumes (touches) every leaf value, returning the row count. */
    long scan() throws Exception;

    /** Running total folded from every consumed value, kept so the JIT cannot elide the decode work. */
    long checksum();

    /**
     * Peak off-heap bytes the engine allocated for decode buffers, or 0 when it decodes on-heap. DuckDB decodes into
     * off-heap Arrow buffers the JVM heap columns cannot see; reporting their peak makes that hidden footprint visible.
     */
    default long offHeapPeakBytes() {
        return 0L;
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

    static ColumnarEngine of(String name, Path file, boolean analyze) {
        return switch (name) {
            case "parquetry" -> new ParquetryColumnarEngine(file, analyze);
            case "parquet-java" -> new ParquetJavaColumnarEngine(file);
            case "duckdb" -> new DuckDbColumnarEngine(file);
            default -> throw new IllegalArgumentException("unknown engine: " + name);
        };
    }
}
