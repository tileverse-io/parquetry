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
package io.tileverse.parquetry.probes;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Columnar read-path comparison of parquetry, parquet-java 1.17.0, and DuckDB over one local Parquet file, full scan
 * only, touching every leaf value without assembling records.
 *
 * <p>This is the orchestrator: it drives measurement and prints the table. The per-engine scan-and-consume logic lives
 * in {@link ColumnarEngine} implementations ({@link ParquetryColumnarEngine}, {@link ParquetJavaColumnarEngine},
 * {@link DuckDbColumnarEngine}).
 *
 * <p>This is the columnar sibling of {@link ReadComparisonProbe}. That probe compares the row APIs, which folds
 * record-assembly cost into every engine. This probe compares the columnar APIs to isolate raw decode throughput and
 * memory: parquetry reads vectorized batches with the typed leaf accessors, parquet-java reads each leaf column with a
 * {@code ColumnReader}, and DuckDB streams the result as Arrow batches and consumes every vector value. No records are
 * assembled on any side.
 *
 * <p>All three engines touch every leaf value and fold it into a sink to defeat dead-code elimination; row counts must
 * match as a correctness check. parquetry and parquet-java decode on-heap. DuckDB decodes into off-heap Arrow buffers
 * and the probe consumes those vectors on-heap: that consumption churns the heap like the others (shown in the heap
 * columns), while {@code duckMem} reports the peak of DuckDB's off-heap Arrow buffers, the footprint the heap columns
 * cannot see.
 *
 * <p>Only the full scan is measured. Filters are row-semantic and out of scope for a columnar comparison.
 *
 * <p>Peak heap is read from {@code MemoryPoolMXBean} after each run and reflects heap occupancy including not-yet-
 * collected garbage at the run's {@code -Xmx} rather than the live working set; read it as an upper bound. The decisive
 * memory signal is whether the scan completes at a pod-sized heap (try {@code -Xmx1g}).
 *
 * <h2>Running</h2>
 *
 * <p>Runs as a {@code main()} app from the shaded {@code probes.jar} ({@code java -jar probes.jar columnar}),
 * configured by {@code parquetry.probe.*} system properties; {@code run-probe-docker.sh --probe columnar} drives it
 * under real container limits.
 *
 * <ul>
 *   <li>{@code parquetry.probe.file}: the file path (required).
 *   <li>{@code parquetry.probe.engines}: comma-separated engines (default all of
 *       {@code parquetry,parquet-java,duckdb}).
 *   <li>{@code parquetry.probe.warmup} / {@code .measure}: warmup and measured run counts (defaults {@code 1} /
 *       {@code 3}).
 *   <li>{@code parquetry.probe.concurrency}: number of full scans to run at once (default {@code 1}). When greater than
 *       {@code 1} each engine launches that many scans per wave (each DuckDB scan on its own connection) and the probe
 *       reports throughput, latency percentiles, and an OK/OOM/ERROR status.
 * </ul>
 */
public final class ColumnarReadComparisonProbe {

    private Path file;
    private int warmupRuns;
    private int measuredRuns;
    private int concurrency;
    private boolean silent;
    private boolean analyze;

    /** Runs the columnar-path read comparison, configured from {@code parquetry.probe.*} system properties. */
    public static void main(String[] args) throws Exception {
        if (System.getProperty("parquetry.probe.file") == null) {
            IO.println("Set -Dparquetry.probe.file=<parquet file> to run the columnar read comparison probe.");
            return;
        }
        ColumnarReadComparisonProbe probe = new ColumnarReadComparisonProbe();
        probe.configure(Config.fromSystemProperties());
        probe.run();
    }

    private void configure(Config config) {
        this.file = config.file();
        this.warmupRuns = config.warmupRuns();
        this.measuredRuns = config.measuredRuns();
        this.concurrency = config.concurrency();
        this.silent = config.silent();
        this.analyze = config.analyze();
    }

    private void run() throws Exception {
        note("Columnar read-path comparison over %s (%.1f MiB), full scan"
                .formatted(file, Files.size(file) / (1024.0 * 1024.0)));
        note("JVM availableProcessors=%d, maxHeap=%d MiB, concurrency=%d%n"
                .formatted(
                        Runtime.getRuntime().availableProcessors(),
                        Runtime.getRuntime().maxMemory() / (1024 * 1024),
                        concurrency));

        List<ColumnarEngine> engines = engines();
        try {
            if (concurrency > 1) {
                runConcurrent(engines);
            } else {
                runSequential(engines);
            }
        } finally {
            engines.forEach(ColumnarReadComparisonProbe::closeQuietly);
        }
        long checksum = engines.stream().mapToLong(ColumnarEngine::checksum).sum();
        note("%n(consumed checksum %d)".formatted(checksum));
        if (analyze) {
            engines.stream()
                    .map(ColumnarEngine::analyzeStats)
                    .flatMap(Optional::stream)
                    .findFirst()
                    .ifPresent(stats -> note("%n%s".formatted(ProbeStats.format(stats))));
        }
    }

    private List<ColumnarEngine> engines() {
        List<ColumnarEngine> engines = new ArrayList<>();
        for (String name : new String[] {"parquetry", "parquet-java", "duckdb"}) {
            if (engineEnabled(name)) {
                engines.add(ColumnarEngine.of(name, file, analyze));
            }
        }
        return engines;
    }

    private static void closeQuietly(ColumnarEngine engine) {
        try {
            engine.close();
        } catch (RuntimeException e) {
            System.err.println("closing " + engine.name() + " failed: " + e);
        }
    }

    /** Prints an explanatory line that frames the table; suppressed under {@code --silent} (table-only output). */
    private void note(String message) {
        if (!silent) {
            IO.println(message);
        }
    }

    private void runSequential(List<ColumnarEngine> engines) {
        List<Row> rows = new ArrayList<>();
        for (ColumnarEngine engine : engines) {
            rows.add(measure(engine));
        }
        printTable(rows);
    }

    private void runConcurrent(List<ColumnarEngine> engines) {
        note("Concurrency: %d full scans in flight per engine%n".formatted(concurrency));
        List<ConcurrentRow> rows = new ArrayList<>();
        for (ColumnarEngine engine : engines) {
            rows.add(measureConcurrent(engine));
        }
        printConcurrentTable(rows);
    }

    /** Honors {@code parquetry.probe.engines} (comma-separated) to run a subset. */
    private boolean engineEnabled(String engine) {
        String selected = System.getProperty("parquetry.probe.engines");
        if (selected == null || selected.isBlank()) {
            return true;
        }
        return Arrays.stream(selected.split(",")).map(String::trim).anyMatch(engine::equals);
    }

    private Row measure(ColumnarEngine engine) {
        try {
            for (int i = 0; i < warmupRuns; i++) {
                engine.scan();
            }
            ProbeMeasurement.settle(); // collect the prior engine's and warmup garbage before measuring this engine
            List<Long> wallNanos = new ArrayList<>(measuredRuns);
            long rows = 0L;
            long peakHeapBytes = 0L;
            long allocBytes = 0L;
            for (int i = 0; i < measuredRuns; i++) {
                ProbeMeasurement.resetPeakHeap();
                long allocBefore = ProbeMeasurement.totalAllocatedBytes();
                long start = System.nanoTime();
                rows = engine.scan();
                wallNanos.add(System.nanoTime() - start);
                allocBytes = ProbeMeasurement.totalAllocatedBytes() - allocBefore;
                peakHeapBytes = Math.max(peakHeapBytes, ProbeMeasurement.peakHeapBytes());
            }
            return Row.of(
                    engine.name(),
                    rows,
                    ProbeMeasurement.medianMillis(wallNanos),
                    peakHeapBytes,
                    allocBytes,
                    engine.offHeapPeakBytes());
        } catch (Exception e) {
            System.err.printf("%n[%s] failed:%n", engine.name());
            e.printStackTrace();
            return Row.skipped(engine.name(), e.getMessage());
        }
    }

    private ConcurrentRow measureConcurrent(ColumnarEngine engine) {
        try {
            ProbeMeasurement.Outcome outcome =
                    ProbeMeasurement.runConcurrent(concurrency, warmupRuns, measuredRuns, engine::scan);
            return new ConcurrentRow(engine.name(), outcome, null);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return ConcurrentRow.failed(engine.name(), "interrupted");
        } catch (RuntimeException e) {
            return ConcurrentRow.failed(engine.name(), e.getMessage());
        }
    }

    // --- output ---

    private void printTable(List<Row> rows) {
        String header = String.format(
                "%-14s %12s %12s %12s %12s %12s",
                "engine", "rows", "wall(ms)", "alloc(MB)", "peakHeap(MB)", "duckMem(MB)");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (Row row : rows) {
            IO.println(row.format());
        }
        if (silent) {
            return;
        }
        IO.println();
        IO.println("Full scan, columnar APIs, no record assembly on any side; every leaf value touched.");
        IO.println(
                "wall(ms) is the raw decode+consume cost: parquetry reads typed vectors per batch, parquet-java reads each leaf with a ColumnReader, DuckDB consumes its Arrow vectors. Row counts must match across engines.");
        IO.println(
                "alloc(MB) and peakHeap(MB) are heap churn and occupancy during the run; for DuckDB they are the on-heap consumption of its Arrow vectors, while duckMem is the off-heap peak of those Arrow buffers.");
        IO.println("peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; an upper bound.");
        IO.println("duckMem(MB) is DuckDB's peak off-heap Arrow buffer memory; the heap columns cannot see it.");
    }

    private void printConcurrentTable(List<ConcurrentRow> rows) {
        String header = String.format(
                "%-14s %8s %12s %10s %10s %10s %12s %12s %8s",
                "engine", "rows", "req/s", "p50(ms)", "p95(ms)", "max(ms)", "peakHeap(MB)", "alloc(MB)", "status");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (ConcurrentRow row : rows) {
            IO.println(row.format());
        }
        printErrorCauses(rows);
        if (silent) {
            return;
        }
        IO.println();
        IO.println("All scans ran %d at once, released together; metrics aggregate every scan".formatted(concurrency));
        IO.println("req/s is measured scans / summed wave wall; p50/p95/max are per-scan latencies.");
        IO.println(
                "peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; the decisive signal is whether status stays OK (not OOM) at a pod-sized heap. alloc(MB) is total churn. DuckDB's off-heap Arrow buffers are not in these heap columns.");
    }

    /** Prints the failure cause for every engine that did not scan cleanly; nothing when all engines passed. */
    private void printErrorCauses(List<ConcurrentRow> rows) {
        boolean anyPrinted = false;
        for (ConcurrentRow row : rows) {
            String detail = row.errorDetail();
            if (detail == null) {
                continue;
            }
            if (!anyPrinted) {
                IO.println();
                anyPrinted = true;
            }
            IO.println("cause [%s]: %s".formatted(row.engine(), detail));
        }
    }

    // --- supporting types ---

    /** One printed line: an engine's full-scan result. */
    private record Row(
            String engine,
            long rows,
            double wallMillis,
            long peakHeapBytes,
            long allocBytes,
            long offHeapPeakBytes,
            String skipReason) {

        static Row of(
                String engine,
                long rows,
                double wallMillis,
                long peakHeapBytes,
                long allocBytes,
                long offHeapPeakBytes) {
            return new Row(engine, rows, wallMillis, peakHeapBytes, allocBytes, offHeapPeakBytes, null);
        }

        static Row skipped(String engine, String reason) {
            return new Row(engine, 0L, 0.0, 0L, 0L, 0L, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-14s   skipped: %s", engine, skipReason);
            }
            String alloc = allocBytes > 0 ? String.format("%.1f", allocBytes / (1024.0 * 1024.0)) : "-";
            String peakHeap = peakHeapBytes > 0 ? String.format("%.1f", peakHeapBytes / (1024.0 * 1024.0)) : "-";
            String duckMem = offHeapPeakBytes > 0 ? String.format("%.1f", offHeapPeakBytes / (1024.0 * 1024.0)) : "-";
            return String.format(
                    "%-14s %12d %12.1f %12s %12s %12s", engine, rows, wallMillis, alloc, peakHeap, duckMem);
        }
    }

    /** One printed line of the concurrency table: an engine's aggregate full-scan result. */
    private record ConcurrentRow(String engine, ProbeMeasurement.Outcome outcome, String skipReason) {

        static ConcurrentRow failed(String engine, String reason) {
            return new ConcurrentRow(engine, null, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-14s   skipped: %s", engine, skipReason);
            }
            String peakHeap = String.format("%.1f", outcome.peakHeapBytes() / (1024.0 * 1024.0));
            String alloc = String.format("%.1f", outcome.allocBytes() / (1024.0 * 1024.0));
            return String.format(
                    "%-14s %8d %12.1f %10.1f %10.1f %10.1f %12s %12s %8s",
                    engine,
                    outcome.rowsPerRead(),
                    outcome.throughputPerSecond(),
                    outcome.p50Millis(),
                    outcome.p95Millis(),
                    outcome.maxMillis(),
                    peakHeap,
                    alloc,
                    statusLabel());
        }

        /** OK, or the failure kind annotated with how many scans failed. */
        private String statusLabel() {
            if (outcome.status() == ProbeMeasurement.Status.OK) {
                return "OK";
            }
            return String.format("%s(%d failed)", outcome.status(), outcome.failed());
        }

        /** The failure cause to print beneath the table, or null when the engine scanned cleanly. */
        String errorDetail() {
            if (skipReason != null) {
                return skipReason;
            }
            if (outcome.status() == ProbeMeasurement.Status.OK) {
                return null;
            }
            String cause = outcome.firstError();
            return cause != null ? cause : statusLabel() + ", no cause captured";
        }
    }

    /** Resolved probe inputs. */
    private record Config(
            Path file, int warmupRuns, int measuredRuns, int concurrency, boolean silent, boolean analyze) {

        static Config fromSystemProperties() {
            String path = System.getProperty("parquetry.probe.file");
            Path file = Path.of(path);
            if (!Files.isRegularFile(file)) {
                throw new IllegalArgumentException("parquetry.probe.file is not a regular file: " + path);
            }
            return new Config(
                    file,
                    intProperty("parquetry.probe.warmup", 1),
                    intProperty("parquetry.probe.measure", 3),
                    intProperty("parquetry.probe.concurrency", 1),
                    Boolean.getBoolean("parquetry.probe.silent"),
                    Boolean.getBoolean("parquetry.probe.analyze"));
        }

        private static int intProperty(String key, int fallback) {
            String value = System.getProperty(key);
            return value == null ? fallback : Integer.parseInt(value);
        }
    }
}
