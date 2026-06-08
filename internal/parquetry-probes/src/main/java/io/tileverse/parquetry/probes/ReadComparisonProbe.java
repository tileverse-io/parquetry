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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Read-path comparison of parquetry, parquet-java 1.17.0, and DuckDB over one local Parquet file, under attribute and
 * spatial filters, materializing every surviving row.
 *
 * <p>This is the orchestrator: it resolves what the file supports, gates the scenarios, drives measurement (warmup,
 * timing, heap, allocation), and prints the table. The per-engine read-and-consume logic lives in {@link ReadEngine}
 * implementations ({@link ParquetryReadEngine}, {@link ParquetJavaReadEngine}, {@link DuckDbReadEngine}).
 *
 * <p>This is a characterization probe, not a JMH microbenchmark: it warms up, runs each engine a few times, and prints
 * a side-by-side table. It runs against any Parquet file: {@code NO_FILTER} always runs, the spatial scenarios
 * auto-enable when the file has a geometry column (GeoParquet primary column or a Geometry/Geography leaf), and the
 * attribute scenarios run when you name an attribute column and value. Scenarios whose inputs do not resolve are
 * skipped. Overture buildings is the reference dataset; see the module README.
 *
 * <p>All three engines read the same local file. Each materializes the full row (every projected column, nested
 * included) the way a streaming consumer does, without retaining records: parquetry walks each row's values in place,
 * parquet-java reads each {@code Group}, and DuckDB pulls every column of every {@code ResultSet} row with
 * {@code getObject}. Row-count parity across the three engines per scenario doubles as a correctness check.
 *
 * <p>Peak heap is read from {@code MemoryPoolMXBean} after each run: it reflects heap occupancy including not-yet-
 * collected garbage at a high {@code -Xmx} rather than the live working set, and should be read as an upper bound. The
 * decisive memory signal is whether a scenario completes at a pod-sized heap (try {@code -Xmx2g}). DuckDB's decode
 * buffers are off-heap; its profiler reports their peak ({@code duckMem}) and the engine scan latency
 * ({@code duckScan}) for context.
 *
 * <h2>Filter scenarios</h2>
 *
 * <ul>
 *   <li>{@code NO_FILTER}: full scan of the whole file.
 *   <li>{@code ATTRIBUTE}: equality on a column named via {@code parquetry.probe.attribute.column}/{@code .value}
 *       (skipped when unset).
 *   <li>{@code SPATIAL}: exact geometry intersection with a query diamond. The diamond's bounding box strictly
 *       over-selects, making the exact test non-trivial. parquetry pushes the exact JTS filter into the reader (bbox
 *       pruning then exact gate, the GeoTools/GeoServer path); parquet-java pushes a numeric prefilter on the
 *       {@code bbox} covering columns and re-checks exactness app-side with the same JTS test; DuckDB uses its native
 *       {@code ST_Intersects}.
 *   <li>{@code ATTRIBUTE_AND_SPATIAL}: both filters combined.
 * </ul>
 *
 * <h2>Running</h2>
 *
 * <p>Runs as a {@code main()} app from the shaded {@code probes.jar}, configured by {@code parquetry.probe.*} system
 * properties (it exits with a hint when {@code parquetry.probe.file} is unset). {@code run-probe-docker.sh} drives it
 * under real container CPU/memory limits; or run it directly:
 *
 * <pre>{@code
 * java -Dparquetry.probe.file=/path/to/file.parquet -jar probes.jar read
 * }</pre>
 *
 * <p>Tunable via system properties:
 *
 * <ul>
 *   <li>{@code parquetry.probe.file}: the file path (required).
 *   <li>{@code parquetry.probe.engines}: comma-separated engines (default {@code parquetry,parquet-java};
 *       {@code duckdb} is opt-in).
 *   <li>{@code parquetry.probe.attribute.column} / {@code .attribute.value}: enable the attribute scenarios with an
 *       equality on this column (skipped when either is unset).
 *   <li>{@code parquetry.probe.geometry.column}: the geometry column for the spatial scenarios (default: the GeoParquet
 *       primary column, else the first Geometry/Geography leaf).
 *   <li>{@code parquetry.probe.cx} / {@code .cy} / {@code .r}: query diamond centre and half-diagonal (default: a
 *       central region derived from the file's GeoParquet bbox; spatial scenarios are skipped when no envelope
 *       resolves).
 *   <li>{@code parquetry.probe.bbox.column}: the GeoParquet 1.1 bbox covering struct for parquet-java pushdown (default
 *       {@code bbox}; the pushdown is used only when {@code <prefix>.xmin/xmax/ymin/ymax} exist).
 *   <li>{@code parquetry.probe.warmup} / {@code .measure}: warmup and measured run counts (defaults {@code 1} /
 *       {@code 3}).
 *   <li>{@code parquetry.probe.concurrency}: reads to run at once (default {@code 1}). When greater than {@code 1} each
 *       engine/scenario pass launches that many reads per wave (each DuckDB read on its own connection, the pool model)
 *       and the probe reports throughput, latency percentiles, and an OK/OOM/ERROR status, modelling a pod admitting
 *       {@code 2 x cores} requests.
 * </ul>
 */
public final class ReadComparisonProbe {

    private Path file;
    private String attributeColumnName;
    private ColumnPath attributeColumn;
    private String attributeValue;
    private String geometryColumnName;
    private ColumnPath geometryColumn;
    private String geometryColumnOverride;
    private String bboxColumn;
    private boolean explicitEnvelope;
    private double cx;
    private double cy;
    private double r;
    private Geometry queryDiamond;
    private Bbox queryEnvelope;
    private boolean spatialAvailable;
    private boolean attributeAvailable;
    private boolean bboxCoveringAvailable;
    private int warmupRuns;
    private int measuredRuns;
    private int concurrency;
    private boolean silent;

    /** Runs the row-path read comparison, configured from {@code parquetry.probe.*} system properties. */
    public static void main(String[] args) throws Exception {
        if (System.getProperty("parquetry.probe.file") == null) {
            IO.println("Set -Dparquetry.probe.file=<parquet file> to run the read-path comparison probe.");
            return;
        }
        ReadComparisonProbe probe = new ReadComparisonProbe();
        probe.configure(Config.fromSystemProperties());
        probe.run();
    }

    /**
     * Honors {@code parquetry.probe.engines} (comma-separated) to run a subset, e.g. just parquetry under a profiler.
     */
    private boolean engineEnabled(String engine) {
        String selected = System.getProperty("parquetry.probe.engines");
        if (selected == null || selected.isBlank()) {
            return true;
        }
        return Arrays.stream(selected.split(",")).map(String::trim).anyMatch(engine::equals);
    }

    /**
     * Honors {@code parquetry.probe.scenarios} (comma-separated names) to run a subset, e.g. just the selective filters
     * under a tight heap where the full scan would not fit.
     */
    private boolean scenarioEnabled(Scenario scenario) {
        String selected = System.getProperty("parquetry.probe.scenarios");
        if (selected == null || selected.isBlank()) {
            return true;
        }
        return Arrays.stream(selected.split(",")).map(String::trim).anyMatch(scenario.name()::equals);
    }

    private void configure(Config config) {
        this.file = config.file();
        this.attributeColumnName = config.attributeColumn();
        this.attributeValue = config.attributeValue();
        this.geometryColumnOverride = config.geometryColumn();
        this.bboxColumn = config.bboxColumn();
        this.explicitEnvelope = config.explicitEnvelope();
        this.cx = config.cx();
        this.cy = config.cy();
        this.r = config.r();
        this.warmupRuns = config.warmupRuns();
        this.measuredRuns = config.measuredRuns();
        this.concurrency = config.concurrency();
        this.silent = config.silent();
    }

    /**
     * Resolves what this file supports: the geometry column (the {@code geometry.column} override, else the GeoParquet
     * primary column, else the first Geometry/Geography leaf), the spatial query envelope (explicit {@code cx/cy/r},
     * else a central region derived from the file's GeoParquet bbox), the attribute filter (from
     * {@code attribute.column}/{@code .value}), and whether GeoParquet 1.1 bbox covering columns exist for the
     * parquet-java pushdown. Scenarios whose inputs do not resolve are skipped.
     */
    private void resolveCapabilities(ParquetReader reader) {
        Optional<GeoParquetMetadata> geo = parseGeoMetadata(reader.keyValueMetadata());
        this.geometryColumnName = resolveGeometryColumn(reader, geo);
        this.geometryColumn = geometryColumnName == null ? null : ColumnPath.of(geometryColumnName.split("\\."));
        resolveEnvelope(geo);
        this.attributeColumn = attributeColumnName == null ? null : ColumnPath.of(attributeColumnName.split("\\."));
        this.attributeAvailable = attributeColumn != null && attributeValue != null;
        this.spatialAvailable = geometryColumn != null && queryEnvelope != null;
        this.bboxCoveringAvailable = hasBboxCovering(reader);
    }

    private String resolveGeometryColumn(ParquetReader reader, Optional<GeoParquetMetadata> geo) {
        if (geometryColumnOverride != null && !geometryColumnOverride.isBlank()) {
            return geometryColumnOverride;
        }
        String primary = geo.map(GeoParquetMetadata::primaryColumn).orElse(null);
        if (primary != null && !primary.isBlank()) {
            return primary;
        }
        for (ColumnPath leaf : reader.schema().leafColumns()) {
            boolean isGeometry = reader.schema()
                    .find(leaf)
                    .filter(SchemaNode.Primitive.class::isInstance)
                    .map(SchemaNode.Primitive.class::cast)
                    .flatMap(SchemaNode.Primitive::logicalType)
                    .filter(ReadComparisonProbe::isGeometryType)
                    .isPresent();
            if (isGeometry) {
                return leaf.dot();
            }
        }
        return null;
    }

    private static boolean isGeometryType(LogicalType logicalType) {
        return logicalType instanceof LogicalType.Geometry || logicalType instanceof LogicalType.Geography;
    }

    private void resolveEnvelope(Optional<GeoParquetMetadata> geo) {
        if (explicitEnvelope) {
            setEnvelope(cx, cy, r);
            return;
        }
        if (geometryColumnName == null) {
            return;
        }
        GeoColumn column = geo.map(GeoParquetMetadata::columns)
                .map(columns -> columns.get(geometryColumnName))
                .orElse(null);
        if (column == null || column.bbox().isEmpty()) {
            return;
        }
        BoundingBox bbox = column.bbox().get();
        double width = bbox.xmax() - bbox.xmin();
        double height = bbox.ymax() - bbox.ymin();
        double half = 0.25 * Math.min(width, height);
        if (half <= 0) {
            return;
        }
        setEnvelope(bbox.xmin() + width / 2, bbox.ymin() + height / 2, half);
    }

    private void setEnvelope(double centerX, double centerY, double halfDiagonal) {
        this.queryDiamond = ProbeGeometry.diamond(centerX, centerY, halfDiagonal);
        this.queryEnvelope = Bbox.of2d(
                centerX - halfDiagonal, centerY - halfDiagonal, centerX + halfDiagonal, centerY + halfDiagonal);
    }

    private boolean hasBboxCovering(ParquetReader reader) {
        for (String corner : new String[] {"xmin", "xmax", "ymin", "ymax"}) {
            if (reader.schema().find(ColumnPath.of(bboxColumn, corner)).isEmpty()) {
                return false;
            }
        }
        return true;
    }

    private static Optional<GeoParquetMetadata> parseGeoMetadata(Map<String, String> keyValueMetadata) {
        String geoJson = keyValueMetadata.get("geo");
        if (geoJson == null || geoJson.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(GeoParquetMetadata.parse(geoJson));
        } catch (RuntimeException _) {
            return Optional.empty();
        }
    }

    private boolean scenarioSupported(Scenario scenario) {
        return switch (scenario) {
            case NO_FILTER -> true;
            case ATTRIBUTE -> attributeAvailable;
            case SPATIAL -> spatialAvailable;
            case ATTRIBUTE_AND_SPATIAL -> attributeAvailable && spatialAvailable;
        };
    }

    private String scenarioSkipReason(Scenario scenario) {
        String noAttribute = "no attribute filter (set parquetry.probe.attribute.column and .attribute.value)";
        String noSpatial = "no geometry column or query envelope";
        return switch (scenario) {
            case NO_FILTER -> "";
            case ATTRIBUTE -> noAttribute;
            case SPATIAL -> noSpatial;
            case ATTRIBUTE_AND_SPATIAL -> attributeAvailable ? noSpatial : noAttribute;
        };
    }

    private String attributeSummary() {
        return attributeAvailable ? attributeColumnName + " = '" + attributeValue + "'" : "none";
    }

    private String spatialSummary() {
        if (!spatialAvailable) {
            return "none";
        }
        String pushdown = bboxCoveringAvailable ? "" : " (no bbox covering for parquet-java pushdown)";
        return "intersects %s on %s%s".formatted(ProbeGeometry.wkt(queryDiamond), geometryColumnName, pushdown);
    }

    private ReadContext context() {
        return new ReadContext(
                file,
                attributeColumnName,
                attributeColumn,
                attributeValue,
                geometryColumnName,
                geometryColumn,
                queryDiamond,
                queryEnvelope,
                bboxColumn,
                bboxCoveringAvailable);
    }

    private List<ReadEngine> engines(ReadContext context) {
        List<ReadEngine> engines = new ArrayList<>();
        if (engineEnabled("parquetry")) {
            engines.add(new ParquetryReadEngine(context));
        }
        if (engineEnabled("parquet-java")) {
            engines.add(new ParquetJavaReadEngine(context));
        }
        if (engineEnabled("duckdb")) {
            engines.add(new DuckDbReadEngine(context));
        }
        return engines;
    }

    private void run() throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            resolveCapabilities(ParquetReader.open(source));
        }
        note("Read-path comparison over %s (%.1f MiB)".formatted(file, Files.size(file) / (1024.0 * 1024.0)));
        note("JVM availableProcessors=%d, maxHeap=%d MiB, concurrency=%d"
                .formatted(
                        Runtime.getRuntime().availableProcessors(),
                        Runtime.getRuntime().maxMemory() / (1024 * 1024),
                        concurrency));
        note("Attribute filter: %s   Spatial filter: %s%n".formatted(attributeSummary(), spatialSummary()));

        List<ReadEngine> engines = engines(context());
        try {
            if (concurrency > 1) {
                runConcurrent(engines);
            } else {
                runSequential(engines);
            }
        } finally {
            engines.forEach(ReadComparisonProbe::closeQuietly);
        }
        long checksum = engines.stream().mapToLong(ReadEngine::checksum).sum();
        note("%n(consumed checksum %d)".formatted(checksum));
    }

    private static void closeQuietly(ReadEngine engine) {
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

    private void runSequential(List<ReadEngine> engines) {
        List<Row> rows = new ArrayList<>();
        for (Scenario scenario : Scenario.values()) {
            if (!scenarioEnabled(scenario)) {
                continue;
            }
            if (!scenarioSupported(scenario)) {
                note("skipping %s: %s".formatted(scenario, scenarioSkipReason(scenario)));
                continue;
            }
            for (ReadEngine engine : engines) {
                rows.add(measure(engine, scenario));
            }
        }
        printTable(rows);
    }

    /**
     * Runs each engine/scenario pass at the configured concurrency, modelling a pod serving concurrent requests:
     * parquetry shares one open dataset across the reads (the long-lived server model), parquet-java opens a reader per
     * read, and DuckDB opens a connection per read (the connection-pool model). Each DuckDB connection runs its own
     * internally-parallel scan. On a small cpuset many connections oversubscribe the cores - the honest untuned-pool
     * picture.
     */
    private void runConcurrent(List<ReadEngine> engines) {
        note("Concurrency: %d reads in flight per engine/scenario pass%n".formatted(concurrency));
        List<ConcurrentRow> rows = new ArrayList<>();
        for (Scenario scenario : Scenario.values()) {
            if (!scenarioEnabled(scenario)) {
                continue;
            }
            if (!scenarioSupported(scenario)) {
                note("skipping %s: %s".formatted(scenario, scenarioSkipReason(scenario)));
                continue;
            }
            for (ReadEngine engine : engines) {
                rows.add(measureConcurrent(engine, scenario));
            }
        }
        printConcurrentTable(rows);
    }

    private Row measure(ReadEngine engine, Scenario scenario) {
        try {
            for (int i = 0; i < warmupRuns; i++) {
                engine.read(scenario);
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
                rows = engine.read(scenario);
                wallNanos.add(System.nanoTime() - start);
                allocBytes = ProbeMeasurement.totalAllocatedBytes() - allocBefore;
                peakHeapBytes = Math.max(peakHeapBytes, ProbeMeasurement.peakHeapBytes());
            }
            return Row.of(
                    engine.name(),
                    scenario,
                    rows,
                    ProbeMeasurement.medianMillis(wallNanos),
                    peakHeapBytes,
                    allocBytes,
                    engine.profile().orElse(null));
        } catch (Exception e) {
            System.err.printf("%n[%s / %s] failed:%n", engine.name(), scenario);
            e.printStackTrace();
            return Row.skipped(engine.name(), scenario, e.getMessage());
        }
    }

    private ConcurrentRow measureConcurrent(ReadEngine engine, Scenario scenario) {
        try {
            ProbeMeasurement.Outcome outcome =
                    ProbeMeasurement.runConcurrent(concurrency, warmupRuns, measuredRuns, () -> engine.read(scenario));
            return new ConcurrentRow(engine.name(), scenario, outcome, null);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return ConcurrentRow.failed(engine.name(), scenario, "interrupted");
        } catch (RuntimeException e) {
            return ConcurrentRow.failed(engine.name(), scenario, e.getMessage());
        }
    }

    // --- output ---

    private void printTable(List<Row> rows) {
        String header = String.format(
                "%-22s %-12s %12s %12s %12s %12s %12s %12s",
                "scenario", "engine", "rows", "wall(ms)", "alloc(MB)", "peakHeap(MB)", "duckScan(ms)", "duckMem(MB)");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (Row row : rows) {
            IO.println(row.format());
        }
        if (silent) {
            return;
        }
        IO.println();
        IO.println(
                "wall(ms) is the consumer cost: every row materialised, every requested column read out (typed JDBC getters per column for DuckDB, falling back to getObject for nested/complex columns; in-place typed value walk for parquetry; Group for parquet-java).");
        IO.println(
                "alloc(MB) and peakHeap(MB) are heap churn and occupancy during the run; for DuckDB they are the JDBC consumer materializing each row (byte[] per binary, Map/List per nested cell), while duckMem is DuckDB's off-heap buffer.");
        IO.println("peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; an upper bound.");
        IO.println(
                "duckScan(ms) is DuckDB's internal engine scan from its profiler. The gap between wall and duckScan is the JDBC row-materialization tax, not decode speed: DuckDB's row API is the bottleneck, the engine scan is fast. For an apples-to-apples decode comparison use the columnar probe.");
        IO.println("duckMem(MB) is DuckDB's self-reported peak buffer memory; its native footprint is off-heap.");
    }

    private void printConcurrentTable(List<ConcurrentRow> rows) {
        String header = String.format(
                "%-22s %-12s %8s %12s %10s %10s %10s %12s %12s %8s",
                "scenario",
                "engine",
                "rows",
                "req/s",
                "p50(ms)",
                "p95(ms)",
                "max(ms)",
                "peakHeap(MB)",
                "alloc(MB)",
                "status");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (ConcurrentRow row : rows) {
            IO.println(row.format());
        }
        if (silent) {
            return;
        }
        IO.println();
        IO.println("All passes ran %d reads at once, released together; metrics aggregate every read."
                .formatted(concurrency));
        IO.println("req/s is measured reads / summed wave wall; p50/p95/max are per-read latencies.");
        IO.println(
                "peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; the decisive signal is whether status stays OK (not OOM) at a pod-sized heap. alloc(MB) is total churn.");
    }

    // --- supporting types ---

    /** One printed line: an engine's result for one scenario. */
    private record Row(
            String engine,
            Scenario scenario,
            long rows,
            double wallMillis,
            long peakHeapBytes,
            long allocBytes,
            DuckDbProfile duck,
            String skipReason) {

        static Row of(
                String engine,
                Scenario scenario,
                long rows,
                double wallMillis,
                long peakHeapBytes,
                long allocBytes,
                DuckDbProfile duck) {
            return new Row(engine, scenario, rows, wallMillis, peakHeapBytes, allocBytes, duck, null);
        }

        static Row skipped(String engine, Scenario scenario, String reason) {
            return new Row(engine, scenario, 0L, 0.0, 0L, 0L, null, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-22s %-12s   skipped: %s", scenario, engine, skipReason);
            }
            String alloc = allocBytes > 0 ? String.format("%.1f", allocBytes / (1024.0 * 1024.0)) : "-";
            String peakHeap = peakHeapBytes > 0 ? String.format("%.1f", peakHeapBytes / (1024.0 * 1024.0)) : "-";
            String duckScan = duck != null ? String.format("%.1f", duck.latencySeconds() * 1000.0) : "-";
            String duckMem = duck != null ? String.format("%.1f", duck.peakBufferBytes() / (1024.0 * 1024.0)) : "-";
            return String.format(
                    "%-22s %-12s %12d %12.1f %12s %12s %12s %12s",
                    scenario, engine, rows, wallMillis, alloc, peakHeap, duckScan, duckMem);
        }
    }

    /** One printed line of the concurrency table: an engine's aggregate result for one scenario. */
    private record ConcurrentRow(
            String engine, Scenario scenario, ProbeMeasurement.Outcome outcome, String skipReason) {

        static ConcurrentRow failed(String engine, Scenario scenario, String reason) {
            return new ConcurrentRow(engine, scenario, null, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-22s %-12s   skipped: %s", scenario, engine, skipReason);
            }
            String peakHeap = String.format("%.1f", outcome.peakHeapBytes() / (1024.0 * 1024.0));
            String alloc = String.format("%.1f", outcome.allocBytes() / (1024.0 * 1024.0));
            return String.format(
                    "%-22s %-12s %8d %12.1f %10.1f %10.1f %10.1f %12s %12s %8s",
                    scenario,
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

        /** OK, or the failure kind annotated with how many reads failed. */
        private String statusLabel() {
            if (outcome.status() == ProbeMeasurement.Status.OK) {
                return "OK";
            }
            return String.format("%s(%d failed)", outcome.status(), outcome.failed());
        }
    }

    /** Resolved probe inputs. */
    private record Config(
            Path file,
            String attributeColumn,
            String attributeValue,
            String geometryColumn,
            String bboxColumn,
            boolean explicitEnvelope,
            double cx,
            double cy,
            double r,
            int warmupRuns,
            int measuredRuns,
            int concurrency,
            boolean silent) {

        static Config fromSystemProperties() {
            String path = System.getProperty("parquetry.probe.file");
            Path file = Path.of(path);
            if (!Files.isRegularFile(file)) {
                throw new IllegalArgumentException("parquetry.probe.file is not a regular file: " + path);
            }
            Double cx = doubleOrNull("parquetry.probe.cx");
            Double cy = doubleOrNull("parquetry.probe.cy");
            Double r = doubleOrNull("parquetry.probe.r");
            boolean explicit = cx != null && cy != null && r != null;
            return new Config(
                    file,
                    System.getProperty("parquetry.probe.attribute.column"),
                    System.getProperty("parquetry.probe.attribute.value"),
                    System.getProperty("parquetry.probe.geometry.column"),
                    System.getProperty("parquetry.probe.bbox.column", "bbox"),
                    explicit,
                    explicit ? cx : 0.0,
                    explicit ? cy : 0.0,
                    explicit ? r : 0.0,
                    intProperty("parquetry.probe.warmup", 1),
                    intProperty("parquetry.probe.measure", 3),
                    intProperty("parquetry.probe.concurrency", 1),
                    Boolean.getBoolean("parquetry.probe.silent"));
        }

        private static Double doubleOrNull(String key) {
            String value = System.getProperty(key);
            return value == null ? null : Double.parseDouble(value);
        }

        private static int intProperty(String key, int fallback) {
            String value = System.getProperty(key);
            return value == null ? fallback : Integer.parseInt(value);
        }
    }
}
