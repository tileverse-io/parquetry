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

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import io.tileverse.parquetry.batch.BinaryVector;
import io.tileverse.parquetry.batch.BooleanVector;
import io.tileverse.parquetry.batch.ColumnVector;
import io.tileverse.parquetry.batch.DoubleVector;
import io.tileverse.parquetry.batch.FixedLenBinaryVector;
import io.tileverse.parquetry.batch.FloatVector;
import io.tileverse.parquetry.batch.Int96Vector;
import io.tileverse.parquetry.batch.IntVector;
import io.tileverse.parquetry.batch.ListVector;
import io.tileverse.parquetry.batch.LongVector;
import io.tileverse.parquetry.batch.MapVector;
import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.batch.StructVector;
import io.tileverse.parquetry.batch.VariantVector;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Columnar read-path comparison of parquetry and parquet-java 1.17.0 over one local Parquet file, full scan only,
 * touching every leaf value without assembling records.
 *
 * <p>This is the columnar sibling of {@link ReadComparisonProbe}. That probe compares the row APIs (parquetry
 * {@code Stream<ParquetRecord>} against parquet-java {@code ParquetReader<Group>}), which folds record-assembly cost
 * into both engines. This probe compares the columnar APIs to isolate raw decode throughput and memory: parquetry's
 * vectorized {@link ParquetRecordBatch} against parquet-java's column-major {@link ColumnReadStoreImpl} leaf reads. No
 * records are built on either side.
 *
 * <p>Both engines read the same local file through {@link LocalInputFile} (no Hadoop filesystem) and touch every leaf
 * value, folding it into a sink to defeat dead-code elimination. parquetry reads each batch with the typed leaf
 * accessors ({@link IntVector#asArray()}, {@link BinaryVector#get(int)}, and the rest), recursing through
 * {@link ListVector}, {@link MapVector}, {@link StructVector}, and {@link VariantVector} down to the primitive and
 * binary leaves; parquet-java iterates each leaf {@link ColumnDescriptor} with a {@link ColumnReader}. Both sum
 * primitive values and binary byte sizes into the sink.
 *
 * <p>Only the full scan is measured. Filters are row-semantic and out of scope for a columnar comparison: the parquetry
 * batch API is pushdown-only and applies no record filter, and parquet-java's column readers read raw leaf pages with
 * no row-level predicate. The probe reads with {@link Predicate#ALWAYS_TRUE}, {@link Projection#ALL}, and
 * {@link ReadOptions#DEFAULTS}.
 *
 * <p>Row counts are reported for both engines (parquetry sums {@link ParquetRecordBatch#rowCount()}, parquet-java sums
 * {@link PageReadStore#getRowCount()} per row group); they must match as a correctness check.
 *
 * <p>Peak heap is read from {@code MemoryPoolMXBean} after each run and reflects heap occupancy including not-yet-
 * collected garbage at the run's {@code -Xmx} rather than the live working set; read it as an upper bound. The decisive
 * memory signal is whether the scan completes at a pod-sized heap (try {@code -Xmx1g}).
 *
 * <h2>Running</h2>
 *
 * <p>This is a JUnit test controlled by the {@code parquetry.probe.file} system property: it is skipped unless that
 * property points at a Parquet file. Run it through the build, giving the test forks enough heap:
 *
 * <pre>{@code
 * ./mvnw -pl :parquetry-benchmarks -am test -Dtest=ColumnarReadComparisonProbe \
 *   -Dparquetry.probe.file=/path/to/buildings.parquet \
 *   -DextraArgLine="-Xmx4g"
 * }</pre>
 *
 * <p>Tunable via system properties:
 *
 * <ul>
 *   <li>{@code parquetry.probe.file}: the file path (required; the probe is skipped when unset).
 *   <li>{@code parquetry.probe.engines}: comma-separated engines to run (default {@code parquetry,parquet-java};
 *       {@code duckdb} has no columnar JDBC equivalent here and is ignored).
 *   <li>{@code parquetry.probe.warmup} / {@code .measure}: warmup and measured run counts (defaults {@code 1} /
 *       {@code 3}).
 *   <li>{@code parquetry.probe.concurrency}: number of full scans to run at once (default {@code 1}). When greater than
 *       {@code 1} each engine instead launches that many scans simultaneously per wave and the probe reports
 *       throughput, latency percentiles, and an OK/OOM/ERROR status, modelling a server pod that admits {@code 2 x
 *       cores} concurrent requests. Pair it with {@code -XX:ActiveProcessorCount=N} to simulate an N-core pod.
 * </ul>
 */
// S2699: characterization probe; it prints a comparison table and only asserts the run completes for every engine.
@SuppressWarnings("java:S2699")
@EnabledIfSystemProperty(named = "parquetry.probe.file", matches = ".+")
final class ColumnarReadComparisonProbe {

    private Path file;
    private int warmupRuns;
    private int measuredRuns;
    private int concurrency;

    /** A sink touched by every consumed value to keep the JIT from eliminating the decode work. */
    private long sink;

    @Test
    void compareColumnarReadPaths() throws Exception {
        configure(Config.fromSystemProperties());
        run();
    }

    private void configure(Config config) {
        this.file = config.file();
        this.warmupRuns = config.warmupRuns();
        this.measuredRuns = config.measuredRuns();
        this.concurrency = config.concurrency();
    }

    private void run() throws Exception {
        IO.println("Columnar read-path comparison over %s (%.1f MiB), full scan%n"
                .formatted(file, Files.size(file) / (1024.0 * 1024.0)));

        if (concurrency > 1) {
            runConcurrent();
        } else {
            runSequential();
        }
        IO.println("%n(consumed checksum %d)".formatted(sink));
    }

    private void runSequential() {
        List<Row> rows = new ArrayList<>();
        if (engineEnabled("parquetry")) {
            rows.add(measure("parquetry", this::readParquetryColumnar));
        }
        if (engineEnabled("parquet-java")) {
            rows.add(measure("parquet-java", this::readParquetJavaColumnar));
        }
        printTable(rows);
    }

    /**
     * Runs each engine at the configured concurrency. parquetry shares one open dataset across the concurrent scans
     * (the long-lived server model); parquet-java opens a file reader per scan (its per-query model).
     */
    private void runConcurrent() {
        IO.println("Concurrency: %d full scans in flight per engine%n".formatted(concurrency));
        List<ConcurrentRow> rows = new ArrayList<>();
        if (engineEnabled("parquetry")) {
            rows.add(measureParquetryConcurrent());
        }
        if (engineEnabled("parquet-java")) {
            rows.add(measureParquetJavaConcurrent());
        }
        printConcurrentTable(rows);
    }

    private ConcurrentRow measureParquetryConcurrent() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetDataset dataset = ParquetDataset.open(source);
            ProbeMeasurement.Outcome outcome = ProbeMeasurement.runConcurrent(
                    concurrency, warmupRuns, measuredRuns, () -> readParquetryColumnar(dataset));
            return new ConcurrentRow("parquetry", outcome, null);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return ConcurrentRow.failed("parquetry", "interrupted");
        } catch (RuntimeException e) {
            return ConcurrentRow.failed("parquetry", e.getMessage());
        }
    }

    private ConcurrentRow measureParquetJavaConcurrent() {
        try {
            ProbeMeasurement.Outcome outcome = ProbeMeasurement.runConcurrent(
                    concurrency, warmupRuns, measuredRuns, this::readParquetJavaColumnar);
            return new ConcurrentRow("parquet-java", outcome, null);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            return ConcurrentRow.failed("parquet-java", "interrupted");
        }
    }

    /**
     * Honors {@code parquetry.probe.engines} (comma-separated) to run a subset. {@code duckdb} has no columnar JDBC
     * equivalent and is always ignored here.
     */
    private boolean engineEnabled(String engine) {
        String selected = System.getProperty("parquetry.probe.engines");
        if (selected == null || selected.isBlank()) {
            return true;
        }
        for (String requested : selected.split(",")) {
            if (requested.trim().equals(engine)) {
                return true;
            }
        }
        return false;
    }

    // --- parquetry arm ---

    /**
     * Reads the file as vectorized batches and touches every leaf value with the typed leaf accessors, recursing into
     * nested vectors down to their primitive and binary leaves. Each batch is closed after it is touched; its values
     * stay valid only until then.
     */
    private long readParquetryColumnar() {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return readParquetryColumnar(ParquetDataset.open(source));
        }
    }

    private long readParquetryColumnar(ParquetDataset dataset) {
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            long rows = 0L;
            for (ParquetRecordBatch batch : (Iterable<ParquetRecordBatch>) batches::iterator) {
                try (batch) {
                    touchBatch(batch);
                    rows += batch.rowCount();
                }
            }
            return rows;
        }
    }

    private void touchBatch(ParquetRecordBatch batch) {
        for (ColumnVector column : batch.columns().values()) {
            touchVector(column);
        }
    }

    /** Folds every value of {@code vector} into the sink, descending nested vectors to their leaves. */
    private void touchVector(ColumnVector vector) {
        switch (vector) {
            case IntVector ints -> touchInts(ints);
            case LongVector longs -> touchLongs(longs);
            case FloatVector floats -> touchFloats(floats);
            case DoubleVector doubles -> touchDoubles(doubles);
            case BooleanVector booleans -> touchBooleans(booleans);
            case BinaryVector binaries -> touchBinaries(binaries);
            case FixedLenBinaryVector fixed -> touchFixedLenBinaries(fixed);
            case Int96Vector int96s -> touchInt96s(int96s);
            case ListVector list -> touchVector(list.child());
            case MapVector map -> {
                touchVector(map.keys());
                touchVector(map.values());
            }
            case StructVector struct -> {
                for (ColumnVector child : struct.children().values()) {
                    touchVector(child);
                }
            }
            case VariantVector variantVector -> {
                touchBinaries(variantVector.metadataColumn());
                touchBinaries(variantVector.valueColumn());
            }
        }
    }

    private void touchInts(IntVector vector) {
        for (int value : vector.asArray()) {
            sink += value;
        }
    }

    private void touchLongs(LongVector vector) {
        for (long value : vector.asArray()) {
            sink += value;
        }
    }

    private void touchFloats(FloatVector vector) {
        for (float value : vector.asArray()) {
            sink += (long) value;
        }
    }

    private void touchDoubles(DoubleVector vector) {
        for (double value : vector.asArray()) {
            sink += (long) value;
        }
    }

    private void touchBooleans(BooleanVector vector) {
        for (boolean value : vector.asArray()) {
            sink += value ? 1L : 0L;
        }
    }

    private void touchBinaries(BinaryVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += segmentByteSize(vector.get(row));
        }
    }

    private void touchFixedLenBinaries(FixedLenBinaryVector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += segmentByteSize(vector.get(row));
        }
    }

    private void touchInt96s(Int96Vector vector) {
        int size = vector.size();
        for (int row = 0; row < size; row++) {
            sink += segmentByteSize(vector.get(row));
        }
    }

    private static long segmentByteSize(MemorySegment segment) {
        return segment == null ? 0L : segment.byteSize();
    }

    // --- parquet-java arm ---

    /**
     * Reads each row group's leaf columns column-major through {@link ColumnReadStoreImpl}, touching every defined
     * value. A leaf is read with a {@link ColumnReader} whose primitive converter is the no-op tree built by
     * {@link DummyRecordConverter}: {@link ColumnReadStoreImpl} needs a record converter only to wire up a primitive
     * converter per leaf, and this probe reads the values straight off the reader, never assembling a record.
     */
    private long readParquetJavaColumnar() throws IOException {
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            String createdBy = reader.getFooter().getFileMetaData().getCreatedBy();
            GroupConverter recordConverter = new DummyRecordConverter(schema).getRootConverter();
            List<ColumnDescriptor> columns = schema.getColumns();
            long rows = 0L;
            PageReadStore pages = reader.readNextRowGroup();
            while (pages != null) {
                ColumnReadStoreImpl store = new ColumnReadStoreImpl(pages, recordConverter, schema, createdBy);
                for (ColumnDescriptor column : columns) {
                    touchColumn(store, column);
                }
                rows += pages.getRowCount();
                pages = reader.readNextRowGroup();
            }
            return rows;
        }
    }

    private void touchColumn(ColumnReadStoreImpl store, ColumnDescriptor column) {
        ColumnReader columnReader = store.getColumnReader(column);
        int maxDefinitionLevel = column.getMaxDefinitionLevel();
        PrimitiveTypeName kind = column.getPrimitiveType().getPrimitiveTypeName();
        for (long i = 0, n = columnReader.getTotalValueCount(); i < n; i++) {
            if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
                touchPrimitive(columnReader, kind);
            }
            columnReader.consume();
        }
    }

    private void touchPrimitive(ColumnReader columnReader, PrimitiveTypeName kind) {
        switch (kind) {
            case BOOLEAN -> sink += columnReader.getBoolean() ? 1L : 0L;
            case INT32 -> sink += columnReader.getInteger();
            case INT64 -> sink += columnReader.getLong();
            case FLOAT -> sink += (long) columnReader.getFloat();
            case DOUBLE -> sink += (long) columnReader.getDouble();
            case INT96, BINARY, FIXED_LEN_BYTE_ARRAY ->
                sink += columnReader.getBinary().length();
        }
    }

    // --- timing + measurement ---

    private Row measure(String engine, ThrowingLongSupplier body) {
        try {
            for (int i = 0; i < warmupRuns; i++) {
                body.getAsLong();
            }
            List<Long> wallNanos = new ArrayList<>(measuredRuns);
            long rows = 0L;
            long peakHeapBytes = 0L;
            long allocBytes = 0L;
            for (int i = 0; i < measuredRuns; i++) {
                ProbeMeasurement.resetPeakHeap();
                long allocBefore = ProbeMeasurement.totalAllocatedBytes();
                long start = System.nanoTime();
                rows = body.getAsLong();
                wallNanos.add(System.nanoTime() - start);
                allocBytes = ProbeMeasurement.totalAllocatedBytes() - allocBefore;
                peakHeapBytes = Math.max(peakHeapBytes, ProbeMeasurement.peakHeapBytes());
            }
            return Row.jvm(engine, rows, ProbeMeasurement.medianMillis(wallNanos), peakHeapBytes, allocBytes);
        } catch (Exception e) {
            System.err.printf("%n[%s] failed:%n", engine);
            e.printStackTrace();
            return Row.skipped(engine, e.getMessage());
        }
    }

    // --- output ---

    private static void printTable(List<Row> rows) {
        String header =
                String.format("%-14s %12s %12s %12s %12s", "engine", "rows", "wall(ms)", "alloc(MB)", "peakHeap(MB)");
        IO.println(header);
        IO.println("-".repeat(header.length()));
        for (Row row : rows) {
            IO.println(row.format());
        }
        IO.println();
        IO.println("Full scan, columnar APIs, no record assembly on either side; every leaf value touched.");
        IO.println("wall(ms) is the raw decode cost: parquetry reads typed vectors per batch, parquet-java");
        IO.println("  reads each leaf column with a ColumnReader. Row counts must match across engines.");
        IO.println("alloc(MB) is heap allocated by all threads during the run (-Xmx-independent churn).");
        IO.println("peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; an upper bound.");
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
        IO.println();
        IO.println("All scans ran %d at once, released together; metrics aggregate every scan".formatted(concurrency));
        IO.println("req/s is measured scans / summed wave wall; p50/p95/max are per-scan latencies.");
        IO.println(
                "peakHeap(MB) is heap occupancy incl. uncollected garbage at the run's -Xmx; the decisive signal is");
        IO.println("  whether status stays OK (not OOM) at a pod-sized heap. alloc(MB) is total churn.");
    }

    // --- supporting types ---

    @FunctionalInterface
    private interface ThrowingLongSupplier {
        long getAsLong() throws Exception;
    }

    /** One printed line: an engine's full-scan result. */
    private record Row(
            String engine, long rows, double wallMillis, long peakHeapBytes, long allocBytes, String skipReason) {

        static Row jvm(String engine, long rows, double wallMillis, long peakHeapBytes, long allocBytes) {
            return new Row(engine, rows, wallMillis, peakHeapBytes, allocBytes, null);
        }

        static Row skipped(String engine, String reason) {
            return new Row(engine, 0L, 0.0, 0L, 0L, reason);
        }

        String format() {
            if (skipReason != null) {
                return String.format("%-14s   skipped: %s", engine, skipReason);
            }
            String alloc = allocBytes > 0 ? String.format("%.1f", allocBytes / (1024.0 * 1024.0)) : "-";
            String peakHeap = peakHeapBytes > 0 ? String.format("%.1f", peakHeapBytes / (1024.0 * 1024.0)) : "-";
            return String.format("%-14s %12d %12.1f %12s %12s", engine, rows, wallMillis, alloc, peakHeap);
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
    }

    /** Resolved probe inputs. */
    private record Config(Path file, int warmupRuns, int measuredRuns, int concurrency) {

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
                    intProperty("parquetry.probe.concurrency", 1));
        }

        private static int intProperty(String key, int fallback) {
            String value = System.getProperty(key);
            return value == null ? fallback : Integer.parseInt(value);
        }
    }
}
