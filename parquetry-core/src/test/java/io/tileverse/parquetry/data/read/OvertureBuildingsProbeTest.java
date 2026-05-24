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
package io.tileverse.parquetry.data.read;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.s3.S3StorageProvider;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ParquetWriter;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.RowGroup;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.ColumnChunk;
import io.tileverse.parquetry.format.ColumnMetaData;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;

/**
 * Probe test: opens a real Overture Maps buildings GeoParquet file (ZSTD, ~11M rows, nested schema with structs / lists
 * / maps) and reads through the first row group via parquetry's {@code ParquetDataset.open(...)} facade.
 *
 * <p>Gated by the {@code PARQUETRY_OVERTURE_PROBE} environment variable - skipped unless explicitly enabled. Intended
 * for manual local verification of end-to-end Parquet reading against a non-trivial, production-grade file, not as a
 * regression test in CI. Requires the absolute file path below to resolve to a readable file.
 */
@EnabledIfEnvironmentVariable(named = "PARQUETRY_OVERTURE_PROBE", matches = "true")
class OvertureBuildingsProbeTest {

    private static final String OVERTURE_FILE =
            "/Users/groldan/tmp/part-00017-bc5e53eb-b1f2-4193-9ba4-84e6c7ca7995-c000.zstd.parquet";

    /**
     * Resolved S3 endpoint for the comparison tests: either the Overture public bucket (default) or a custom
     * S3-compatible endpoint pulled from environment variables. Lets us run the same throughput probes against a
     * high-bandwidth local MinIO setup without forking the tests.
     *
     * <p>Set {@code PARQUETRY_PROBE_S3_ENDPOINT} (e.g. {@code http://homelab:9000}) to switch to a custom endpoint; the
     * remaining {@code PARQUETRY_PROBE_S3_*} vars supply bucket, key, region, credentials, and path-style addressing.
     */
    private record EndpointConfig(
            String region,
            Optional<URI> endpointOverride,
            boolean pathStyle,
            AwsCredentialsProvider creds,
            String bucket,
            String key) {

        String label() {
            return endpointOverride.map(URI::toString).orElse("aws-public") + " :: " + bucket + "/" + key;
        }
    }

    private static EndpointConfig endpointFromEnv() {
        String endpoint = System.getenv("PARQUETRY_PROBE_S3_ENDPOINT");
        if (endpoint == null || endpoint.isBlank()) {
            // Default: Overture buildings public bucket, anonymous us-west-2 access.
            return new EndpointConfig(
                    "us-west-2",
                    Optional.empty(),
                    false,
                    AnonymousCredentialsProvider.create(),
                    "overturemaps-us-west-2",
                    "release/2026-04-15.0/theme=buildings/type=building/"
                            + "part-00000-ea4676e5-311d-537a-872f-f784b95e670b-c000.zstd.parquet");
        }
        String region = envOrDefault("PARQUETRY_PROBE_S3_REGION", "us-east-1");
        String bucket = requireEnv("PARQUETRY_PROBE_S3_BUCKET");
        String key = requireEnv("PARQUETRY_PROBE_S3_KEY");
        String access = envOrDefault("PARQUETRY_PROBE_S3_ACCESS_KEY", "minioadmin");
        String secret = envOrDefault("PARQUETRY_PROBE_S3_SECRET_KEY", "minioadmin");
        boolean pathStyle = Boolean.parseBoolean(envOrDefault("PARQUETRY_PROBE_S3_PATH_STYLE", "true"));
        AwsCredentialsProvider creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(access, secret));
        return new EndpointConfig(region, Optional.of(URI.create(endpoint)), pathStyle, creds, bucket, key);
    }

    private static String envOrDefault(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : v;
    }

    private static int intFromEnv(String name, int fallback) {
        String v = System.getenv(name);
        return v == null || v.isBlank() ? fallback : Integer.parseInt(v.trim());
    }

    private static String requireEnv(String name) {
        String v = System.getenv(name);
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("missing required env var: " + name);
        }
        return v;
    }

    private static S3Client buildSyncClient(EndpointConfig cfg) {
        S3ClientBuilder b = S3Client.builder().region(Region.of(cfg.region())).credentialsProvider(cfg.creds());
        cfg.endpointOverride().ifPresent(b::endpointOverride);
        if (cfg.pathStyle()) {
            b.serviceConfiguration(
                    S3Configuration.builder().pathStyleAccessEnabled(true).build());
        }
        return b.build();
    }

    private static S3AsyncClient buildCrtAsync(EndpointConfig cfg) {
        S3CrtAsyncClientBuilder b = S3AsyncClient.crtBuilder()
                .region(Region.of(cfg.region()))
                .credentialsProvider(cfg.creds())
                .forcePathStyle(cfg.pathStyle());
        cfg.endpointOverride().ifPresent(b::endpointOverride);
        return b.build();
    }

    /**
     * Opens a parquetry {@link Storage} pointing at the same bucket the comparison clients use. For the Overture
     * default this goes through the SPI path; for a custom endpoint it builds a sync {@link S3Client} with the right
     * endpoint / credentials and hands it to {@link S3StorageProvider#open(URI, S3Client)} directly.
     */
    private static Storage openStorage(EndpointConfig cfg) {
        if (cfg.endpointOverride().isEmpty()) {
            StorageConfig spi = new StorageConfig("s3://" + cfg.bucket() + "/")
                    .providerId("s3")
                    .setParameter(S3StorageProvider.S3_REGION.key(), cfg.region())
                    .setParameter(S3StorageProvider.S3_ANONYMOUS.key(), "true");
            return StorageFactory.open(spi);
        }
        S3Client syncForStorage = buildSyncClient(cfg);
        URI bucketUri = URI.create("s3://" + cfg.bucket() + "/");
        return S3StorageProvider.open(bucketUri, syncForStorage);
    }

    @Test
    void testCopyGeometry() throws IOException {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Endpoint: " + cfg.label());

        // No memory cache wrapper - every pass fetches cold, so the per-pass times reflect actual fetch + decode wall
        // time. Useful to compare across runs and to compare against the raw fetch-only ceiling from
        // compareS3SyncParallelismCurve.
        int runs = 5;
        try (Storage storage = openStorage(cfg);
                RangeReader rangeReader = storage.openRangeReader(cfg.key())) {
            ParquetReader reader = ParquetReader.open(rangeReader);
            for (int run = 0; run < runs; run++) {
                IO.println("--- Run %d/%d ---".formatted(run + 1, runs));
                readAllBatches(reader, 100_000);
            }
        }
    }

    private void readAllBatches(ParquetReader reader, int limit) {
        long start = System.currentTimeMillis();
        AtomicLong count = new AtomicLong();

        Predicate filter = Predicate.ALWAYS_TRUE;
        Projection geom = Projection.of(Set.of(ColumnPath.of("geometry")));
        ReadOptions readOpts = ReadOptions.DEFAULTS;

        AtomicLong lastMs = new AtomicLong(start);
        try (Stream<ParquetRecordBatch> stream = reader.readBatches(filter, geom, readOpts)) {
            stream.takeWhile(_ -> count.get() < limit).forEach(batch -> {
                long now = System.currentTimeMillis();
                long time = now - lastMs.get();
                lastMs.set(now);
                int rowCount = batch.rowCount();
                IO.println("read batch of %,d, time (ms): %,d".formatted(rowCount, time));
                count.addAndGet(rowCount);
            });
        }

        long end = System.currentTimeMillis();
        long ellapsed = end - start;
        IO.println("Run Time (s): real %.3f, count: %,d".formatted(ellapsed / 1000d, count.get()));
    }

    /**
     * Long-running variant of {@link #testCopyGeometry} aimed at JProfiler CPU sampling. Runs the same per-pass read 50
     * times after a 3-pass warm-up, so the JVM spends enough time in the steady-state pipeline for a sampling profiler
     * to collect stable hotspots. Prints only summary stats (min/max/mean/p50/p95 of per-pass wall time) to keep the
     * console output small while profiling.
     *
     * <p>Attach JProfiler via {@code prepare_profiling} (or {@code attach} after starting the test) and target the CPU
     * subsystem.
     */
    @Test
    void profileCopyGeometryLoop() throws IOException {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Endpoint: " + cfg.label());

        int warmupRuns = 3;
        int timedRuns = intFromEnv("PARQUETRY_PROFILE_TIMED_RUNS", 50);
        int rowLimit = 100_000;
        long[] perRunMs = new long[timedRuns];

        try (Storage storage = openStorage(cfg);
                RangeReader rangeReader = storage.openRangeReader(cfg.key())) {
            ParquetReader reader = ParquetReader.open(rangeReader);

            IO.println("Warmup: %d runs of %,d rows each".formatted(warmupRuns, rowLimit));
            for (int i = 0; i < warmupRuns; i++) {
                drainBatches(reader, rowLimit);
            }

            IO.println("Profiled: %d runs of %,d rows each".formatted(timedRuns, rowLimit));
            com.sun.management.ThreadMXBean threadMx =
                    (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            long totalCount = 0L;
            long alloc0 = threadMx.getCurrentThreadAllocatedBytes();
            long overall0 = System.nanoTime();
            for (int i = 0; i < timedRuns; i++) {
                long t0 = System.nanoTime();
                totalCount += drainBatches(reader, rowLimit);
                perRunMs[i] = (System.nanoTime() - t0) / 1_000_000L;
            }
            long overallMs = (System.nanoTime() - overall0) / 1_000_000L;
            long allocBytes = threadMx.getCurrentThreadAllocatedBytes() - alloc0;

            printRunStats(perRunMs, totalCount, overallMs);
            IO.println("  allocated %,d bytes total = %.2f MB/run, %.1f bytes/row"
                    .formatted(
                            allocBytes,
                            allocBytes / (double) timedRuns / (1024 * 1024),
                            allocBytes / (double) totalCount));
        }
    }

    private static long drainBatches(ParquetReader reader, int limit) {
        Predicate filter = Predicate.ALWAYS_TRUE;
        Projection geom = Projection.of(Set.of(ColumnPath.of("geometry")));
        ReadOptions readOpts = ReadOptions.DEFAULTS;
        AtomicLong count = new AtomicLong();
        try (Stream<ParquetRecordBatch> stream = reader.readBatches(filter, geom, readOpts)) {
            stream.takeWhile(_ -> count.get() < limit).forEach(batch -> count.addAndGet(batch.rowCount()));
        }
        return count.get();
    }

    private static void printRunStats(long[] perRunMs, long totalCount, long overallMs) {
        long[] sorted = perRunMs.clone();
        java.util.Arrays.sort(sorted);
        long min = sorted[0];
        long max = sorted[sorted.length - 1];
        long p50 = sorted[sorted.length / 2];
        long p95 = sorted[(int) Math.ceil(sorted.length * 0.95) - 1];
        double mean = java.util.Arrays.stream(perRunMs).average().orElse(0d);
        double rowsPerSec = totalCount / (overallMs / 1000d);
        IO.println("Stats over %d timed runs (total %,d rows in %,d ms, %.0f rows/s):"
                .formatted(perRunMs.length, totalCount, overallMs, rowsPerSec));
        IO.println("  min=%d ms, p50=%d ms, mean=%.1f ms, p95=%d ms, max=%d ms".formatted(min, p50, mean, p95, max));
    }

    /**
     * Whole-file read of every column and every row from the configured endpoint, for comparison against external
     * readers (e.g. DuckDB {@code SELECT *}). Reports per-pass wall time, row/byte throughput and decode-thread
     * allocation. Single-threaded today; this is the baseline the read-parallelization work is measured against.
     */
    @Test
    void readFullFileFromEndpoint() throws IOException {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Full-file read: " + cfg.label());
        int passes = intFromEnv("PARQUETRY_PROBE_PASSES", 3);
        double fileMiB = 525_570_468 / (1024d * 1024d);
        com.sun.management.ThreadMXBean threadMx =
                (com.sun.management.ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
        try (Storage storage = openStorage(cfg);
                RangeReader rangeReader = storage.openRangeReader(cfg.key())) {
            ParquetReader reader = ParquetReader.open(rangeReader);
            for (int p = 0; p < passes; p++) {
                AtomicLong count = new AtomicLong();
                long alloc0 = threadMx.getCurrentThreadAllocatedBytes();
                long t0 = System.nanoTime();
                try (Stream<ParquetRecordBatch> stream =
                        reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    stream.forEach(batch -> count.addAndGet(batch.rowCount()));
                }
                long ms = (System.nanoTime() - t0) / 1_000_000L;
                long allocMiB = (threadMx.getCurrentThreadAllocatedBytes() - alloc0) / (1024 * 1024);
                IO.println("pass %d: %,d rows in %,d ms (%.2fM rows/s, %.0f MiB/s, %,d MB alloc)"
                        .formatted(p, count.get(), ms, count.get() / (ms * 1000d), fileMiB / (ms / 1000d), allocMiB));
            }
        }
    }

    /**
     * GeoServer-style concurrency sweep: at each level {@code N} in {@code PARQUETRY_PROBE_CONCURRENCY} (default
     * {@code 1,2,4,8,16,32}), launch {@code N} virtual-thread "users" that each open their own {@code RangeReader} and
     * read the whole file through {@link ReadOptions#DEFAULTS} - so all users share one {@code FetchBudget}, one
     * {@code DecodeExecutor}, and one {@code ByteBufferPool}, exactly as concurrent requests in a server would.
     * Reports, per level, aggregate wall time, per-read p50/max latency, aggregate throughput, and the memory the level
     * touched (peak heap-used, peak direct-buffer bytes, GC time/count delta), so the degradation curve and the
     * bounded-memory behavior are both visible.
     *
     * <p>Peak heap-used is sampled and includes collectable garbage, so it is an upper bound on the working set, not
     * the live set. The point is the shape across concurrency, not an absolute pod-sizing number.
     */
    @Test
    void concurrentReadScaling() throws Exception {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Concurrent read scaling: " + cfg.label());
        long maxHeap = Runtime.getRuntime().maxMemory();
        IO.println("JVM max heap: %.2f GB, availableProcessors: %d, shared decode pool: %d, file: %.0f MiB"
                .formatted(
                        maxHeap / (1024d * 1024d * 1024d),
                        Runtime.getRuntime().availableProcessors(),
                        DecodeExecutor.shared().parallelism(),
                        525_570_468 / (1024d * 1024d)));

        int[] levels = parseLevels(envOrDefault("PARQUETRY_PROBE_CONCURRENCY", "1,2,4,8,16,32"));
        double fileMiB = 525_570_468 / (1024d * 1024d);

        IO.println("Warming up (2 single reads, not measured)...");
        runConcurrentReads(cfg, 1, fileMiB, false);
        runConcurrentReads(cfg, 1, fileMiB, false);

        IO.println("");
        IO.println(
                "users |  wall(s) | read p50(s) | read max(s) |  agg rows/s | agg MiB/s | peak heap(MB) | peak direct(MB) | GC ms/cnt");
        IO.println(
                "------+----------+-------------+-------------+-------------+-----------+---------------+-----------------+----------");
        for (int n : levels) {
            runConcurrentReads(cfg, n, fileMiB, true);
        }
    }

    private void runConcurrentReads(EndpointConfig cfg, int users, double fileMiB, boolean report) throws Exception {
        settle();
        long gcMs0 = gcTimeMs();
        long gcCnt0 = gcCount();
        PeakSampler sampler = new PeakSampler();
        sampler.start();

        long[] perReadMs = new long[users];
        long[] rowsPerRead = new long[users];
        AtomicReference<Throwable> failure = new AtomicReference<>();
        List<Thread> threads = new ArrayList<>(users);
        long t0 = System.nanoTime();
        for (int i = 0; i < users; i++) {
            int idx = i;
            threads.add(Thread.ofVirtual().start(() -> {
                try (Storage storage = openStorage(cfg);
                        RangeReader rr = storage.openRangeReader(cfg.key())) {
                    ParquetReader reader = ParquetReader.open(rr);
                    AtomicLong count = new AtomicLong();
                    long r0 = System.nanoTime();
                    try (Stream<ParquetRecordBatch> stream =
                            reader.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                        stream.forEach(batch -> count.addAndGet(batch.rowCount()));
                    }
                    perReadMs[idx] = (System.nanoTime() - r0) / 1_000_000L;
                    rowsPerRead[idx] = count.get();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }));
        }
        for (Thread t : threads) {
            t.join();
        }
        long wallMs = (System.nanoTime() - t0) / 1_000_000L;
        sampler.stop();

        if (failure.get() != null) {
            throw new IllegalStateException("a concurrent read failed at users=" + users, failure.get());
        }
        if (!report) {
            return;
        }

        long totalRows = java.util.Arrays.stream(rowsPerRead).sum();
        long[] sorted = perReadMs.clone();
        java.util.Arrays.sort(sorted);
        long p50 = sorted[sorted.length / 2];
        long max = sorted[sorted.length - 1];
        double aggRowsPerSec = totalRows / (wallMs / 1000d);
        double aggMiBPerSec = (users * fileMiB) / (wallMs / 1000d);
        long gcMs = gcTimeMs() - gcMs0;
        long gcCnt = gcCount() - gcCnt0;
        IO.println("%5d | %8.2f | %11.2f | %11.2f | %,11.0f | %9.0f | %13d | %15d | %d/%d"
                .formatted(
                        users,
                        wallMs / 1000d,
                        p50 / 1000d,
                        max / 1000d,
                        aggRowsPerSec,
                        aggMiBPerSec,
                        sampler.peakHeapMiB(),
                        sampler.peakDirectMiB(),
                        gcMs,
                        gcCnt));
    }

    /** Samples peak heap-used and peak direct-buffer bytes on a background daemon thread. */
    private static final class PeakSampler {
        private volatile boolean running = true;
        private volatile long peakHeap;
        private volatile long peakDirect;
        private Thread thread;

        void start() {
            MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
            BufferPoolMXBean direct = directBufferPool();
            thread = Thread.ofPlatform().daemon(true).start(() -> {
                while (running) {
                    long heap = memory.getHeapMemoryUsage().getUsed();
                    if (heap > peakHeap) {
                        peakHeap = heap;
                    }
                    if (direct != null) {
                        long used = direct.getMemoryUsed();
                        if (used > peakDirect) {
                            peakDirect = used;
                        }
                    }
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            });
        }

        void stop() {
            running = false;
            try {
                thread.join(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        long peakHeapMiB() {
            return peakHeap / (1024 * 1024);
        }

        long peakDirectMiB() {
            return peakDirect / (1024 * 1024);
        }
    }

    private static BufferPoolMXBean directBufferPool() {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) {
                return pool;
            }
        }
        return null;
    }

    private static long gcTimeMs() {
        long sum = 0L;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = gc.getCollectionTime();
            if (time > 0) {
                sum += time;
            }
        }
        return sum;
    }

    private static long gcCount() {
        long sum = 0L;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = gc.getCollectionCount();
            if (count > 0) {
                sum += count;
            }
        }
        return sum;
    }

    private static void settle() {
        System.gc();
        try {
            Thread.sleep(300);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static int[] parseLevels(String csv) {
        String[] parts = csv.split(",");
        int[] levels = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            levels[i] = Integer.parseInt(parts[i].trim());
        }
        return levels;
    }

    /**
     * Read-&gt;write round trip for a 1:1 comparison against DuckDB's {@code COPY (...) TO ... (FORMAT parquet,
     * COMPRESSION 'zstd')}. The write path is flat-only (nested structs/lists/maps fail fast), so this reads and
     * rewrites the writable flat-scalar columns - everything except the nested {@code names}/{@code bbox} structs and
     * {@code sources} list, which still includes the heavy {@code geometry} WKB column - to a local ZSTD file via
     * {@link ParquetWriter} ({@link WriteOptions#defaults()} = Parquet 2.0, ZSTD-3). Reports wall, rows/s, output size
     * and peak heap/direct/GC, comparable to DuckDB's {@code enable_profiling='json'} output. Run the matching DuckDB
     * COPY over the same projection (printed below) for the like-for-like number.
     */
    @Test
    void readWriteRoundTripVsDuckDbCopy(@TempDir Path tmp) throws Exception {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Read->write round trip (flat writable subset) vs DuckDB COPY: " + cfg.label());

        try (Storage storage = openStorage(cfg);
                RangeReader rangeReader = storage.openRangeReader(cfg.key())) {
            ParquetDataset dataset = ParquetDataset.open(rangeReader);
            List<ColumnPath> flatLeaves = writableFlatLeaves(dataset.schema());
            IO.println("Writable flat columns (%d of %d leaves): %s"
                    .formatted(
                            flatLeaves.size(),
                            dataset.schema().leafColumns().size(),
                            flatLeaves.stream().map(ColumnPath::dot).toList()));
            IO.println("Matching DuckDB projection -> SELECT "
                    + flatLeaves.stream().map(ColumnPath::dot).collect(Collectors.joining(", ")));

            Projection projection = Projection.of(new LinkedHashSet<>(flatLeaves));
            ParquetReader reader = ParquetReader.open(rangeReader);
            int passes = intFromEnv("PARQUETRY_PROBE_PASSES", 3);
            for (int p = 0; p < passes; p++) {
                Path outFile = tmp.resolve("parquetry-copy-" + p + ".parquet");
                settle();
                long gcMs0 = gcTimeMs();
                long gcCnt0 = gcCount();
                PeakSampler sampler = new PeakSampler();
                sampler.start();
                long rows = 0L;
                long t0 = System.nanoTime();
                try (OutputStream fileOut = new BufferedOutputStream(java.nio.file.Files.newOutputStream(outFile))) {
                    ParquetWriter writer = null;
                    try (Stream<ParquetRecordBatch> batches =
                            reader.readBatches(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)) {
                        Iterator<ParquetRecordBatch> it = batches.iterator();
                        while (it.hasNext()) {
                            try (ParquetRecordBatch batch = it.next()) {
                                if (writer == null) {
                                    writer = ParquetWriter.create(
                                            fileOut, batch.projectedSchema(), WriteOptions.defaults());
                                }
                                writer.writeBatch(batch);
                                rows += batch.rowCount();
                            }
                        }
                    } finally {
                        if (writer != null) {
                            writer.close();
                        }
                    }
                }
                long ms = (System.nanoTime() - t0) / 1_000_000L;
                sampler.stop();
                long outBytes = java.nio.file.Files.size(outFile);
                IO.println(
                        "pass %d: %,d rows read+written in %,d ms (%.2fM rows/s) -> %.1f MiB zstd; peak heap %d MB, peak direct %d MB, GC %d ms/%d"
                                .formatted(
                                        p,
                                        rows,
                                        ms,
                                        rows / (ms * 1000d),
                                        outBytes / (1024d * 1024d),
                                        sampler.peakHeapMiB(),
                                        sampler.peakDirectMiB(),
                                        gcTimeMs() - gcMs0,
                                        gcCount() - gcCnt0));
                java.nio.file.Files.deleteIfExists(outFile);
            }
        }
        IO.println(
                "DuckDB COPY reference (full SELECT *, all 26 cols incl. nested): ~1.54 s, ~5.7 GB RSS, 461 MB out.");
    }

    /**
     * The leaf columns the flat-only write path can emit: root-level (single path segment), non-repeated primitives.
     * Excludes nested-group leaves (path length &gt; 1, e.g. {@code bbox.xmin}, {@code names.common}) and repeated
     * columns (e.g. {@code sources}).
     */
    private static List<ColumnPath> writableFlatLeaves(ParquetSchema schema) {
        List<ColumnPath> flat = new ArrayList<>();
        for (ColumnPath leaf : schema.leafColumns()) {
            boolean rootLevel = leaf.parts().size() == 1;
            boolean notRepeated = schema.maxLevels(leaf).maxRepetitionLevel() == 0;
            if (rootLevel && notRepeated) {
                flat.add(leaf);
            }
        }
        return flat;
    }

    /**
     * Wire-time A/B/C for the same byte range against three S3 fetch paths:
     *
     * <ol>
     *   <li><b>sync {@link S3Client#getObjectAsBytes}</b> - mirrors what {@code S3RangeReader} does today. With
     *       {@code aws-crt-client} on the classpath the sync transport is CRT-backed, but the request itself is a
     *       single HTTPS stream.
     *   <li><b>CRT {@link S3AsyncClient}</b> via {@code getObject(req, AsyncResponseTransformer.toBytes())} - the
     *       S3-aware CRT path, which can split a single {@code GetObject} into parallel sub-range fetches under the
     *       hood.
     *   <li><b>{@link S3TransferManager}</b> wrapping the same CRT async client - the high-level entry point that uses
     *       the same splitting policy and writes straight to a file.
     * </ol>
     *
     * <p>The CRT S3 client only splits a single {@code GetObject} when the range exceeds its part size (default 8 MiB).
     * To actually exercise the splitting behavior, the request range is widened to {@link #LARGE_RANGE_BYTES}, four
     * times the default part size, starting at the first geometry chunk's offset. Bytes in that span beyond the
     * geometry chunk belong to other columns - irrelevant for measuring transport throughput.
     */
    private static final int LARGE_RANGE_BYTES = 32 * 1024 * 1024;

    @Test
    void compareS3FetchPathsForOneChunk() throws Exception {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Endpoint: " + cfg.label());

        ChunkRange chunk;
        try (Storage storage = openStorage(cfg);
                RangeReader rr = storage.openRangeReader(cfg.key())) {
            ChunkRange firstChunk = firstGeometryChunkRange(rr);
            long fileSize = rr.size().orElseThrow(() -> new IllegalStateException("S3 size required for this test"));
            long startOffset = firstChunk.offset();
            int length = (int) Math.min((long) LARGE_RANGE_BYTES, fileSize - startOffset);
            chunk = new ChunkRange(startOffset, length);
        }
        int crtPartSize = 8 * 1024 * 1024;
        IO.println("Target: %.1f MiB range (~%dx CRT default 8 MiB partSize)"
                .formatted(chunk.length() / 1024d / 1024d, chunk.length() / crtPartSize));
        IO.println("  offset=%,d, length=%,d bytes".formatted(chunk.offset(), chunk.length()));

        try (S3Client sync = buildSyncClient(cfg);
                S3AsyncClient crtAsync = buildCrtAsync(cfg);
                S3TransferManager tm =
                        S3TransferManager.builder().s3Client(crtAsync).build()) {

            GetObjectRequest req = GetObjectRequest.builder()
                    .bucket(cfg.bucket())
                    .key(cfg.key())
                    .range("bytes=" + chunk.offset() + "-" + (chunk.offset() + chunk.length() - 1))
                    .build();

            // Warm up each client with a tiny fetch so the first timed call doesn't pay for connection setup.
            warmUp(sync, crtAsync, cfg.bucket(), cfg.key());

            int runs = 3;
            for (int i = 1; i <= runs; i++) {
                IO.println("--- pass %d/%d ---".formatted(i, runs));
                timeSync(sync, req, chunk.length());
                timeAsyncCrt(crtAsync, req, chunk.length());
                timeTransferManager(tm, req, chunk.length());
            }
        }
    }

    private record ChunkRange(long offset, int length) {}

    private static ChunkRange firstGeometryChunkRange(RangeReader rr) {
        FileMetaData footer = ParquetFormat.readFooter(rr);
        io.tileverse.parquetry.format.RowGroup rg0 = footer.rowGroups().getFirst();
        for (ColumnChunk c : rg0.columns()) {
            ColumnMetaData meta = c.metaData().orElseThrow();
            if (!meta.pathInSchema().equals(List.of("geometry"))) {
                continue;
            }
            long start = meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
            long size = meta.totalCompressedSize();
            if (size > Integer.MAX_VALUE) {
                throw new IllegalStateException("geometry chunk exceeds int range: " + size);
            }
            return new ChunkRange(start, (int) size);
        }
        throw new IllegalStateException("no geometry column in row group 0");
    }

    private static void warmUp(S3Client sync, S3AsyncClient async, String bucket, String key) {
        GetObjectRequest tiny = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=0-1023")
                .build();
        sync.getObjectAsBytes(tiny);
        async.getObject(tiny, AsyncResponseTransformer.toBytes()).join();
    }

    private static void timeSync(S3Client client, GetObjectRequest req, int expected) {
        long t0 = System.nanoTime();
        ResponseBytes<GetObjectResponse> bytes = client.getObjectAsBytes(req);
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        printFetchRow("sync           ", bytes.asByteArray().length, expected, ms);
    }

    private static void timeAsyncCrt(S3AsyncClient client, GetObjectRequest req, int expected) {
        long t0 = System.nanoTime();
        ResponseBytes<GetObjectResponse> bytes =
                client.getObject(req, AsyncResponseTransformer.toBytes()).join();
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        printFetchRow("crt-async      ", bytes.asByteArray().length, expected, ms);
    }

    private static void timeTransferManager(S3TransferManager tm, GetObjectRequest req, int expected)
            throws IOException {
        Path tmp = Files.createTempFile("tm-overture-", ".bin");
        try {
            long t0 = System.nanoTime();
            tm.downloadFile(DownloadFileRequest.builder()
                            .getObjectRequest(req)
                            .destination(tmp)
                            .build())
                    .completionFuture()
                    .join();
            long ms = (System.nanoTime() - t0) / 1_000_000L;
            long size = Files.size(tmp);
            printFetchRow("tm.downloadFile", (int) size, expected, ms);
        } finally {
            Files.deleteIfExists(tmp);
        }
    }

    private static void printFetchRow(String label, long actual, long expected, long ms) {
        double mbps = (actual / (1024d * 1024d)) / Math.max(ms, 1) * 1000d;
        String mark = actual == expected ? "ok" : "MISMATCH";
        IO.println("  %s: %,d ms  %.1f MiB/s  bytes=%,d (%s)".formatted(label, ms, mbps, actual, mark));
    }

    /**
     * Parallelism-curve sweep for concurrent sync GETs on the same {@link S3Client}. Fetches the same fixed set of
     * geometry chunks at each parallelism level, so per-level throughput is directly comparable. The serial baseline
     * comes from {@code parallelism=1}; the curve climbs until aggregate throughput saturates the link, exposing the
     * useful-concurrency ceiling for this storage backend on this network. Used to pick a default pool size for the
     * parquetry column-chunk prefetch pipeline.
     */
    @Test
    void compareS3SyncParallelismCurve() throws Exception {
        EndpointConfig cfg = endpointFromEnv();
        IO.println("Endpoint: " + cfg.label());

        int totalChunks = 16;
        int[] parallelismLevels = {1, 2, 4, 8, 16};

        List<ChunkRange> chunks;
        try (Storage storage = openStorage(cfg);
                RangeReader rr = storage.openRangeReader(cfg.key())) {
            chunks = firstNGeometryChunkRanges(rr, totalChunks);
        }
        long totalBytes = chunks.stream().mapToLong(ChunkRange::length).sum();
        IO.println("Target: %d geometry chunks (RG 0..%d), total %,d bytes (%.1f MiB)"
                .formatted(totalChunks, totalChunks - 1, totalBytes, totalBytes / 1024d / 1024d));

        try (S3Client sync = buildSyncClient(cfg)) {
            // Warm-up GET so the first timed call doesn't pay for connection setup.
            sync.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(cfg.bucket())
                    .key(cfg.key())
                    .range("bytes=0-1023")
                    .build());

            int passesPerLevel = 2;
            for (int parallelism : parallelismLevels) {
                IO.println("--- parallelism=%d ---".formatted(parallelism));
                try (ExecutorService pool = Executors.newFixedThreadPool(parallelism)) {
                    for (int pass = 1; pass <= passesPerLevel; pass++) {
                        timeParallelSyncFetches(
                                sync, pool, cfg.bucket(), cfg.key(), chunks, totalBytes, parallelism, pass);
                    }
                }
            }
        }
    }

    private static List<ChunkRange> firstNGeometryChunkRanges(RangeReader rr, int n) {
        FileMetaData footer = ParquetFormat.readFooter(rr);
        List<ChunkRange> out = new ArrayList<>(n);
        for (io.tileverse.parquetry.format.RowGroup rg : footer.rowGroups()) {
            if (out.size() >= n) {
                break;
            }
            geometryChunkOf(rg).ifPresent(out::add);
        }
        if (out.size() < n) {
            throw new IllegalStateException(
                    "needed " + n + " geometry chunks but file only has " + out.size() + " row groups");
        }
        return out;
    }

    private static java.util.Optional<ChunkRange> geometryChunkOf(io.tileverse.parquetry.format.RowGroup rg) {
        for (ColumnChunk c : rg.columns()) {
            ColumnMetaData meta = c.metaData().orElseThrow();
            if (!meta.pathInSchema().equals(List.of("geometry"))) {
                continue;
            }
            long start = meta.dictionaryPageOffset().orElse(meta.dataPageOffset());
            long size = meta.totalCompressedSize();
            if (size > Integer.MAX_VALUE) {
                throw new IllegalStateException("geometry chunk exceeds int range: " + size);
            }
            return java.util.Optional.of(new ChunkRange(start, (int) size));
        }
        return java.util.Optional.empty();
    }

    private static void timeParallelSyncFetches(
            S3Client client,
            ExecutorService pool,
            String bucket,
            String key,
            List<ChunkRange> chunks,
            long expectedBytes,
            int parallelism,
            int pass)
            throws Exception {
        List<Callable<Integer>> tasks = new ArrayList<>(chunks.size());
        for (ChunkRange ch : chunks) {
            tasks.add(
                    () -> client.getObjectAsBytes(rangeRequest(bucket, key, ch)).asByteArray().length);
        }
        long t0 = System.nanoTime();
        List<Future<Integer>> futures = pool.invokeAll(tasks);
        long actual = 0L;
        for (Future<Integer> f : futures) {
            actual += f.get();
        }
        long ms = (System.nanoTime() - t0) / 1_000_000L;
        printFetchRow("p=%2d pass %d  ".formatted(parallelism, pass), actual, expectedBytes, ms);
    }

    private static GetObjectRequest rangeRequest(String bucket, String key, ChunkRange ch) {
        return GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .range("bytes=" + ch.offset() + "-" + (ch.offset() + ch.length() - 1))
                .build();
    }

    /**
     * Full-schema regression check via the row API: read every row with {@link Projection#ALL} (no flat-only
     * workaround). Asserts the row count and inspects a sample row to confirm the repeated / map columns
     * ({@code sources}, {@code names.common}) come back as the expected {@link java.util.List} / {@link java.util.Map}
     * shapes.
     */
    @Test
    void readsFullSchema() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        URI uri = file.getParent().toUri();
        try (Storage storage = StorageFactory.open(uri);
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            int leafCount = dataset.schema().leafColumns().size();
            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println();
            System.out.println("=== Overture buildings probe :: full schema ===");
            System.out.println("Leaf columns: " + leafCount + "; rows across "
                    + dataset.rowGroups().size() + " row groups: " + totalRows);

            long[] counted = {0L};
            ParquetRecord[] sampleHolder = {null};
            long start = System.nanoTime();
            try (Stream<ParquetRecord> records =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                records.forEach(r -> {
                    if (sampleHolder[0] == null) {
                        sampleHolder[0] = r;
                    }
                    counted[0]++;
                });
            }
            long ms = (System.nanoTime() - start) / 1_000_000L;
            System.out.println(String.format(
                    "Full-schema iterate: %s rows in %d ms (%.1fk rows/sec)",
                    counted[0], ms, (counted[0] / (ms / 1000.0)) / 1000.0));

            assertThat(counted[0]).as("full-schema read row count").isEqualTo(totalRows);

            ParquetRecord sample = sampleHolder[0];
            if (sample == null) {
                return;
            }
            ColumnPath sources = ColumnPath.of("sources");
            Object sourcesValue = sample.get(sources);
            System.out.println("Sample row[0] sources: "
                    + (sourcesValue == null
                            ? "(null)"
                            : sourcesValue.getClass().getSimpleName() + " of size "
                                    + ((java.util.Collection<?>) sourcesValue).size()));
            assertThat(sourcesValue)
                    .as("sources column must be a non-empty List of structs")
                    .isInstanceOf(java.util.List.class);

            ColumnPath namesCommon = ColumnPath.of("names", "common");
            Object namesCommonValue = sample.get(namesCommon);
            System.out.println("Sample row[0] names.common: "
                    + (namesCommonValue == null
                            ? "(null)"
                            : namesCommonValue.getClass().getSimpleName() + " of size "
                                    + ((namesCommonValue instanceof java.util.Map<?, ?> m) ? m.size() : -1)));
            if (namesCommonValue != null) {
                assertThat(namesCommonValue)
                        .as("names.common column must be a Map when present")
                        .isInstanceOf(java.util.Map.class);
            }
        }
    }

    /**
     * Full-schema regression check via the batch API. Asserts the summed batch row count matches the dataset's total
     * row count and spot-checks the first emitted batch's row 0 for the same {@code sources} List shape that
     * {@link #readsFullSchema} verifies on the row API.
     */
    @Test
    void readsFullSchemaViaBatchApi() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println();
            System.out.println("=== Overture buildings probe :: full schema via batch API ===");
            System.out.println("Rows across " + dataset.rowGroups().size() + " row groups: " + totalRows);

            ColumnPath sourcesPath = ColumnPath.of("sources");
            long[] counted = {0L};
            boolean[] sampleChecked = {false};
            long start = System.nanoTime();
            try (Stream<ParquetRecordBatch> batches =
                    dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> {
                    try (ParquetRecordBatch owned = batch) {
                        int rowCount = owned.rowCount();
                        if (!sampleChecked[0] && rowCount > 0) {
                            ParquetRecord sample = owned.materialize(0);
                            Object sourcesValue = sample.get(sourcesPath);
                            System.out.println("Sample row[0] sources: "
                                    + (sourcesValue == null
                                            ? "(null)"
                                            : sourcesValue.getClass().getSimpleName() + " of size "
                                                    + ((java.util.Collection<?>) sourcesValue).size()));
                            assertThat(sourcesValue)
                                    .as("sources column must be a non-empty List of structs (batch API)")
                                    .isInstanceOf(java.util.List.class);
                            sampleChecked[0] = true;
                        }
                        counted[0] += rowCount;
                    }
                });
            }
            long ms = (System.nanoTime() - start) / 1_000_000L;
            System.out.println(String.format(
                    "Full-schema batch iterate: %s rows in %d ms (%.1fk rows/sec)",
                    counted[0], ms, (counted[0] / (ms / 1000.0)) / 1000.0));

            assertThat(counted[0]).as("full-schema batch read row count").isEqualTo(totalRows);
        }
    }

    @Test
    void opensAndReadsFirstRowGroup() throws Exception {
        Path file = Path.of(OVERTURE_FILE);
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);

            System.out.println();
            System.out.println("=== Overture buildings probe ===");
            System.out.println("File: " + file);
            System.out.println("Size: " + Files.size(file) + " bytes");

            Map<String, String> kv = dataset.keyValueMetadata();
            System.out.println("Key-value metadata keys: " + kv.keySet());
            String geo = kv.get("geo");
            if (geo != null) {
                int previewLen = Math.min(geo.length(), 240);
                System.out.println(
                        "'geo' (preview): " + geo.substring(0, previewLen) + (geo.length() > previewLen ? " ..." : ""));
            }

            assertThat(dataset.rowGroups()).isNotEmpty();
            System.out.println("Row groups: " + dataset.rowGroups().size());
            RowGroup first = dataset.rowGroups().get(0);
            System.out.println("First row group: rowCount=" + first.rowCount()
                    + ", totalByteSize=" + first.totalByteSize()
                    + ", totalCompressedSize=" + first.totalCompressedSize());

            int leafCount = dataset.schema().leafColumns().size();
            System.out.println("Leaf column count: " + leafCount);
            System.out.println("First 6 leaves: "
                    + dataset.schema().leafColumns().stream().limit(6).toList());

            Set<ColumnPath> flatLeaves = Set.of(
                    ColumnPath.of("id"),
                    ColumnPath.of("geometry"),
                    ColumnPath.of("bbox", "xmin"),
                    ColumnPath.of("bbox", "xmax"),
                    ColumnPath.of("bbox", "ymin"),
                    ColumnPath.of("bbox", "ymax"),
                    ColumnPath.of("version"),
                    ColumnPath.of("subtype"),
                    ColumnPath.of("height"),
                    ColumnPath.of("has_parts"));
            Projection projection = Projection.of(flatLeaves);
            System.out.println("Projected " + flatLeaves.size() + " leaves (flat / optional-group only).");

            long totalRows =
                    dataset.rowGroups().stream().mapToLong(RowGroup::rowCount).sum();
            System.out.println("Total rows across all " + dataset.rowGroups().size() + " row groups: " + totalRows);

            ReadOptions opts = ReadOptions.DEFAULTS;
            long fileSize = Files.size(file);

            long[] counted = {0L};
            ParquetRecord[] sampleHolder = {null};
            long startIter = System.nanoTime();
            try (Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, projection, opts)) {
                records.forEach(r -> {
                    if (sampleHolder[0] == null) {
                        sampleHolder[0] = r;
                    }
                    counted[0]++;
                });
            }
            long iterMs = (System.nanoTime() - startIter) / 1_000_000L;
            System.out.println(String.format(
                    "Iterate-only:    %s rows in %d ms (%.1fk rows/sec, %.1f MB/sec compressed)",
                    counted[0],
                    iterMs,
                    (counted[0] / (iterMs / 1000.0)) / 1000.0,
                    (fileSize / 1024.0 / 1024.0) / (iterMs / 1000.0)));

            Path csvOut = Path.of("/tmp/parquetry_proj.csv");
            long[] csvRows = {0L};
            long startCsv = System.nanoTime();
            try (BufferedWriter writer = Files.newBufferedWriter(csvOut, StandardCharsets.UTF_8);
                    Stream<ParquetRecord> records = dataset.read(Predicate.ALWAYS_TRUE, projection, opts)) {
                records.forEach(r -> {
                    appendCsvRow(writer, r);
                    csvRows[0]++;
                });
            }
            long csvMs = (System.nanoTime() - startCsv) / 1_000_000L;
            long csvSize = Files.size(csvOut);
            System.out.println(String.format(
                    "Iterate + CSV:   %s rows in %d ms (%.1fk rows/sec) -> %.1f MB at %s",
                    csvRows[0], csvMs, (csvRows[0] / (csvMs / 1000.0)) / 1000.0, csvSize / 1024.0 / 1024.0, csvOut));
            System.out.println(
                    "duckdb reference (same projection -> CSV with HEADER false): 15,379 ms (~736k rows/sec)");
            ParquetRecord sample = sampleHolder[0];
            if (sample != null) {
                System.out.println("Sample row[0]:");
                System.out.println("  id = " + sample.getString(ColumnPath.of("id")));
                System.out.println("  bbox.xmin = " + sample.getFloat(ColumnPath.of("bbox", "xmin")));
                System.out.println("  bbox.ymin = " + sample.getFloat(ColumnPath.of("bbox", "ymin")));
                System.out.println("  version = " + sample.getInt(ColumnPath.of("version")));
                ColumnPath subtype = ColumnPath.of("subtype");
                System.out.println("  subtype = " + (sample.isNull(subtype) ? "(null)" : sample.getString(subtype)));
                ColumnPath height = ColumnPath.of("height");
                System.out.println("  height = " + (sample.isNull(height) ? "(null)" : sample.getDouble(height)));
                System.out.println("  has_parts = " + sample.getBoolean(ColumnPath.of("has_parts")));
                ColumnPath primary = ColumnPath.of("names", "primary");
                System.out.println(
                        "  names.primary = " + (sample.isNull(primary) ? "(null)" : sample.getString(primary)));
                byte[] wkb = sample.getGeometryBytes(ColumnPath.of("geometry"));
                System.out.println("  geometry (WKB) length = " + (wkb == null ? "null" : wkb.length));
            }
            assertThat(counted[0]).isEqualTo(totalRows);
            assertThat(csvRows[0]).isEqualTo(totalRows);
        }
    }

    private static final ColumnPath C_ID = ColumnPath.of("id");
    private static final ColumnPath C_GEOM = ColumnPath.of("geometry");
    private static final ColumnPath C_BBOX_XMIN = ColumnPath.of("bbox", "xmin");
    private static final ColumnPath C_BBOX_XMAX = ColumnPath.of("bbox", "xmax");
    private static final ColumnPath C_BBOX_YMIN = ColumnPath.of("bbox", "ymin");
    private static final ColumnPath C_BBOX_YMAX = ColumnPath.of("bbox", "ymax");
    private static final ColumnPath C_VERSION = ColumnPath.of("version");
    private static final ColumnPath C_SUBTYPE = ColumnPath.of("subtype");
    private static final ColumnPath C_HEIGHT = ColumnPath.of("height");
    private static final ColumnPath C_HAS_PARTS = ColumnPath.of("has_parts");
    private static final HexFormat HEX = HexFormat.of();

    // CSV serializer matching the projection in opensAndReadsFirstRowGroup. Field handling:
    // - Strings (id, subtype): written raw; Overture IDs are hex and subtype values don't contain commas/quotes.
    // - Geometry: WKB bytes -> lowercase hex (parquetry has no WKB->WKT decoder yet; that lands with the geo
    // integration).
    // - bbox sub-fields: Float.toString.
    // - Optional fields: empty token for null (matching duckdb's default CSV null serialization).
    private static void appendCsvRow(BufferedWriter writer, ParquetRecord r) {
        try {
            writer.write(r.getString(C_ID));
            writer.write(',');
            byte[] wkb = r.getGeometryBytes(C_GEOM);
            if (wkb != null) {
                writer.write(HEX.formatHex(wkb));
            }
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_XMIN)));
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_XMAX)));
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_YMIN)));
            writer.write(',');
            writer.write(Float.toString(r.getFloat(C_BBOX_YMAX)));
            writer.write(',');
            writer.write(Integer.toString(r.getInt(C_VERSION)));
            writer.write(',');
            if (!r.isNull(C_SUBTYPE)) {
                writer.write(r.getString(C_SUBTYPE));
            }
            writer.write(',');
            if (!r.isNull(C_HEIGHT)) {
                writer.write(Double.toString(r.getDouble(C_HEIGHT)));
            }
            writer.write(',');
            writer.write(Boolean.toString(r.getBoolean(C_HAS_PARTS)));
            writer.write('\n');
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
