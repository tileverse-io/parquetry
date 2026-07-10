/*
 * (c) Copyright 2026 Multiversio LLC. All rights reserved.
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
package io.tileverse.parquetry.benchmarks;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.limits.ResourceLimits;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;

/**
 * Characterizes the resident-memory cost of reading a large-row-group file under a deliberately tiny fetch budget.
 *
 * <p>A mandatory fetch buffers a whole row group's column chunks for progress. The read path holds that buffer in
 * pooled native memory while the {@link io.tileverse.parquetry.runtime.FetchBudget} has room, otherwise it maps an
 * on-disk file. This benchmark pins the budget tiny (via {@link ResourceLimits#fixed}, which derives a fetch budget of
 * ten percent of the stated memory), forcing every mandatory fetch onto a mapped file. A mapped file is reclaimable
 * page cache: the kernel can drop those pages under pressure, unlike the anonymous RAM a native buffer occupies. The
 * signal is therefore peak resident set size (RSS): the {@code concurrency x row-group-span} term lands on reclaimable
 * mmap rather than on anonymous RAM, and peak RSS stays far below what holding every concurrent fetch in RAM would
 * cost.
 *
 * <p>RSS is the only memory number here that reflects the container's real footprint; heap occupancy and JVM allocation
 * counters do not see file-backed mappings. Peak RSS is read from the operating system with {@code ps} around the timed
 * read and reported through the {@link Rss} secondary counters (JMH prints them as extra columns). This is a sizing
 * tool, not a correctness check; treat the numbers as characterization.
 *
 * <p>The {@code concurrency} axis launches that many overlapping reads per operation, modelling a server pod serving
 * several requests at once. Without the valve, peak RSS would grow with concurrency (every concurrent fetch in RAM);
 * with it, the overflow is file-backed and RSS is bounded by what the kernel keeps resident.
 *
 * <p>Build and run the shaded jar from the {@code benchmarks} profile:
 *
 * <pre>{@code
 * ./mvnw -Pbenchmarks -pl :parquetry-benchmarks -am package
 * java --enable-preview --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
 *   -jar internal/parquetry-benchmarks/target/benchmarks.jar FetchSpillBenchmark
 * }</pre>
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 2)
@Measurement(iterations = 3, time = 2)
@Fork(1)
public class FetchSpillBenchmark {

    /** Bytes the writer accumulates per row group before flushing; pinned large to make each fetch big. */
    private static final long ROW_GROUP_BYTES = 128L << 20;

    /** Memory the read runtime is told it has; the derived fetch budget is ten percent of this. */
    private static final long TINY_MEMORY_BYTES = 8L << 20;

    /** Disk handed to the spill store; ample, the overflow always finds room. */
    private static final long AMPLE_DISK_BYTES = 8L << 30;

    private static final int WIDE_COLUMNS = 8;

    @Param({"1", "2", "4"})
    private int concurrency;

    /**
     * Shrinks the fixture for the CI sanity run that only checks the benchmark still runs. The default keeps the large
     * row group the characterization needs; it is not a measurement mode.
     */
    @Param({"false"})
    private boolean smoke;

    private Path workDir;
    private Path fixture;
    private ParquetRuntime tinyFetchRuntime;

    @Setup(Level.Trial)
    public void setUp() throws IOException {
        workDir = Files.createTempDirectory("parquetry-bench-fetch-spill-");
        Path spillDir = Files.createDirectories(workDir.resolve("spill"));
        long rowGroupBytes = smoke ? (4L << 20) : ROW_GROUP_BYTES;
        long targetRows = LargeRowGroupFixture.rowsForUncompressedBytes(rowGroupBytes * 2L, WIDE_COLUMNS);
        fixture = workDir.resolve("large-row-group.parquet");
        LargeRowGroupFixture.write(fixture, targetRows, WIDE_COLUMNS, rowGroupBytes);
        tinyFetchRuntime = tinyFetchRuntime(spillDir);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        SyntheticParquet.deleteRecursively(workDir);
    }

    @Benchmark
    public void spilledFetch(Rss rss, Blackhole blackhole) throws InterruptedException {
        rss.reset();
        runConcurrentReads(blackhole);
        rss.sample();
    }

    private void runConcurrentReads(Blackhole blackhole) throws InterruptedException {
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch go = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(concurrency);
        AtomicReference<Throwable> firstError = new AtomicReference<>();
        for (int worker = 0; worker < concurrency; worker++) {
            startReader(ready, go, done, firstError, blackhole);
        }
        ready.await();
        go.countDown();
        done.await();
        rethrowIfAny(firstError.get());
    }

    private void startReader(
            CountDownLatch ready,
            CountDownLatch go,
            CountDownLatch done,
            AtomicReference<Throwable> firstError,
            Blackhole blackhole) {
        Thread thread = new Thread(() -> readOnce(ready, go, done, firstError, blackhole), "fetch-spill-read");
        thread.start();
    }

    private void readOnce(
            CountDownLatch ready,
            CountDownLatch go,
            CountDownLatch done,
            AtomicReference<Throwable> firstError,
            Blackhole blackhole) {
        ready.countDown();
        try {
            go.await();
            blackhole.consume(consumeFixture());
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            firstError.compareAndSet(null, e);
        } finally {
            done.countDown();
        }
    }

    private long consumeFixture() {
        ByteRangeSource source = ByteRangeSource.ofFile(fixture);
        ParquetFileReader reader = ParquetFileReader.open(source, tinyFetchRuntime, Optional.empty());
        try (Stream<ParquetRecord> records = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return records.count();
        } finally {
            source.close();
        }
    }

    private ParquetRuntime tinyFetchRuntime(Path spillDir) {
        ResourceLimits limits = ResourceLimits.fixed(
                TINY_MEMORY_BYTES, AMPLE_DISK_BYTES, Runtime.getRuntime().availableProcessors(), spillDir);
        return ParquetRuntime.builder().resourceLimits(limits).build();
    }

    private void rethrowIfAny(Throwable error) {
        if (error instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (error != null) {
            throw new IllegalStateException("A concurrent reader failed", error);
        }
    }

    /**
     * Peak resident set size of this process, sampled around the read and reported as a JMH secondary counter. RSS is
     * read from the operating system because file-backed mappings (the spilled fetch buffers) are invisible to the
     * JVM's own heap and allocation counters. The value is the maximum kibibytes seen since the last {@link #reset()}.
     */
    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Rss {

        private long peakKib;

        void reset() {
            peakKib = readResidentKib();
        }

        void sample() {
            peakKib = Math.max(peakKib, readResidentKib());
        }

        public long peakRssKib() {
            return peakKib;
        }

        private static long readResidentKib() {
            List<String> command = List.of(
                    "ps",
                    "-o",
                    "rss=",
                    "-p",
                    Long.toString(ProcessHandle.current().pid()));
            try {
                return runResidentKibCommand(command);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return 0L;
            } catch (IOException _) {
                return 0L;
            }
        }

        private static long runResidentKibCommand(List<String> command) throws IOException, InterruptedException {
            Process process =
                    new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = readLastLine(process);
            // Liveness guard only: the output was already read above and is returned regardless of the wait result.
            process.waitFor(5, TimeUnit.SECONDS);
            if (output == null || output.isBlank()) {
                return 0L;
            }
            return Long.parseLong(output.trim());
        }

        private static String readLastLine(Process process) throws IOException {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                // headerless `ps -o rss=` prints one line; take the last to skip any stderr noise merged in.
                String last = null;
                String line;
                while ((line = reader.readLine()) != null) {
                    last = line;
                }
                return last;
            }
        }
    }
}
