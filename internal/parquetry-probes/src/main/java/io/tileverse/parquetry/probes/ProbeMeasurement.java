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
package io.tileverse.parquetry.probes;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.management.ThreadMXBean;

/**
 * Shared measurement machinery for the read-comparison probes.
 *
 * <p>The static helpers ({@link #resetPeakHeap()}, {@link #peakHeapBytes()}, {@link #totalAllocatedBytes()},
 * {@link #medianMillis(List)}) feed the single-read sequential path each probe runs by default. The
 * {@link #runConcurrent(int, int, int, ReadTask)} runner models a server pod serving a fixed number of concurrent
 * reads: it launches that many reads at once (released together so they overlap), repeats the wave to gather a latency
 * distribution, and reports aggregate throughput, latency percentiles, peak heap, allocation, and an OK/OOM/ERROR
 * status. A GeoServer pod under the control-flow extension admits 2xNcores concurrent requests; this lets the probes
 * answer whether parquetry completes that workload at a pod-sized heap and how throughput scales with cores.
 */
final class ProbeMeasurement {

    private ProbeMeasurement() {}

    /** One full read, returning the number of rows it consumed. */
    @FunctionalInterface
    interface ReadTask {
        long read() throws Exception;
    }

    /** Whether every concurrent read completed, or how the failures broke down. */
    enum Status {
        OK,
        OOM,
        ERROR
    }

    /** Aggregate result of running a read task at a fixed concurrency over one or more waves. */
    record Outcome(
            long rowsPerRead,
            double throughputPerSecond,
            double p50Millis,
            double p95Millis,
            double maxMillis,
            long peakHeapBytes,
            long allocBytes,
            int completed,
            int failed,
            Status status,
            String firstError) {}

    /**
     * Runs {@code task} at the given concurrency. Each wave releases {@code concurrency} reads simultaneously and waits
     * for all to finish; {@code warmupWaves} waves warm the JIT and the shared decode pools and are discarded, then
     * {@code measuredWaves} waves are measured. Throughput is the measured reads divided by the summed wave wall;
     * latency percentiles come from every measured read; peak heap and allocation span the whole measured phase.
     */
    static Outcome runConcurrent(int concurrency, int warmupWaves, int measuredWaves, ReadTask task)
            throws InterruptedException {
        for (int wave = 0; wave < warmupWaves; wave++) {
            runWave(concurrency, task);
        }

        settle();
        resetPeakHeap();
        long allocBefore = totalAllocatedBytes();
        List<Double> latenciesMillis = new ArrayList<>(concurrency * Math.max(1, measuredWaves));
        long measuredWallNanos = 0L;
        long rowsPerRead = 0L;
        int completed = 0;
        int failed = 0;
        boolean oom = false;
        String firstError = null;
        for (int wave = 0; wave < measuredWaves; wave++) {
            WaveResult result = runWave(concurrency, task);
            measuredWallNanos += result.wallNanos();
            latenciesMillis.addAll(result.latenciesMillis());
            completed += result.completed();
            failed += result.failed();
            rowsPerRead = result.rowsPerRead() > 0 ? result.rowsPerRead() : rowsPerRead;
            oom = oom || result.oom();
            if (firstError == null) {
                firstError = result.firstError();
            }
        }
        long allocBytes = totalAllocatedBytes() - allocBefore;
        long peakHeapBytes = peakHeapBytes();
        double throughputPerSecond = measuredWallNanos == 0L ? 0.0 : completed / (measuredWallNanos / 1_000_000_000.0);
        Status status = statusOf(failed, oom);
        return new Outcome(
                rowsPerRead,
                throughputPerSecond,
                percentileMillis(latenciesMillis, 50.0),
                percentileMillis(latenciesMillis, 95.0),
                maxMillis(latenciesMillis),
                peakHeapBytes,
                allocBytes,
                completed,
                failed,
                status,
                firstError);
    }

    private static Status statusOf(int failed, boolean oom) {
        if (failed == 0) {
            return Status.OK;
        }
        return oom ? Status.OOM : Status.ERROR;
    }

    /** Launches {@code concurrency} reads, releases them together, and waits for all to finish. */
    private static WaveResult runWave(int concurrency, ReadTask task) throws InterruptedException {
        WaveState state = new WaveState(concurrency);
        for (int worker = 0; worker < concurrency; worker++) {
            int index = worker;
            Thread thread = new Thread(() -> runOneRead(index, task, state), "probe-read-" + worker);
            thread.start();
        }

        state.ready.await();
        long start = System.nanoTime();
        state.go.countDown();
        state.done.await();
        long wallNanos = System.nanoTime() - start;

        return state.collect(wallNanos);
    }

    /**
     * One worker: signal readiness, wait for the shared release, time a single read, and record its latency, row count,
     * and outcome. A worker that fails must record it rather than vanish, otherwise the wave's status would falsely
     * read OK; an {@link Error} on a worker is the OOM signal the matrix is looking for.
     */
    @SuppressWarnings("java:S1181") // a worker Error (notably OOM) must reach the report, not be swallowed
    private static void runOneRead(int index, ReadTask task, WaveState state) {
        state.ready.countDown();
        try {
            state.go.await();
            long start = System.nanoTime();
            state.rows[index] = task.read();
            state.latenciesMillis[index] = (System.nanoTime() - start) / 1_000_000.0;
            state.succeeded[index] = true;
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        } catch (OutOfMemoryError e) {
            state.oom.set(true);
            String detail = e.getClass().getName() + ": " + e.getMessage();
            if (state.firstError.compareAndSet(null, detail)) {
                System.err.println("[probe] first OOM: " + detail);
            }
        } catch (Throwable e) {
            state.firstError.compareAndSet(null, e.toString());
        } finally {
            state.done.countDown();
        }
    }

    /** Mutable per-wave state shared between the coordinator and its worker threads. */
    private static final class WaveState {
        private final int concurrency;
        private final CountDownLatch ready;
        private final CountDownLatch go = new CountDownLatch(1);
        private final CountDownLatch done;
        private final double[] latenciesMillis;
        private final long[] rows;
        private final boolean[] succeeded;
        private final AtomicBoolean oom = new AtomicBoolean(false);
        private final AtomicReference<String> firstError = new AtomicReference<>();

        WaveState(int concurrency) {
            this.concurrency = concurrency;
            this.ready = new CountDownLatch(concurrency);
            this.done = new CountDownLatch(concurrency);
            this.latenciesMillis = new double[concurrency];
            this.rows = new long[concurrency];
            this.succeeded = new boolean[concurrency];
        }

        WaveResult collect(long wallNanos) {
            List<Double> latencies = new ArrayList<>(concurrency);
            long rowsPerRead = 0L;
            int completed = 0;
            for (int i = 0; i < concurrency; i++) {
                if (succeeded[i]) {
                    completed++;
                    latencies.add(latenciesMillis[i]);
                    rowsPerRead = rows[i];
                }
            }
            int failed = concurrency - completed;
            return new WaveResult(wallNanos, latencies, rowsPerRead, completed, failed, oom.get(), firstError.get());
        }
    }

    private record WaveResult(
            long wallNanos,
            List<Double> latenciesMillis,
            long rowsPerRead,
            int completed,
            int failed,
            boolean oom,
            String firstError) {}

    // --- heap, allocation, and timing helpers (shared with each probe's sequential path) ---

    /**
     * Collects the previous engine's garbage before the next engine's measurement window opens; its peak-heap baseline
     * then excludes heap the prior engine left uncollected. A best-effort {@link System#gc()} settle: it reduces
     * cross-engine contamination but does not fully isolate engines (live off-heap state and GC history persist across
     * engines). For pristine memory numbers, run one engine per process (the {@code parquetry.probe.engines} property).
     */
    static void settle() {
        for (int round = 0; round < 2; round++) {
            System.gc();
            try {
                Thread.sleep(200L);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static final MemoryMXBean MEMORY = ManagementFactory.getMemoryMXBean();
    private static final long HEAP_SAMPLE_INTERVAL_MS = 5L;

    // Peak of TOTAL live heap since the last reset, tracked by a background sampler. Summing each pool's
    // getPeakUsage() instead (Eden + Survivor + Old) double-counts across time - the pools peak at different
    // moments - and reports more than -Xmx; sampling the aggregate getHeapMemoryUsage().getUsed() is the true peak.
    private static final AtomicLong peakHeapUsed = new AtomicLong();
    private static final AtomicBoolean heapSamplerStarted = new AtomicBoolean();

    static void resetPeakHeap() {
        ensureHeapSampler();
        peakHeapUsed.set(currentHeapUsed());
    }

    static long peakHeapBytes() {
        return peakHeapUsed.get();
    }

    private static long currentHeapUsed() {
        return MEMORY.getHeapMemoryUsage().getUsed();
    }

    private static void ensureHeapSampler() {
        if (heapSamplerStarted.compareAndSet(false, true)) {
            Thread sampler = new Thread(ProbeMeasurement::sampleHeapForever, "probe-heap-sampler");
            sampler.setDaemon(true);
            sampler.start();
        }
    }

    private static void sampleHeapForever() {
        while (true) {
            peakHeapUsed.accumulateAndGet(currentHeapUsed(), Math::max);
            try {
                Thread.sleep(HEAP_SAMPLE_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** Bytes allocated on the heap by all threads (including terminated decode workers) since JVM start. */
    static long totalAllocatedBytes() {
        ThreadMXBean bean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
        return bean.getTotalThreadAllocatedBytes();
    }

    static double medianMillis(List<Long> nanos) {
        List<Long> sorted = new ArrayList<>(nanos);
        sorted.sort(Long::compareTo);
        long median = sorted.get(sorted.size() / 2);
        return median / 1_000_000.0;
    }

    private static double percentileMillis(List<Double> latenciesMillis, double percentile) {
        if (latenciesMillis.isEmpty()) {
            return 0.0;
        }
        List<Double> sorted = new ArrayList<>(latenciesMillis);
        sorted.sort(Double::compareTo);
        int rank = (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1;
        int index = Math.max(0, Math.min(rank, sorted.size() - 1));
        return sorted.get(index);
    }

    private static double maxMillis(List<Double> latenciesMillis) {
        double max = 0.0;
        for (double value : latenciesMillis) {
            max = Math.max(max, value);
        }
        return max;
    }
}
