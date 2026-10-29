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

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import io.tileverse.parquetry.io.SegmentPool;

/**
 * The segment pool under the borrow traffic of a loaded reader. A filtered read of a page-indexed GeoParquet file (500
 * m boxes over 16 million buildings) borrows about four hundred buffers per query: two thirds of them the 8 KiB block
 * of a validity bitmap, a value slice or a batch window's column slice, a quarter the decompressed pages of 64 to 72
 * KiB, the rest 16 KiB to 1 MiB. 32 clients at a thousand queries per second put close to a million borrows and returns
 * per second through the pool. {@code borrowReturn} draws one capacity from that mix per operation; {@code pageLoad}
 * borrows the four buffers of one page load and returns them in reverse order, the shape of a decode worker's page
 * loop. Run with {@code -t 32} for the loaded-server shape and {@code -t 1} for the uncontended cost. {@code retained}
 * is how many idle buffers of the mix sit in the pool's free lists while the benchmark runs, the steady state of a
 * loaded server, where the idle buffers outnumber the borrowed ones.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@Threads(32)
public class SegmentPoolBenchmark {

    /** The capacities of the measured mix, in bytes; {@link #WEIGHTS} holds their shares by index. */
    private static final long[] CAPACITIES = {
        8L * 1024, 72L * 1024, 64L * 1024, 16L * 1024, 32L * 1024, 56L * 1024, 136L * 1024, 1_064L * 1024
    };

    private static final int[] WEIGHTS = {67, 13, 12, 3, 2, 1, 1, 1};

    @Param({"0", "4000"})
    private int retained;

    /** The smoke run keeps the shape and caps the idle buffers at a few dozen. */
    @Param({"false"})
    private boolean smoke;

    private SegmentPool pool;

    @Setup
    public void setUp() {
        // No idle sweep and no total ceiling: the free lists alone are under test.
        long largeBufferThreshold = 4L << 20;
        long maxPooledBytes = 512L << 20;
        long maxLargePooledBytes = 1024L << 20;
        long noCeiling = Long.MAX_VALUE;
        long noIdleDecay = 0L;
        int blockSize = 8192;
        SegmentPool.Options options = new SegmentPool.Options(
                largeBufferThreshold, maxPooledBytes, maxLargePooledBytes, noCeiling, noIdleDecay, blockSize);
        pool = SegmentPool.create(options);
        fillFreeLists(smoke ? Math.min(retained, 64) : retained);
    }

    /** Borrows {@code idleBuffers} of the mix and returns them all, leaving them idle in the free lists. */
    private void fillFreeLists(int idleBuffers) {
        Draw draw = new Draw();
        List<SegmentPool.Pooled> idle = new ArrayList<>(idleBuffers);
        for (int i = 0; i < idleBuffers; i++) {
            idle.add(pool.borrow(draw.nextCapacity()));
        }
        for (SegmentPool.Pooled pooled : idle) {
            pooled.close();
        }
    }

    @TearDown
    public void tearDown() {
        pool.close();
    }

    @State(Scope.Thread)
    public static class Draw {

        private final SplittableRandom random = new SplittableRandom();
        private final long[] table = weightedTable();

        long nextCapacity() {
            return table[random.nextInt(table.length)];
        }

        private static long[] weightedTable() {
            int total = 0;
            for (int weight : WEIGHTS) {
                total += weight;
            }
            long[] table = new long[total];
            int next = 0;
            for (int i = 0; i < WEIGHTS.length; i++) {
                for (int j = 0; j < WEIGHTS[i]; j++) {
                    table[next++] = CAPACITIES[i];
                }
            }
            return table;
        }
    }

    @Benchmark
    public long borrowReturn(Draw draw) {
        long size = draw.nextCapacity();
        try (SegmentPool.Pooled pooled = pool.borrow(size)) {
            return pooled.segment().byteSize();
        }
    }

    @Benchmark
    public long pageLoad() {
        long total = 0;
        try (SegmentPool.Pooled page = pool.borrow(72L * 1024);
                SegmentPool.Pooled validity = pool.borrow(8L * 1024);
                SegmentPool.Pooled values = pool.borrow(16L * 1024);
                SegmentPool.Pooled slice = pool.borrow(8L * 1024)) {
            total += page.segment().byteSize() + validity.segment().byteSize();
            total += values.segment().byteSize() + slice.segment().byteSize();
        }
        return total;
    }
}
