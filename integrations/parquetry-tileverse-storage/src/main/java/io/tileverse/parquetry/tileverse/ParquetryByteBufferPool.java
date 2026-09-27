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
package io.tileverse.parquetry.tileverse;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;

import io.tileverse.io.ByteBufferPool;

/**
 * The tileverse {@link ByteBufferPool} of a process that has parquetry on its classpath, discovered through
 * {@link java.util.ServiceLoader}: {@link ByteBufferPool#getDefault()} returns it in preference to tileverse's built-in
 * pool. Every tileverse reader in the process borrows its scratch here, whether it serves parquetry, a PMTiles store or
 * a COG store next to it.
 *
 * <p><strong>Direct borrows</strong> come from the default {@link ParquetRuntime}'s {@link SegmentPool} and reserve
 * their size against its {@link FetchBudget}, the same pool and budget parquetry's own reads draw from. The reservation
 * is soft: an over-budget borrow still returns a usable buffer and holds no reservation to release. Closing the handle
 * returns the segment and releases the reservation.
 *
 * <p><strong>Heap borrows</strong> serve the merge scratch of batched reads and the header, directory and tile buffers
 * of a PMTiles reader. They come from a heap-only pool built with tileverse's own {@link ByteBufferPool#builder()},
 * with block-aligned reuse, idle release and leak detection as in the built-in pool, retaining at most
 * {@link #HEAP_RETAINED_BUFFERS} buffers and {@link #HEAP_RETAINED_BYTES} bytes between borrows. A borrow above the
 * retention quota is still served and is released, not retained, when closed. Heap borrows are never charged to the
 * {@link FetchBudget}, which governs off-heap fetch memory: a heap charge there would push parquetry's own mandatory
 * fetches onto the spill store.
 *
 * <p>Both pools are resolved on first borrow, never in the constructor: {@code ServiceLoader} instantiates this class
 * from inside the built-in pool's own class initialization, and the runtime's resources must not be built there either.
 *
 * <p><strong>Thread-safety:</strong> {@link #borrowDirect} and {@link #borrowHeap} are concurrent-safe, and a handle
 * may be closed from a thread other than the one that borrowed it, as the S3 batch path does when a fetch completes on
 * an SDK thread. Each handle's {@link PooledByteBuffer#close()} is idempotent.
 *
 * <p>{@link #getHeapPoolStatistics()} and {@link #getLeakCount()} report the heap pool.
 * {@link #getDirectPoolStatistics()} reports the {@link SegmentPool}'s retained segments and bytes, with its total
 * borrows counted as returns; the segment pool does not distinguish a fresh allocation from a reuse, and reports both
 * as zero.
 */
public final class ParquetryByteBufferPool implements ByteBufferPool {

    /** Heap buffers retained between borrows: tileverse's own default. */
    static final int HEAP_RETAINED_BUFFERS = ByteBufferPool.DEFAULT_MAX_HEAP_BUFFERS;

    /** Heap bytes retained between borrows: tileverse's own default, one merged fetch at the 32 MiB fetch cap. */
    static final long HEAP_RETAINED_BYTES = ByteBufferPool.DEFAULT_MAX_HEAP_BYTES;

    /** Public no-argument constructor for {@link java.util.ServiceLoader} discovery. */
    public ParquetryByteBufferPool() {
        // resources come from ParquetRuntime.defaultRuntime() and HeapScratch at borrow time, not construction time
    }

    private static SegmentPool segmentPool() {
        return ParquetRuntime.defaultRuntime().segmentPool();
    }

    private static FetchBudget fetchBudget() {
        return ParquetRuntime.defaultRuntime().fetchBudget();
    }

    private static ByteBufferPool heapScratch() {
        return HeapScratch.POOL;
    }

    @Override
    public PooledByteBuffer borrowDirect(int size) {
        requireNonNegative(size);
        boolean reserved = fetchBudget().tryReserve(size);
        SegmentPool.Pooled pooled = borrowOrRelease(size, reserved);
        ByteBuffer view = pooled.segment().asByteBuffer();
        Runnable release = directRelease(pooled, reserved, size);
        return new Handle(view, release);
    }

    @Override
    public PooledByteBuffer borrowHeap(int size) {
        requireNonNegative(size);
        return heapScratch().borrowHeap(size);
    }

    /** Releases the heap buffers retained between borrows. Direct segments are governed by the {@link SegmentPool}. */
    @Override
    public void clear() {
        heapScratch().clear();
    }

    @Override
    public long getLeakCount() {
        return heapScratch().getLeakCount();
    }

    @Override
    public PoolStatistics getHeapPoolStatistics() {
        return heapScratch().getHeapPoolStatistics();
    }

    @Override
    public PoolStatistics getDirectPoolStatistics() {
        SegmentPool.PoolStats stats = segmentPool().stats();
        long returned = stats.totalBorrows() - stats.outstandingBorrows();
        return new PoolStatistics(0, stats.freeSegments(), stats.retainedBytes(), 0, 0, returned, 0);
    }

    private static SegmentPool.Pooled borrowOrRelease(int size, boolean reserved) {
        try {
            return segmentPool().borrow(size);
        } catch (RuntimeException e) {
            if (reserved) {
                fetchBudget().release(size);
            }
            throw e;
        }
    }

    private static Runnable directRelease(SegmentPool.Pooled pooled, boolean reserved, int size) {
        return () -> {
            pooled.close();
            if (reserved) {
                fetchBudget().release(size);
            }
        };
    }

    private static void requireNonNegative(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative, got " + size);
        }
    }

    /**
     * The heap pool, built on the first heap borrow. A nested holder keeps its construction out of this class's
     * constructor: {@code ServiceLoader} runs that constructor while the built-in pool's class is still initializing,
     * and building another built-in pool there would re-enter that initialization.
     */
    private static final class HeapScratch {

        static final ByteBufferPool POOL = ByteBufferPool.builder()
                .maxHeapBuffers(HEAP_RETAINED_BUFFERS)
                .maxHeapBytes(HEAP_RETAINED_BYTES)
                .build();

        private HeapScratch() {}
    }

    private static final class Handle implements PooledByteBuffer {

        private final ByteBuffer view;
        private final Runnable release;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        Handle(ByteBuffer view, Runnable release) {
            this.view = view;
            this.release = release;
        }

        @Override
        public ByteBuffer buffer() {
            return view;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                release.run();
            }
        }
    }
}
