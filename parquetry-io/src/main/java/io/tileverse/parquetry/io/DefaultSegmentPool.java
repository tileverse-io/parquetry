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
package io.tileverse.parquetry.io;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A bounded pool of native {@link MemorySegment}s for column-chunk fetch and per-page decode buffers, the JDK-only
 * {@link SegmentPool#getDefault() default}. It reuses native memory across reads while keeping a long-running server's
 * native footprint flat.
 *
 * <p>Reuse matters because {@link Arena#allocate} always zeroes its memory: a fetch span or a decompression output is
 * allocated, fully overwritten, then discarded, and a fresh allocation per use pays that zeroing every time. Pooling
 * pays the zeroing once and reuses the buffer; the cost shows up directly under heavy scans.
 *
 * <p>Two size classes share the same reuse machinery but keep separate idle-retention caps, letting the common buffers
 * of each class reuse without a rare outlier pinning memory. {@code largeBufferThreshold} is the dividing line.
 *
 * <ul>
 *   <li><b>Small buffers</b> (rounded capacity at most {@code largeBufferThreshold}) reuse up to {@code maxPooledBytes}
 *       of idle retention.
 *   <li><b>Large buffers</b> (above the threshold) reuse up to {@code maxLargePooledBytes}. Coalesced fetch ranges land
 *       here; reusing them across row groups is the bulk of the saved zeroing.
 * </ul>
 *
 * <p>In each class a borrow takes the smallest free backing that fits, or allocates a fresh one; a return is retained
 * for reuse while its class is under its cap, and otherwise has its arena closed at once, freeing the native memory
 * deterministically. A return over the cap evicting the incoming backing keeps a freak oversized request from pinning
 * memory: it is allocated, used, then freed on return rather than parked.
 *
 * <p>Every backing comes from an explicitly-closed {@link Arena}, never a GC-managed {@link Arena#ofAuto() auto} arena.
 * Auto-arena segments are reserved against {@code -XX:MaxDirectMemorySize} (they free on garbage collection, which the
 * JVM must drive under direct-memory pressure); explicitly-closed arenas are not. Sourcing the pool from explicit
 * arenas keeps the streaming working set off that ceiling, which would otherwise default to {@code -Xmx} and starve a
 * concurrent reader. A per-backing arena (rather than one pool-wide arena) is what lets a single evicted backing free
 * on its own.
 *
 * <p>Capacities are rounded up to {@code blockSize}, mapping requests of slightly different sizes onto one shared
 * capacity they can reuse from one another.
 *
 * <p>When the policy sets a positive {@code idleRetentionNanos}, the pool runs a timer on a daemon thread that
 * periodically frees backings left idle past that window, returning their native memory to the OS during quiet periods.
 * The thread is parked between sweeps and wakes briefly every {@link #SWEEP_INTERVAL_SECONDS} seconds. The sweep only
 * reaps when it can take the lock without waiting; a pool actively serving borrows holds the lock briefly and is
 * skipped that round, which is harmless because the next sweep one interval later reaps it. {@link #close()} stops the
 * timer and frees everything retained.
 *
 * <p>The timer task holds only a {@link WeakReference} to its pool, keeping the pool collectible while the daemon
 * thread is alive. A private pool abandoned without {@link #close()} would otherwise be pinned for the life of the JVM
 * by its own timer thread; instead the next sweep observes the cleared reference, shuts the timer down, and lets the
 * thread and its native footprint be reclaimed.
 */
final class DefaultSegmentPool implements SegmentPool {

    static final DefaultSegmentPool INSTANCE = fromOptions(SegmentPool.Options.elastic());

    /**
     * How often the idle sweep runs. This is the reaping granularity, not a policy knob: a backing idle past
     * {@link #idleRetentionNanos} is freed within one interval after the window elapses. Coarse to stay cheap when
     * idle.
     */
    private static final long SWEEP_INTERVAL_SECONDS = 15;

    static DefaultSegmentPool fromOptions(SegmentPool.Options options) {
        DefaultSegmentPool pool = new DefaultSegmentPool(
                options.largeBufferThreshold(),
                options.maxPooledBytes(),
                options.maxLargePooledBytes(),
                options.idleRetentionNanos(),
                options.blockSize());
        pool.startIdleSweep();
        return pool;
    }

    /** Rounded capacities at or below this reuse from {@link #smallClass}; larger ones from {@link #largeClass}. */
    private final long largeBufferThreshold;

    /** Requested sizes round up to a multiple of this, mapping near-equal requests onto one reusable capacity. */
    private final int blockSize;

    /** How long a retained backing may sit idle before the sweep frees it; zero disables idle decay. */
    private final long idleRetentionNanos;

    /** Guards both size classes' free lists and retained-byte counters; borrows and returns mutate them. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Reuse pool for buffers at or below {@link #largeBufferThreshold}. */
    private final SizeClass smallClass;

    /** Reuse pool for buffers above {@link #largeBufferThreshold} (coalesced fetch ranges, large decode buffers). */
    private final SizeClass largeClass;

    /**
     * The idle-sweep timer, created when idle decay is enabled and scheduled after construction; null when disabled.
     */
    private final ScheduledExecutorService idleSweep;

    /** Borrows handed out but not yet returned; reaching zero proves every borrowed segment was closed. */
    private final AtomicLong outstandingBorrows = new AtomicLong();

    /** Borrows ever made, never decremented; a monitoring counter, zero proves the pool was never drawn from. */
    private final AtomicLong totalBorrows = new AtomicLong();

    DefaultSegmentPool(long largeBufferThreshold, long maxPooledBytes, long maxLargePooledBytes, int blockSize) {
        this(largeBufferThreshold, maxPooledBytes, maxLargePooledBytes, 0L, blockSize);
    }

    DefaultSegmentPool(
            long largeBufferThreshold,
            long maxPooledBytes,
            long maxLargePooledBytes,
            long idleRetentionNanos,
            int blockSize) {
        if (largeBufferThreshold <= 0) {
            throw new IllegalArgumentException("largeBufferThreshold must be > 0, got " + largeBufferThreshold);
        }
        if (maxPooledBytes < 0) {
            throw new IllegalArgumentException("maxPooledBytes must be >= 0, got " + maxPooledBytes);
        }
        if (maxLargePooledBytes < 0) {
            throw new IllegalArgumentException("maxLargePooledBytes must be >= 0, got " + maxLargePooledBytes);
        }
        if (idleRetentionNanos < 0) {
            throw new IllegalArgumentException("idleRetentionNanos must be >= 0, got " + idleRetentionNanos);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be > 0, got " + blockSize);
        }
        this.largeBufferThreshold = largeBufferThreshold;
        this.blockSize = blockSize;
        this.idleRetentionNanos = idleRetentionNanos;
        this.smallClass = new SizeClass(maxPooledBytes);
        this.largeClass = new SizeClass(maxLargePooledBytes);
        this.idleSweep =
                idleRetentionNanos > 0 ? Executors.newSingleThreadScheduledExecutor(reaperThreadFactory()) : null;
    }

    @Override
    public Pooled borrow(long byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize must be >= 0, got " + byteSize);
        }
        long capacity = roundUpToBlockSize(byteSize);
        SizeClass sizeClass = capacity > largeBufferThreshold ? largeClass : smallClass;
        Backing backing = takeFit(sizeClass, capacity);
        if (backing == null) {
            backing = allocateBacking(capacity);
        }
        MemorySegment view = backing.view(byteSize);
        outstandingBorrows.incrementAndGet();
        totalBorrows.incrementAndGet();
        return new PooledSegment(this, backing, view);
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            int freeSegments = smallClass.size() + largeClass.size();
            long retainedBytes = smallClass.retainedBytes() + largeClass.retainedBytes();
            return new PoolStats(outstandingBorrows.get(), totalBorrows.get(), freeSegments, retainedBytes);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        stopIdleSweep();
        List<Backing> drained = new ArrayList<>();
        lock.lock();
        try {
            drained.addAll(smallClass.drain());
            drained.addAll(largeClass.drain());
        } finally {
            lock.unlock();
        }
        freeAll(drained);
    }

    /**
     * Stops the idle timer and waits for an in-flight sweep to finish, making {@code close()} a quiescence point: once
     * it returns, no sweep is still closing arenas on the reaper thread. Restores the interrupt and proceeds if the
     * wait is interrupted; the drain below still frees everything retained.
     */
    private void stopIdleSweep() {
        ScheduledExecutorService timer = idleSweep;
        if (timer == null) {
            return;
        }
        timer.shutdownNow();
        try {
            timer.awaitTermination(SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    /** Called by a handle's first {@code close()}; the handles guarantee at most one call per borrow. */
    void borrowReturned() {
        outstandingBorrows.decrementAndGet();
    }

    void giveBack(Backing backing) {
        long size = backing.byteSize();
        SizeClass sizeClass = size > largeBufferThreshold ? largeClass : smallClass;
        long nowNanos = System.nanoTime();
        boolean retained;
        lock.lock();
        try {
            retained = sizeClass.offer(backing, nowNanos);
        } finally {
            lock.unlock();
        }
        if (!retained) {
            backing.close();
        }
    }

    /**
     * Frees every retained backing idle for at least {@code thresholdNanos} as of {@code nowNanos}, taking the lock
     * normally. The package-private entry point the tests drive directly; the live pool reaps through {@link #sweep()}.
     */
    void reapIdle(long nowNanos, long thresholdNanos) {
        if (thresholdNanos <= 0) {
            return;
        }
        List<Backing> stale;
        lock.lock();
        try {
            stale = collectStale(nowNanos, thresholdNanos);
        } finally {
            lock.unlock();
        }
        freeAll(stale);
    }

    private void startIdleSweep() {
        if (idleSweep != null) {
            idleSweep.scheduleWithFixedDelay(
                    new IdleSweep(this, idleSweep), SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
        }
    }

    private static ThreadFactory reaperThreadFactory() {
        return Thread.ofPlatform()
                .daemon()
                .name("parquetry-segment-pool-reaper-", 0)
                .factory();
    }

    /**
     * Reaps idle backings only when the lock can be taken without waiting. Skipping a contended tick is harmless: the
     * next sweep one interval later reaps, and a pool busy enough to hold the lock at the tick is actively churning its
     * buffers rather than letting them sit idle.
     */
    private void sweep() {
        if (!lock.tryLock()) {
            return;
        }
        List<Backing> stale;
        try {
            stale = collectStale(System.nanoTime(), idleRetentionNanos);
        } finally {
            lock.unlock();
        }
        freeAll(stale);
    }

    private List<Backing> collectStale(long nowNanos, long thresholdNanos) {
        List<Backing> stale = new ArrayList<>();
        stale.addAll(smallClass.reapStale(nowNanos, thresholdNanos));
        stale.addAll(largeClass.reapStale(nowNanos, thresholdNanos));
        return stale;
    }

    private Backing takeFit(SizeClass sizeClass, long capacity) {
        lock.lock();
        try {
            return sizeClass.takeFit(capacity);
        } finally {
            lock.unlock();
        }
    }

    private static Backing allocateBacking(long capacity) {
        Arena arena = Arena.ofShared();
        MemorySegment segment = arena.allocate(capacity);
        return new Backing(arena, segment);
    }

    private static void freeAll(List<Backing> backings) {
        for (Backing backing : backings) {
            backing.close();
        }
    }

    private long roundUpToBlockSize(long byteSize) {
        if (byteSize == 0) {
            return blockSize;
        }
        return ((byteSize + blockSize - 1) / blockSize) * blockSize;
    }

    /**
     * The periodic idle sweep, scheduled on the pool's timer. It holds only a {@link WeakReference} to the pool so the
     * pool stays collectible while the timer thread is alive; once the pool has been collected the sweep shuts its own
     * timer down, releasing the thread and executor. Each run resolves the reference to a strong local only for the
     * duration of one sweep.
     */
    private static final class IdleSweep implements Runnable {

        private final WeakReference<DefaultSegmentPool> poolRef;
        private final ScheduledExecutorService timer;

        IdleSweep(DefaultSegmentPool pool, ScheduledExecutorService timer) {
            this.poolRef = new WeakReference<>(pool);
            this.timer = timer;
        }

        @Override
        public void run() {
            DefaultSegmentPool pool = poolRef.get();
            if (pool == null) {
                timer.shutdown();
                return;
            }
            try {
                pool.sweep();
            } catch (RuntimeException _) {
                // A failed sweep must not stop the timer from trying again next interval.
            }
        }
    }

    /** A pooled backing and the explicit arena that owns it. */
    record Backing(Arena arena, MemorySegment segment) {

        long byteSize() {
            return segment.byteSize();
        }

        /** A view of the first {@code byteSize} bytes, handed to a borrower. */
        MemorySegment view(long byteSize) {
            return segment.asSlice(0, byteSize);
        }

        /** Frees the backing's native memory by closing its arena. */
        void close() {
            arena.close();
        }
    }

    /**
     * One size class's reuse state: a capacity-sorted free list of retained backings under a byte cap, each stamped
     * with the time it was returned for idle reaping. Not thread-safe; the enclosing pool serializes every call through
     * its single {@link #lock}.
     */
    private static final class SizeClass {

        private final List<Retained> free = new ArrayList<>();
        private final long maxRetainedBytes;
        private long retainedBytes;

        SizeClass(long maxRetainedBytes) {
            this.maxRetainedBytes = maxRetainedBytes;
        }

        /** Removes and returns the smallest retained backing that fits {@code capacity}, or {@code null} when none. */
        Backing takeFit(long capacity) {
            for (int i = 0; i < free.size(); i++) {
                Retained candidate = free.get(i);
                if (candidate.byteSize() >= capacity) {
                    retainedBytes -= candidate.byteSize();
                    return free.remove(i).backing();
                }
            }
            return null;
        }

        /** Retains {@code backing} (stamped {@code nowNanos}) for reuse when it fits under the cap. */
        boolean offer(Backing backing, long nowNanos) {
            long size = backing.byteSize();
            if (retainedBytes + size > maxRetainedBytes) {
                return false;
            }
            free.add(insertionPoint(size), new Retained(backing, nowNanos));
            retainedBytes += size;
            return true;
        }

        /** Removes and returns backings idle for at least {@code thresholdNanos} as of {@code nowNanos}. */
        List<Backing> reapStale(long nowNanos, long thresholdNanos) {
            List<Backing> stale = new ArrayList<>();
            Iterator<Retained> iterator = free.iterator();
            while (iterator.hasNext()) {
                Retained retained = iterator.next();
                if (nowNanos - retained.retainedAtNanos() >= thresholdNanos) {
                    retainedBytes -= retained.byteSize();
                    stale.add(retained.backing());
                    iterator.remove();
                }
            }
            return stale;
        }

        List<Backing> drain() {
            List<Backing> drained = new ArrayList<>(free.size());
            for (Retained retained : free) {
                drained.add(retained.backing());
            }
            free.clear();
            retainedBytes = 0;
            return drained;
        }

        int size() {
            return free.size();
        }

        long retainedBytes() {
            return retainedBytes;
        }

        private int insertionPoint(long byteSize) {
            int low = 0;
            int high = free.size();
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (free.get(mid).byteSize() >= byteSize) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            return low;
        }

        /** A retained backing paired with the time it entered the free list, for idle reaping. */
        private record Retained(Backing backing, long retainedAtNanos) {

            long byteSize() {
                return backing.byteSize();
            }
        }
    }
}
