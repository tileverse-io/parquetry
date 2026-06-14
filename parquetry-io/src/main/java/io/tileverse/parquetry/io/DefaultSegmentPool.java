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
package io.tileverse.parquetry.io;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
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
 * <p>Idle backings are held until reused, evicted by an over-cap return, or {@link #close() released}; there is no
 * time-based decay yet, leaving each class able to hold up to its cap of idle native memory between bursts.
 */
final class DefaultSegmentPool implements SegmentPool {

    static final DefaultSegmentPool INSTANCE = fromOptions(SegmentPool.Options.elastic());

    static DefaultSegmentPool fromOptions(SegmentPool.Options options) {
        return new DefaultSegmentPool(
                options.largeBufferThreshold(),
                options.maxPooledBytes(),
                options.maxLargePooledBytes(),
                options.blockSize());
    }

    /** Rounded capacities at or below this reuse from {@link #smallClass}; larger ones from {@link #largeClass}. */
    private final long largeBufferThreshold;

    /** Requested sizes round up to a multiple of this, mapping near-equal requests onto one reusable capacity. */
    private final int blockSize;

    /** Guards both size classes' free lists and retained-byte counters; borrows and returns mutate them. */
    private final ReentrantLock lock = new ReentrantLock();

    /** Reuse pool for buffers at or below {@link #largeBufferThreshold}. */
    private final SizeClass smallClass;

    /** Reuse pool for buffers above {@link #largeBufferThreshold} (coalesced fetch ranges, large decode buffers). */
    private final SizeClass largeClass;

    /** Borrows handed out but not yet returned; reaching zero proves every borrowed segment was closed. */
    private final AtomicLong outstandingBorrows = new AtomicLong();

    /** Borrows ever made, never decremented; a monitoring counter, zero proves the pool was never drawn from. */
    private final AtomicLong totalBorrows = new AtomicLong();

    DefaultSegmentPool(long largeBufferThreshold, long maxPooledBytes, long maxLargePooledBytes, int blockSize) {
        if (largeBufferThreshold <= 0) {
            throw new IllegalArgumentException("largeBufferThreshold must be > 0, got " + largeBufferThreshold);
        }
        if (maxPooledBytes < 0) {
            throw new IllegalArgumentException("maxPooledBytes must be >= 0, got " + maxPooledBytes);
        }
        if (maxLargePooledBytes < 0) {
            throw new IllegalArgumentException("maxLargePooledBytes must be >= 0, got " + maxLargePooledBytes);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be > 0, got " + blockSize);
        }
        this.largeBufferThreshold = largeBufferThreshold;
        this.blockSize = blockSize;
        this.smallClass = new SizeClass(maxPooledBytes);
        this.largeClass = new SizeClass(maxLargePooledBytes);
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
        MemorySegment view = backing.segment().asSlice(0, byteSize);
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
        List<Backing> drained = new ArrayList<>();
        lock.lock();
        try {
            drained.addAll(smallClass.drain());
            drained.addAll(largeClass.drain());
        } finally {
            lock.unlock();
        }
        for (Backing backing : drained) {
            backing.arena().close();
        }
    }

    /** Called by a handle's first {@code close()}; the handles guarantee at most one call per borrow. */
    void borrowReturned() {
        outstandingBorrows.decrementAndGet();
    }

    void giveBack(Backing backing) {
        long size = backing.segment().byteSize();
        SizeClass sizeClass = size > largeBufferThreshold ? largeClass : smallClass;
        boolean retained;
        lock.lock();
        try {
            retained = sizeClass.offer(backing);
        } finally {
            lock.unlock();
        }
        if (!retained) {
            backing.arena().close();
        }
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

    private long roundUpToBlockSize(long byteSize) {
        if (byteSize == 0) {
            return blockSize;
        }
        return ((byteSize + blockSize - 1) / blockSize) * blockSize;
    }

    /** A pooled backing and the explicit arena that owns it; the arena is closed when the backing is evicted. */
    record Backing(Arena arena, MemorySegment segment) {}

    /**
     * One size class's reuse state: a capacity-sorted free list of retained backings under a byte cap. Not thread-safe;
     * the enclosing pool serializes every call through its single {@link #lock}.
     */
    private static final class SizeClass {

        private final List<Backing> free = new ArrayList<>();
        private final long maxRetainedBytes;
        private long retainedBytes;

        SizeClass(long maxRetainedBytes) {
            this.maxRetainedBytes = maxRetainedBytes;
        }

        /** Removes and returns the smallest retained backing that fits {@code capacity}, or {@code null} when none. */
        Backing takeFit(long capacity) {
            for (int i = 0; i < free.size(); i++) {
                Backing candidate = free.get(i);
                if (candidate.segment().byteSize() >= capacity) {
                    retainedBytes -= candidate.segment().byteSize();
                    return free.remove(i);
                }
            }
            return null;
        }

        /** Retains {@code backing} for reuse when it fits under the cap; returns whether it was retained. */
        boolean offer(Backing backing) {
            long size = backing.segment().byteSize();
            if (retainedBytes + size > maxRetainedBytes) {
                return false;
            }
            free.add(insertionPoint(size), backing);
            retainedBytes += size;
            return true;
        }

        List<Backing> drain() {
            List<Backing> drained = new ArrayList<>(free);
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
                if (free.get(mid).segment().byteSize() >= byteSize) {
                    high = mid;
                } else {
                    low = mid + 1;
                }
            }
            return low;
        }
    }
}
