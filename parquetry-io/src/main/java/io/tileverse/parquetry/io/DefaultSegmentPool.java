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
 * <p>The footprint problem it avoids: a pool that retained every returned buffer would grow to the largest size ever
 * requested and stay there for the life of the process; a single rare large request would pin that much native memory
 * forever. The pool prevents that by treating two size classes differently, with {@code largeBufferThreshold} as the
 * dividing line.
 *
 * <ul>
 *   <li><b>Small buffers</b> (rounded capacity at most {@code largeBufferThreshold}) are reused. They live in a free
 *       list sorted by capacity; a borrow takes the smallest free segment that fits, or allocates a fresh one when none
 *       does. Retained bytes never exceed {@code maxPooledBytes}; a return that would breach that cap is not kept, its
 *       native memory freed immediately instead.
 *   <li><b>Large buffers</b> (above the threshold) are never reused. Each is freed the moment its borrower closes. A
 *       rare heavy request thus leaves no lasting footprint, rather than parking a large backing for the JVM lifetime.
 * </ul>
 *
 * <p>Every backing comes from an explicitly-closed {@link Arena}, never a GC-managed {@link Arena#ofAuto() auto} arena.
 * Auto-arena segments are reserved against {@code -XX:MaxDirectMemorySize} (they free on garbage collection, which the
 * JVM must drive under direct-memory pressure); explicitly-closed arenas are not. Sourcing the pool from explicit
 * arenas keeps the streaming working set off that ceiling, which would otherwise default to {@code -Xmx} and starve a
 * concurrent reader.
 *
 * <p>Capacities are rounded up to {@code blockSize}, mapping requests of slightly different sizes onto one shared
 * capacity they can reuse from one another.
 */
final class DefaultSegmentPool implements SegmentPool {

    static final DefaultSegmentPool INSTANCE = fromOptions(SegmentPool.Options.elastic());

    static DefaultSegmentPool fromOptions(SegmentPool.Options options) {
        return new DefaultSegmentPool(options.largeBufferThreshold(), options.maxPooledBytes(), options.blockSize());
    }

    /** Rounded capacities at or below this are reused (pooled); larger ones are freed on return. */
    private final long largeBufferThreshold;

    /** Upper bound on the total capacity held in {@link #free}; a return that would breach it is freed, not kept. */
    private final long maxPooledBytes;

    /** Requested sizes round up to a multiple of this, mapping near-equal requests onto one reusable capacity. */
    private final int blockSize;

    /** Guards the {@link #free} list and {@link #retainedBytes}; borrows and returns mutate both. */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Returned small backings available for reuse, sorted ascending by capacity to let a borrow take the smallest fit.
     */
    private final List<Backing> free = new ArrayList<>();

    /** Borrows handed out but not yet returned; reaching zero proves every borrowed segment was closed. */
    private final AtomicLong outstandingBorrows = new AtomicLong();

    /** Borrows ever made, never decremented; a monitoring counter, zero proves the pool was never drawn from. */
    private final AtomicLong totalBorrows = new AtomicLong();

    /** Sum of the capacities currently in {@link #free}; kept at or below {@link #maxPooledBytes}. */
    private long retainedBytes;

    DefaultSegmentPool(long largeBufferThreshold, long maxPooledBytes, int blockSize) {
        if (largeBufferThreshold <= 0) {
            throw new IllegalArgumentException("largeBufferThreshold must be > 0, got " + largeBufferThreshold);
        }
        if (maxPooledBytes < 0) {
            throw new IllegalArgumentException("maxPooledBytes must be >= 0, got " + maxPooledBytes);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be > 0, got " + blockSize);
        }
        this.largeBufferThreshold = largeBufferThreshold;
        this.maxPooledBytes = maxPooledBytes;
        this.blockSize = blockSize;
    }

    @Override
    public Pooled borrow(long byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize must be >= 0, got " + byteSize);
        }
        long capacity = roundUpToBlockSize(byteSize);
        Pooled borrowed =
                capacity > largeBufferThreshold ? borrowUnpooled(byteSize, capacity) : borrowPooled(byteSize, capacity);
        outstandingBorrows.incrementAndGet();
        totalBorrows.incrementAndGet();
        return borrowed;
    }

    @Override
    public PoolStats stats() {
        lock.lock();
        try {
            return new PoolStats(outstandingBorrows.get(), totalBorrows.get(), free.size(), retainedBytes);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        List<Backing> drained;
        lock.lock();
        try {
            drained = new ArrayList<>(free);
            free.clear();
            retainedBytes = 0;
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

    private Pooled borrowUnpooled(long byteSize, long capacity) {
        Arena arena = Arena.ofShared();
        MemorySegment backing = arena.allocate(capacity);
        MemorySegment view = backing.asSlice(0, byteSize);
        return new UnpooledSegment(this, arena, view);
    }

    private Pooled borrowPooled(long byteSize, long capacity) {
        Backing backing = takeFree(capacity);
        if (backing == null) {
            Arena arena = Arena.ofShared();
            MemorySegment segment = arena.allocate(capacity);
            backing = new Backing(arena, segment);
        }
        MemorySegment view = backing.segment().asSlice(0, byteSize);
        return new PooledSegment(this, backing, view);
    }

    void giveBack(Backing backing) {
        long size = backing.segment().byteSize();
        boolean evict;
        lock.lock();
        try {
            evict = retainedBytes + size > maxPooledBytes;
            if (!evict) {
                int index = insertionPoint(size);
                free.add(index, backing);
                retainedBytes += size;
            }
        } finally {
            lock.unlock();
        }
        if (evict) {
            backing.arena().close();
        }
    }

    private Backing takeFree(long capacity) {
        lock.lock();
        try {
            for (int i = 0; i < free.size(); i++) {
                Backing candidate = free.get(i);
                if (candidate.segment().byteSize() >= capacity) {
                    retainedBytes -= candidate.segment().byteSize();
                    return free.remove(i);
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
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

    /** A pooled backing and the explicit arena that owns it; the arena is closed when the backing is evicted. */
    record Backing(Arena arena, MemorySegment segment) {}

    private long roundUpToBlockSize(long byteSize) {
        if (byteSize == 0) {
            return blockSize;
        }
        return ((byteSize + blockSize - 1) / blockSize) * blockSize;
    }
}
