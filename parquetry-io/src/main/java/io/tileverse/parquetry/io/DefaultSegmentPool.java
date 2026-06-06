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
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded native-segment pool, the JDK-only {@link SegmentPool#getDefault() default}. Retention is split by size to
 * keep a long-lived process from ratcheting native memory to its burst high-water:
 *
 * <ul>
 *   <li><b>Small buffers</b> (rounded capacity at most {@code largeBufferThreshold}) are pooled in a capacity-sorted
 *       free list and reused; a borrow takes the smallest segment large enough, or allocates a fresh one from an auto
 *       arena. Total retained bytes never exceed {@code maxPooledBytes}; a return that would breach the cap is dropped
 *       and becomes garbage-collectable.
 *   <li><b>Large buffers</b> (above the threshold) are never pooled. Each is allocated in its own shared arena and
 *       freed deterministically when the borrower closes, with no dependency on garbage collection (which runs on heap,
 *       not native, pressure). This keeps one heavy burst from pinning large backings for the JVM lifetime.
 * </ul>
 *
 * <p>Capacities are rounded up to a block size to make reuse across slightly different request sizes likely.
 */
final class DefaultSegmentPool implements SegmentPool {

    static final long DEFAULT_LARGE_BUFFER_THRESHOLD = 256L * 1024;
    static final long DEFAULT_MAX_POOLED_BYTES = 4L * 1024 * 1024;
    static final int DEFAULT_BLOCK_SIZE = 8192;

    static final DefaultSegmentPool INSTANCE =
            new DefaultSegmentPool(DEFAULT_LARGE_BUFFER_THRESHOLD, DEFAULT_MAX_POOLED_BYTES, DEFAULT_BLOCK_SIZE);

    private final long largeBufferThreshold;
    private final long maxPooledBytes;
    private final int blockSize;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<MemorySegment> free = new ArrayList<>();
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
        if (capacity > largeBufferThreshold) {
            return borrowUnpooled(byteSize, capacity);
        }
        return borrowPooled(byteSize, capacity);
    }

    private Pooled borrowUnpooled(long byteSize, long capacity) {
        Arena arena = Arena.ofShared();
        MemorySegment backing = arena.allocate(capacity);
        MemorySegment view = backing.asSlice(0, byteSize);
        return new UnpooledSegment(arena, view);
    }

    private Pooled borrowPooled(long byteSize, long capacity) {
        MemorySegment backing = takeFree(capacity);
        if (backing == null) {
            backing = Arena.ofAuto().allocate(capacity);
        }
        MemorySegment view = backing.asSlice(0, byteSize);
        return new PooledSegment(this, backing, view);
    }

    void giveBack(MemorySegment backing) {
        long size = backing.byteSize();
        lock.lock();
        try {
            if (retainedBytes + size > maxPooledBytes) {
                return;
            }
            int index = insertionPoint(size);
            free.add(index, backing);
            retainedBytes += size;
        } finally {
            lock.unlock();
        }
    }

    private MemorySegment takeFree(long capacity) {
        lock.lock();
        try {
            for (int i = 0; i < free.size(); i++) {
                MemorySegment candidate = free.get(i);
                if (candidate.byteSize() >= capacity) {
                    retainedBytes -= candidate.byteSize();
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
            if (free.get(mid).byteSize() >= byteSize) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        return low;
    }

    private long roundUpToBlockSize(long byteSize) {
        if (byteSize == 0) {
            return blockSize;
        }
        return ((byteSize + blockSize - 1) / blockSize) * blockSize;
    }

    // visible for testing
    int freeCount() {
        lock.lock();
        try {
            return free.size();
        } finally {
            lock.unlock();
        }
    }

    // visible for testing
    long pooledBytes() {
        lock.lock();
        try {
            return retainedBytes;
        } finally {
            lock.unlock();
        }
    }
}
