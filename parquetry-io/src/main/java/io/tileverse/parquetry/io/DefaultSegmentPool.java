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
package io.tileverse.parquetry.io;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded native-segment pool, the JDK-only {@link SegmentPool#getDefault() default}. Free segments are held in a
 * capacity-sorted list under a lock; a borrow reuses the smallest segment large enough, or allocates a fresh one from
 * an auto arena (an evicted segment then becomes garbage-collectable). Capacities are rounded up to a block size to
 * make reuse across slightly different request sizes likely, mirroring the direct-buffer pooling the reader relied on.
 */
final class DefaultSegmentPool implements SegmentPool {

    static final int DEFAULT_MAX_POOLED_SEGMENTS = 32;
    static final int DEFAULT_BLOCK_SIZE = 8192;

    static final DefaultSegmentPool INSTANCE = new DefaultSegmentPool(DEFAULT_MAX_POOLED_SEGMENTS, DEFAULT_BLOCK_SIZE);

    private final int maxPooledSegments;
    private final int blockSize;
    private final ReentrantLock lock = new ReentrantLock();
    private final List<MemorySegment> free = new ArrayList<>();

    DefaultSegmentPool(int maxPooledSegments, int blockSize) {
        if (maxPooledSegments <= 0) {
            throw new IllegalArgumentException("maxPooledSegments must be > 0, got " + maxPooledSegments);
        }
        if (blockSize <= 0) {
            throw new IllegalArgumentException("blockSize must be > 0, got " + blockSize);
        }
        this.maxPooledSegments = maxPooledSegments;
        this.blockSize = blockSize;
    }

    @Override
    public Pooled borrow(long byteSize) {
        if (byteSize < 0) {
            throw new IllegalArgumentException("byteSize must be >= 0, got " + byteSize);
        }
        long capacity = roundUpToBlockSize(byteSize);
        MemorySegment backing = takeFree(capacity);
        if (backing == null) {
            backing = Arena.ofAuto().allocate(capacity);
        }
        MemorySegment view = backing.asSlice(0, byteSize);
        return new PooledSegment(this, backing, view);
    }

    void giveBack(MemorySegment backing) {
        lock.lock();
        try {
            if (free.size() >= maxPooledSegments) {
                return;
            }
            int index = insertionPoint(backing.byteSize());
            free.add(index, backing);
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
}
