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
package io.tileverse.parquetry.tileverse;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.internal.read.FetchBudget;
import io.tileverse.parquetry.io.SegmentPool;

import io.tileverse.io.ByteBufferPool;

/**
 * A tileverse {@link ByteBufferPool} that vends {@code asByteBuffer()} views of parquetry's shared Arena
 * {@link SegmentPool}, accounted against parquetry's shared {@link FetchBudget}. A co-resident reader that borrows
 * direct buffers here (for example a PMTiles or VersaTiles store on the same instance) draws from the same off-heap
 * memory parquetry's own reads use, letting both share one budget instead of running two pools.
 *
 * <p>Discovered through {@link java.util.ServiceLoader}; {@link ByteBufferPool#getDefault()} prefers this provider over
 * the built-in pool when the integration module is on the classpath. The public no-argument constructor exists for that
 * discovery.
 *
 * <p><strong>Direct borrows</strong> reserve their size against the shared {@link FetchBudget} (soft accounting) and
 * borrow a native segment from the shared {@link SegmentPool}, returning a direct {@code ByteBuffer} view sized to the
 * request. Borrowing never blocks: an over-budget borrow still returns a usable buffer and simply holds no reservation
 * to release. Closing the handle returns the segment and releases any reservation.
 *
 * <p><strong>Heap borrows</strong> return a plain on-heap {@code ByteBuffer.allocate(size)} that supports
 * {@code array()}/{@code hasArray()} (tileverse callers rely on that) and are not routed through the arena.
 *
 * <p><strong>Thread-safety:</strong> {@link #borrowDirect} and {@link #borrowHeap} are concurrent-safe; each returned
 * {@link PooledByteBuffer} handle is owned by a single borrower and is not safe to share or close across threads. Its
 * {@link PooledByteBuffer#close()} is idempotent for that owner.
 */
public final class ParquetryByteBufferPool implements ByteBufferPool {

    private final SegmentPool segmentPool = SegmentPool.getDefault();
    private final FetchBudget fetchBudget = FetchBudget.defaultBudget();

    /** Public no-argument constructor for {@link java.util.ServiceLoader} discovery. */
    public ParquetryByteBufferPool() {
        // discovered via ServiceLoader; shares the default SegmentPool and FetchBudget with parquetry's own reads
    }

    @Override
    public PooledByteBuffer borrowDirect(int size) {
        requireNonNegative(size);
        boolean reserved = fetchBudget.tryReserve(size);
        SegmentPool.Pooled pooled = borrowOrRelease(size, reserved);
        ByteBuffer view = pooled.segment().asByteBuffer();
        Runnable release = directRelease(pooled, reserved, size);
        return new Handle(view, release);
    }

    @Override
    public PooledByteBuffer borrowHeap(int size) {
        requireNonNegative(size);
        ByteBuffer view = ByteBuffer.allocate(size);
        return new Handle(view, () -> {});
    }

    private SegmentPool.Pooled borrowOrRelease(int size, boolean reserved) {
        try {
            return segmentPool.borrow(size);
        } catch (RuntimeException e) {
            if (reserved) {
                fetchBudget.release(size);
            }
            throw e;
        }
    }

    private Runnable directRelease(SegmentPool.Pooled pooled, boolean reserved, int size) {
        return () -> {
            pooled.close();
            if (reserved) {
                fetchBudget.release(size);
            }
        };
    }

    private static void requireNonNegative(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("size must be non-negative, got " + size);
        }
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
