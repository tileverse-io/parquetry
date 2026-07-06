/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
 * A tileverse {@link ByteBufferPool} that vends {@code asByteBuffer()} views of the default {@link ParquetRuntime}'s
 * {@link SegmentPool}, accounted against the same runtime's {@link FetchBudget} - the exact instances parquetry's own
 * reads draw from. A co-resident reader that borrows direct buffers here (for example a PMTiles or VersaTiles store on
 * the same instance) shares one off-heap pool and one budget with those reads instead of running two pools.
 *
 * <p>Discovered through {@link java.util.ServiceLoader}; {@link ByteBufferPool#getDefault()} prefers this provider over
 * the built-in pool when the integration module is on the classpath. The public no-argument constructor exists for that
 * discovery; the runtime's resources are resolved lazily on first borrow, keeping provider discovery free of runtime
 * construction side effects.
 *
 * <p><strong>Direct borrows</strong> reserve their size against the runtime's {@link FetchBudget} (soft accounting) and
 * borrow a native segment from the runtime's {@link SegmentPool}, returning a direct {@code ByteBuffer} view sized to
 * the request. Borrowing never blocks: an over-budget borrow still returns a usable buffer and simply holds no
 * reservation to release. Closing the handle returns the segment and releases any reservation.
 *
 * <p><strong>Heap borrows</strong> return a plain on-heap {@code ByteBuffer.allocate(size)} that supports
 * {@code array()}/{@code hasArray()} (tileverse callers rely on that) and are not routed through the arena.
 *
 * <p><strong>Thread-safety:</strong> {@link #borrowDirect} and {@link #borrowHeap} are concurrent-safe; each returned
 * {@link PooledByteBuffer} handle is owned by a single borrower and is not safe to share or close across threads. Its
 * {@link PooledByteBuffer#close()} is idempotent for that owner.
 */
public final class ParquetryByteBufferPool implements ByteBufferPool {

    /** Public no-argument constructor for {@link java.util.ServiceLoader} discovery. */
    public ParquetryByteBufferPool() {
        // resources come from ParquetRuntime.defaultRuntime() at borrow time, not construction time
    }

    private static SegmentPool segmentPool() {
        return ParquetRuntime.defaultRuntime().segmentPool();
    }

    private static FetchBudget fetchBudget() {
        return ParquetRuntime.defaultRuntime().fetchBudget();
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
        ByteBuffer view = ByteBuffer.allocate(size);
        return new Handle(view, () -> {});
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
