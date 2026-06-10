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

import java.lang.foreign.MemorySegment;

/**
 * A pool of native {@link MemorySegment}s for column-chunk fetch and per-page decompression buffers. Borrowing reuses
 * native memory to keep the streaming memory budget bounded; closing the handle returns the backing segment.
 *
 * <p>The interface is sealed with {@link DefaultSegmentPool} as its only implementation. The read path derives element
 * counts from segment sizes, which makes the exact-size promise of {@link #borrow} a correctness invariant, not a
 * convention; sealing puts its enforcement in one reviewable place instead of trusting every implementor.
 * {@link #getDefault()} is the process-wide shared pool, letting a single physical pool serve several readers on the
 * same instance. {@link #create()} builds a private pool with its own retention state and {@link #stats() accounting},
 * for tests and for callers that need isolation from the shared default.
 *
 * <p>Pools support concurrent {@link #borrow} calls from multiple threads. A returned {@link Pooled} handle, by
 * contrast, is owned by a single borrower and is not safe to share across threads.
 */
public sealed interface SegmentPool permits DefaultSegmentPool {

    /** Borrows a native segment of exactly {@code byteSize} bytes; return it by closing the handle. */
    Pooled borrow(long byteSize);

    /** A point-in-time snapshot of this pool's accounting, for leak assertions and monitoring. */
    PoolStats stats();

    /** Process-wide shared default: a small bounded pool of native segments, JDK-only. */
    static SegmentPool getDefault() {
        return DefaultSegmentPool.INSTANCE;
    }

    /** A new private pool with the default retention bounds, independent of {@link #getDefault()}. */
    static SegmentPool create() {
        return new DefaultSegmentPool(
                DefaultSegmentPool.DEFAULT_LARGE_BUFFER_THRESHOLD,
                DefaultSegmentPool.DEFAULT_MAX_POOLED_BYTES,
                DefaultSegmentPool.DEFAULT_BLOCK_SIZE);
    }

    /**
     * A point-in-time snapshot of a pool's accounting.
     *
     * @param outstandingBorrows borrows not yet closed; zero proves every borrowed segment was returned
     * @param totalBorrows borrows ever made, never decremented; zero proves the pool was never drawn from
     * @param freeSegments backings currently retained for reuse
     * @param retainedBytes total capacity of the retained backings
     */
    record PoolStats(long outstandingBorrows, long totalBorrows, int freeSegments, long retainedBytes) {}

    /**
     * A borrowed segment owned by a single borrower. The owner closes it exactly once when done; repeated
     * {@link #close()} calls from that owner are a no-op. A {@code Pooled} handle is not designed to be closed
     * concurrently from multiple threads.
     */
    interface Pooled extends AutoCloseable {

        /**
         * The borrowed segment, sized to the requested byte length. Valid only until {@link #close()}; reading or
         * writing it afterwards is undefined, as the underlying native memory may have been handed to another borrow.
         */
        MemorySegment segment();

        /** Returns the segment to the pool. Idempotent for the owning borrower. */
        @Override
        void close();
    }
}
