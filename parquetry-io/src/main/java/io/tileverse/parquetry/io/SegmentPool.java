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

import io.tileverse.parquetry.io.limits.IoLimits;
import io.tileverse.parquetry.io.limits.ResourceLimits;

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

    /** Process-wide shared default: a JDK-only pool with the {@link Options#elastic() elastic} retention policy. */
    static SegmentPool getDefault() {
        return DefaultSegmentPool.INSTANCE;
    }

    /** A new private pool with the {@link Options#elastic() elastic} options, independent of {@link #getDefault()}. */
    static SegmentPool create() {
        return create(Options.elastic());
    }

    /** A new private pool with the given retention policy, independent of {@link #getDefault()}. */
    static SegmentPool create(Options options) {
        return DefaultSegmentPool.fromOptions(options);
    }

    /**
     * The pool's retention policy. Small buffers (rounded capacity at most {@code largeBufferThreshold}) are pooled for
     * reuse up to {@code maxPooledBytes} of idle retention; larger buffers are freed deterministically when the
     * borrower closes. Capacities round up to {@code blockSize} to make reuse across nearby request sizes likely. A
     * zero {@code maxPooledBytes} disables retention entirely (every return frees).
     */
    record Options(long largeBufferThreshold, long maxPooledBytes, int blockSize) {

        public Options {
            if (largeBufferThreshold <= 0) {
                throw new IllegalArgumentException("largeBufferThreshold must be > 0, got " + largeBufferThreshold);
            }
            if (maxPooledBytes < 0) {
                throw new IllegalArgumentException("maxPooledBytes must be >= 0, got " + maxPooledBytes);
            }
            if (blockSize <= 0) {
                throw new IllegalArgumentException("blockSize must be > 0, got " + blockSize);
            }
        }

        /**
         * The pod-sized policy: a 4 MB pooling threshold (page-value segments and most coalesced fetch spans stay
         * poolable) and an idle-retention cap of one eighth of the off-heap allowance, clamped to [16 MB, 512 MB].
         * Computed once; the limits probe reads container and filesystem facts.
         */
        public static Options elastic() {
            return ElasticHolder.OPTIONS;
        }

        private static final class ElasticHolder {
            private static final Options OPTIONS = new Options(
                    4L << 20,
                    Math.clamp(IoLimits.from(ResourceLimits.getDefault()).maxOffHeapBytes() / 8, 16L << 20, 512L << 20),
                    8192);
        }
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
