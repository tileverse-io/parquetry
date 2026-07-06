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
package io.tileverse.parquetry.io;

import java.lang.foreign.MemorySegment;
import java.util.concurrent.TimeUnit;

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

    /**
     * Frees the native memory of every backing currently retained for reuse, releasing the pool's idle footprint
     * deterministically. Call this when a private pool is done; outstanding borrows are unaffected and must be closed
     * by their owners. The process-wide {@link #getDefault() default} lives for the JVM lifetime and is never closed.
     *
     * <p>Intentionally not {@link AutoCloseable}: most pools are short-lived and their retained backings are reclaimed
     * at process exit, and requiring try-with-resources on every borrow site would obscure the borrow/return contract
     * that actually governs memory. Closing is idempotent.
     */
    void close();

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
     * The pool's retention policy. Two size classes reuse returned buffers under separate idle-retention caps, with
     * {@code largeBufferThreshold} as the dividing line: a buffer of rounded capacity at most the threshold reuses up
     * to {@code maxPooledBytes} of idle retention, a larger one up to {@code maxLargePooledBytes}. A return that would
     * breach its class's cap is freed at once rather than retained. Capacities round up to {@code blockSize} to make
     * reuse across nearby request sizes likely. A zero cap disables retention for that class entirely (every return in
     * it frees).
     *
     * <p>A positive {@code idleRetentionNanos} enables background idle decay: a retained backing left untouched for
     * that long is freed by a per-pool background reaper thread, returning its native memory to the OS during quiet
     * periods. Zero disables decay, keeping retained backings until they are reused, evicted by an over-cap return, or
     * the pool closes.
     */
    record Options(
            long largeBufferThreshold,
            long maxPooledBytes,
            long maxLargePooledBytes,
            long idleRetentionNanos,
            int blockSize) {

        public Options {
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
        }

        /**
         * The pod-sized policy: a 4 MB threshold (page-value segments and most coalesced fetch spans stay poolable), a
         * small-buffer idle cap of one eighth of the off-heap allowance clamped to [16 MB, 512 MB], a large-buffer idle
         * cap of one quarter clamped to [64 MB, 1 GB] (coalesced fetch ranges dominate the large class, which gets the
         * wider cap), and a 60-second idle-retention window after which the background reaper returns idle buffers to
         * the OS. Computed once; the limits probe reads container and filesystem facts.
         */
        public static Options elastic() {
            return ElasticHolder.OPTIONS;
        }

        private static final class ElasticHolder {
            private static final Options OPTIONS = elasticOptions();

            private static Options elasticOptions() {
                long maxOffHeapBytes =
                        IoLimits.from(ResourceLimits.getDefault()).maxOffHeapBytes();
                long maxPooledBytes = Math.clamp(maxOffHeapBytes / 8, 16L << 20, 512L << 20);
                long maxLargePooledBytes = Math.clamp(maxOffHeapBytes / 4, 64L << 20, 1024L << 20);
                long idleRetentionNanos = TimeUnit.SECONDS.toNanos(60);
                return new Options(4L << 20, maxPooledBytes, maxLargePooledBytes, idleRetentionNanos, 8192);
            }
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
