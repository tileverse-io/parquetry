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
package io.tileverse.parquetry.runtime;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

import com.sun.management.HotSpotDiagnosticMXBean;

/**
 * Bounds the total in-flight bytes of column-chunk fetches. Shared process-wide: however many concurrent reads run, the
 * fetch memory stays under one cap.
 *
 * <p>Speculative work (prefetch, decode-ahead) reserves through {@link #tryReserve(long)}. That call never waits, and
 * it yields while mandatory fetches are queued. A mandatory fetch reserves through {@link #reserve(long, Duration)}: it
 * waits a bounded time for room in first-come order and reports {@code false} when the wait expires, and the caller
 * then falls back to disk. Reservations are byte-granular and released when the owning {@code RowGroupFetch} is closed.
 */
public final class FetchBudget {

    private static final double DEFAULT_FRACTION = 0.1;

    private final long capacity;
    private final ByteQueue queue;
    private final LongAdder waits = new LongAdder();
    private final LongAdder waitedNanos = new LongAdder();
    private final LongAdder expired = new LongAdder();

    private FetchBudget(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("FetchBudget capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.queue = new ByteQueue(capacity);
    }

    /** A budget capped at {@code maxBytes}. */
    public static FetchBudget ofBytes(long maxBytes) {
        return new FetchBudget(maxBytes);
    }

    /** A budget capped at {@code fraction} of {@code min(maxHeap, maxDirectMemory)}; {@code fraction} in (0, 1]. */
    public static FetchBudget ofMaxMemoryFraction(double fraction) {
        if (Double.isNaN(fraction) || fraction <= 0.0 || fraction > 1.0) {
            throw new IllegalArgumentException("fraction must be in (0, 1], got " + fraction);
        }
        long basis = Math.min(Runtime.getRuntime().maxMemory(), maxDirectMemoryBytes());
        long capacity = Math.max(1L, (long) (basis * fraction));
        return new FetchBudget(capacity);
    }

    /** The shared default budget: a deliberately conservative fraction of available memory. */
    public static FetchBudget defaultBudget() {
        return DefaultHolder.INSTANCE;
    }

    /**
     * Tries to reserve {@code bytes} without waiting. A {@code false} result is a normal outcome, not an error: the
     * caller must not speculate further. The reservation is refused while a mandatory fetch is queued for room, even
     * when the bytes are free: speculation must not take the room owed to the first waiter in line. A non-positive
     * request reserves nothing and succeeds.
     */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        return queue.tryReserveNow(bytes);
    }

    /**
     * Reserves {@code bytes}, waiting up to {@code maxWait} in first-come order for room. Returns {@code false} when
     * the wait expires, reserving nothing; the caller then falls back to disk. A request above the capacity fails at
     * once, because no release could ever satisfy it, and a zero wait reserves only when there is room now. An
     * interrupt during the wait is restored on the thread and counts as an expired wait.
     */
    public boolean reserve(long bytes, Duration maxWait) {
        if (bytes <= 0) {
            return true;
        }
        if (bytes > capacity) {
            return false;
        }
        if (queue.tryReserveNow(bytes)) {
            return true;
        }
        if (maxWait.isZero()) {
            return false;
        }
        return waitForRoom(bytes, maxWait);
    }

    private boolean waitForRoom(long bytes, Duration maxWait) {
        waits.increment();
        long start = System.nanoTime();
        try {
            boolean granted = queue.tryAcquireSharedNanos(bytes, TimeUnit.NANOSECONDS.convert(maxWait));
            if (!granted) {
                expired.increment();
            }
            return granted;
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
            expired.increment();
            return false;
        } finally {
            waitedNanos.add(System.nanoTime() - start);
        }
    }

    /**
     * Returns up to {@code bytes} of headroom to the budget and lets the first queued waiter try again. A release is
     * clamped to the remaining headroom: a redundant, mismatched, or oversized release can neither inflate
     * {@code available} above {@code capacity} nor overflow it.
     */
    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        queue.releaseShared(bytes);
    }

    public long capacity() {
        return capacity;
    }

    public long available() {
        return queue.available();
    }

    /** How often mandatory fetches waited for room, how long in total, and how many waits expired into a spill. */
    public WaitStats waitStats() {
        return new WaitStats(waits.sum(), waitedNanos.sum(), expired.sum());
    }

    /**
     * The wait counters of a budget.
     *
     * @param waits reservations that parked for room
     * @param waitedNanos the total time spent parked by those reservations
     * @param expired the waits that ended without room, each of them a spill or an interrupted fetch
     */
    public record WaitStats(long waits, long waitedNanos, long expired) {}

    private static long maxDirectMemoryBytes() {
        try {
            HotSpotDiagnosticMXBean diagnostic = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
            long configured =
                    Long.parseLong(diagnostic.getVMOption("MaxDirectMemorySize").getValue());
            if (configured > 0) {
                return configured;
            }
        } catch (RuntimeException _) {
            // The HotSpot diagnostic bean is absent on non-HotSpot JVMs and its option value can be non-numeric.
            // Either way, fall back to the heap size as the memory basis.
        }
        return Runtime.getRuntime().maxMemory();
    }

    /**
     * A fair byte semaphore. The state is the available bytes; a waiter queues in first-come order, and the head of the
     * queue is served before any later request, however small that request is.
     */
    private static final class ByteQueue extends AbstractQueuedLongSynchronizer {

        private static final long serialVersionUID = 1L;

        private final long capacity;

        ByteQueue(long capacity) {
            this.capacity = capacity;
            setState(capacity);
        }

        long available() {
            return getState();
        }

        /** Takes {@code bytes} when they are free now and no waiter is queued; never parks. */
        boolean tryReserveNow(long bytes) {
            if (hasQueuedThreads()) {
                return false;
            }
            return takeIfFree(bytes) >= 0;
        }

        @Override
        protected long tryAcquireShared(long bytes) {
            if (hasQueuedPredecessors()) {
                return -1;
            }
            return takeIfFree(bytes);
        }

        /** The bytes left after taking {@code bytes}, or -1 when they are not all free. */
        private long takeIfFree(long bytes) {
            while (true) {
                long free = getState();
                long remaining = free - bytes;
                if (remaining < 0) {
                    return -1;
                }
                if (compareAndSetState(free, remaining)) {
                    return remaining;
                }
            }
        }

        @Override
        protected boolean tryReleaseShared(long bytes) {
            while (true) {
                long free = getState();
                long grant = Math.min(bytes, capacity - free);
                if (compareAndSetState(free, free + grant)) {
                    return true;
                }
            }
        }
    }

    private static final class DefaultHolder {
        private static final FetchBudget INSTANCE = FetchBudget.ofMaxMemoryFraction(DEFAULT_FRACTION);
    }
}
