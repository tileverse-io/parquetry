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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Bounds the total heap bytes a read may hold in speculatively decoded batches. Shared process-wide so that, regardless
 * of how many concurrent reads run, peak speculative decode memory stays under one cap.
 *
 * <p>The basis is heap alone ({@link Runtime#maxMemory()}): decoded batches are on-heap, unlike {@link FetchBudget}'s
 * off-heap fetch buffers. {@code maxMemory()} reflects the container's memory limit; a fraction therefore auto-scales
 * per pod.
 *
 * <p>Every decoded row group reserves against this budget. A worker that cannot reserve a batch's heap bytes spills the
 * batch to disk rather than parking; it parks in {@link #reserve(long, BooleanSupplier)} only as a last resort, when
 * the disk budget is also full. A read that is closing calls {@link #wakeWaiters()} to release any such parked worker.
 *
 * <p>A reservation and a release take no lock: the decode workers of a loaded server reserve and release a buffer for
 * each page and each batch window, a million times a second between them, and a lock handed between that many threads
 * that often costs more than the work it guards. The headroom is an atomic counter; a reservation subtracts its bytes
 * and adds them back when the result went negative, one atomic add each, with no retry loop to spin on under
 * contention. The lock and its condition serve the last-resort wait only, and a release signals them only while a
 * reserver is parked.
 */
public final class DecodeBudget {

    private static final double DEFAULT_FRACTION = 0.25;

    private final long capacity;
    private final AtomicLong available;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition headroomChanged = lock.newCondition();

    /**
     * Reservers parked in {@link #reserve(long, BooleanSupplier)}; a release signals the condition only when positive.
     */
    private final AtomicInteger waiters = new AtomicInteger();

    private DecodeBudget(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("DecodeBudget capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.available = new AtomicLong(capacity);
    }

    /** A budget capped at {@code maxBytes}. */
    public static DecodeBudget ofBytes(long maxBytes) {
        return new DecodeBudget(maxBytes);
    }

    /** A budget capped at {@code fraction} of the heap ({@link Runtime#maxMemory()}); {@code fraction} in (0, 1]. */
    public static DecodeBudget ofMaxMemoryFraction(double fraction) {
        if (Double.isNaN(fraction) || fraction <= 0.0 || fraction > 1.0) {
            throw new IllegalArgumentException("fraction must be in (0, 1], got " + fraction);
        }
        long basis = Runtime.getRuntime().maxMemory();
        long capacity = Math.max(1L, (long) (basis * fraction));
        return new DecodeBudget(capacity);
    }

    /** The shared default budget: a quarter of the heap, leaving room for the consumer's working set. */
    public static DecodeBudget defaultBudget() {
        return DefaultHolder.INSTANCE;
    }

    /**
     * Tries to reserve {@code bytes} without blocking. A {@code false} result is a normal outcome: the caller must not
     * speculate further until headroom frees. A refused request leaves the headroom as it was, and cannot make a
     * fitting request on another thread fail. A non-positive request reserves nothing and succeeds.
     */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        long free = available.get();
        while (free >= bytes) {
            if (available.compareAndSet(free, free - bytes)) {
                return true;
            }
            free = available.get();
        }
        return false;
    }

    /**
     * Reserves {@code bytes}, blocking until headroom frees or {@code giveUp} returns {@code true}. Returns
     * {@code true} if the bytes were reserved, {@code false} if it gave up first (reserving nothing). A non-positive
     * request reserves nothing and returns {@code true}. {@code giveUp} is re-checked on every wakeup; whatever flips
     * it must therefore also call {@link #wakeWaiters()}.
     *
     * <p>The reserver registers as a waiter before it looks at the headroom and holds the lock until it parks, and a
     * release that sees a registered waiter takes the same lock before it signals: a release between the look and the
     * park cannot be missed.
     */
    public boolean reserve(long bytes, BooleanSupplier giveUp) {
        if (bytes <= 0) {
            return true;
        }
        lock.lock();
        waiters.incrementAndGet();
        try {
            while (!tryReserve(bytes)) {
                if (giveUp.getAsBoolean()) {
                    return false;
                }
                headroomChanged.awaitUninterruptibly();
            }
            return true;
        } finally {
            waiters.decrementAndGet();
            lock.unlock();
        }
    }

    /**
     * Returns up to {@code bytes} of headroom to the budget and wakes any parked reservers. A release is clamped to the
     * remaining headroom; a redundant, mismatched, or oversized release can therefore neither inflate {@code available}
     * above {@code capacity} nor overflow it. The add and the clamp are one atomic update: no reservation sees headroom
     * above the capacity.
     */
    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        available.accumulateAndGet(bytes, (free, more) -> free + Math.min(more, capacity - free));
        if (waiters.get() > 0) {
            wakeWaiters();
        }
    }

    /**
     * Wakes every parked reserver to re-check its give-up condition. Called when a read is closing; a worker parked in
     * the last-resort reservation (heap and disk both full) then stops waiting and gives up.
     */
    public void wakeWaiters() {
        lock.lock();
        try {
            headroomChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public long capacity() {
        return capacity;
    }

    public long available() {
        return available.get();
    }

    private static final class DefaultHolder {
        private static final DecodeBudget INSTANCE = DecodeBudget.ofMaxMemoryFraction(DEFAULT_FRACTION);
    }
}
