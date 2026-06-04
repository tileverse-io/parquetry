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
package io.tileverse.parquetry.internal.read;

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
 * <p>Only speculative decode-ahead reserves against this budget. The in-order current row group never reserves (it is
 * mandatory for progress); when the consumer advances to a previously speculative row group, the coordinator promotes
 * it and calls {@link #wakeWaiters()} to release a worker parked in {@link #reserve(long, BooleanSupplier)}.
 */
public final class DecodeBudget {

    private static final double DEFAULT_FRACTION = 0.25;

    private final long capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition headroomChanged = lock.newCondition();
    private long available;

    private DecodeBudget(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("DecodeBudget capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.available = capacity;
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
     * speculate further until headroom frees. A non-positive request reserves nothing and succeeds.
     */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        lock.lock();
        try {
            if (available >= bytes) {
                available -= bytes;
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reserves {@code bytes}, blocking until headroom frees or {@code giveUp} returns {@code true}. Returns
     * {@code true} if the bytes were reserved, {@code false} if it gave up first (reserving nothing). A non-positive
     * request reserves nothing and returns {@code true}. {@code giveUp} is re-checked on every wakeup; a promotion that
     * flips it must therefore also call {@link #wakeWaiters()}.
     */
    public boolean reserve(long bytes, BooleanSupplier giveUp) {
        if (bytes <= 0) {
            return true;
        }
        lock.lock();
        try {
            while (available < bytes) {
                if (giveUp.getAsBoolean()) {
                    return false;
                }
                headroomChanged.awaitUninterruptibly();
            }
            available -= bytes;
            return true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns up to {@code bytes} of headroom to the budget and wakes any parked reservers. A release is clamped to the
     * remaining headroom; a redundant, mismatched, or oversized release can therefore neither inflate {@code available}
     * above {@code capacity} nor overflow it.
     */
    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        lock.lock();
        try {
            long headroom = capacity - available;
            long grant = Math.min(bytes, headroom);
            available += grant;
            headroomChanged.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Wakes every parked reserver to re-check its give-up condition. Called by the coordinator when a speculative row
     * group is promoted to current (or a read is closing); a worker parked on an oversized or starved reservation then
     * stops waiting.
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
        lock.lock();
        try {
            return available;
        } finally {
            lock.unlock();
        }
    }

    private static final class DefaultHolder {
        private static final DecodeBudget INSTANCE = DecodeBudget.ofMaxMemoryFraction(DEFAULT_FRACTION);
    }
}
