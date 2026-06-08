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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

/**
 * Bounds the total bytes a read may write to spill files. Shared process-wide so that, regardless of how many
 * concurrent reads spill, total spilled disk stays under one cap and cannot exhaust the temp filesystem.
 *
 * <p>A producer that cannot reserve heap in {@link DecodeBudget} spills the decoded batch and reserves its serialized
 * size here; the reservation releases when the consumer restores the batch or the read closes. When this budget is also
 * full the producer parks as a last resort.
 */
public final class DiskBudget {

    private static final double DEFAULT_FRACTION = 0.5;
    private static final long DEFAULT_CAP_BYTES = 8L << 30;

    private final long capacity;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition headroomChanged = lock.newCondition();
    private long available;
    private long minAvailable;

    private DiskBudget(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("DiskBudget capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.available = capacity;
        this.minAvailable = capacity;
    }

    /** A budget capped at {@code maxBytes}. */
    public static DiskBudget ofBytes(long maxBytes) {
        return new DiskBudget(maxBytes);
    }

    /**
     * A budget capped at {@code fraction} of the usable space on {@code dir}'s filesystem; {@code fraction} in (0, 1].
     */
    public static DiskBudget ofFractionOfFree(double fraction, Path dir) {
        if (Double.isNaN(fraction) || fraction <= 0.0 || fraction > 1.0) {
            throw new IllegalArgumentException("fraction must be in (0, 1], got " + fraction);
        }
        long usable = usableSpace(dir);
        long capacity = Math.max(1L, (long) (usable * fraction));
        return new DiskBudget(capacity);
    }

    /** The shared default budget: half the temp directory's free space, capped to keep one runaway read bounded. */
    public static DiskBudget defaultBudget() {
        return DefaultHolder.INSTANCE;
    }

    /**
     * Tries to reserve {@code bytes} without blocking. A {@code false} result is a normal outcome (disk full); the
     * caller parks. A non-positive request reserves nothing and succeeds.
     */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        lock.lock();
        try {
            if (available >= bytes) {
                consume(bytes);
                return true;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reserves {@code bytes}, blocking until headroom frees or {@code giveUp} returns {@code true}. Returns
     * {@code true} if reserved, {@code false} if it gave up first. {@code giveUp} is re-checked on every wakeup.
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
            consume(bytes);
            return true;
        } finally {
            lock.unlock();
        }
    }

    /** Deducts a reservation and records the low-water mark. Caller holds {@link #lock}. */
    private void consume(long bytes) {
        available -= bytes;
        minAvailable = Math.min(minAvailable, available);
    }

    /** Returns up to {@code bytes} of headroom and wakes parked reservers, clamped to never exceed {@code capacity}. */
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

    /** Wakes every parked reserver to re-check its give-up condition (called when a read is closing). */
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

    /**
     * The lowest {@link #available()} ever reached: the peak disk a spill held at once. Monotonically non-increasing,
     * so it records that a spill occurred even after the reservation has been released. Useful for observability and
     * for tests that must detect a transient spill without racing its release.
     */
    public long minAvailable() {
        lock.lock();
        try {
            return minAvailable;
        } finally {
            lock.unlock();
        }
    }

    private static long usableSpace(Path dir) {
        try {
            FileStore store = Files.getFileStore(dir);
            return Math.max(1L, store.getUsableSpace());
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot determine usable space for spill directory " + dir, e);
        }
    }

    private static final class DefaultHolder {
        private static final DiskBudget INSTANCE = build();

        private static DiskBudget build() {
            Path tempDir = Path.of(System.getProperty("java.io.tmpdir"));
            long byFraction = (long) (usableSpace(tempDir) * DEFAULT_FRACTION);
            return DiskBudget.ofBytes(Math.clamp(byFraction, 1L, DEFAULT_CAP_BYTES));
        }
    }
}
