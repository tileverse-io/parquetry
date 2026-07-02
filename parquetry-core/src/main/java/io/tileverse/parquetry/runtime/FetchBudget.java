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
package io.tileverse.parquetry.runtime;

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.management.HotSpotDiagnosticMXBean;

/**
 * Bounds the total in-flight coalesced bytes a read may speculatively prefetch. Shared process-wide so that, regardless
 * of how many concurrent reads run, peak speculative fetch memory stays under one cap.
 *
 * <p>{@link #tryReserve(long)} is non-blocking: it gates speculative prefetch only. The current row group's fetch is
 * never gated (it is mandatory for progress), so peak fetch memory is at most one row group's coalesced span plus the
 * reserved prefetch bytes. Reservations are byte-granular and released when the owning {@code RowGroupFetch} is closed.
 */
public final class FetchBudget {

    private static final double DEFAULT_FRACTION = 0.1;

    private final long capacity;
    private final AtomicLong available;

    private FetchBudget(long capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("FetchBudget capacity must be > 0, got " + capacity);
        }
        this.capacity = capacity;
        this.available = new AtomicLong(capacity);
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
     * Tries to reserve {@code bytes} without blocking. A {@code false} result is a normal outcome, not an error: the
     * caller must fall back to a non-speculative (inline) fetch rather than waiting. A non-positive request reserves
     * nothing and succeeds.
     */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        long current = available.get();
        while (current >= bytes) {
            if (available.compareAndSet(current, current - bytes)) {
                return true;
            }
            current = available.get();
        }
        return false;
    }

    /**
     * Returns up to {@code bytes} of headroom to the budget. A release is clamped to the remaining headroom, so a
     * redundant, mismatched, or oversized release can neither inflate {@code available} above {@code capacity} nor
     * overflow it.
     */
    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        available.updateAndGet(current -> {
            long headroom = capacity - current;
            long grant = Math.min(bytes, headroom);
            return current + grant;
        });
    }

    public long capacity() {
        return capacity;
    }

    public long available() {
        return available.get();
    }

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

    private static final class DefaultHolder {
        private static final FetchBudget INSTANCE = FetchBudget.ofMaxMemoryFraction(DEFAULT_FRACTION);
    }
}
