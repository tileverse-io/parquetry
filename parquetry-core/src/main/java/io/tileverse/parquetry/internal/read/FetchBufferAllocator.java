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
package io.tileverse.parquetry.internal.read;

import java.lang.foreign.MemorySegment;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.BudgetExhaustedException;
import io.tileverse.parquetry.runtime.FetchBudget;

import lombok.NonNull;

/**
 * The valve of a mandatory fetch buffer. A mandatory fetch is needed for progress and must get a buffer. The buffer is
 * a native {@link SegmentPool} segment when the {@link FetchBudget} has room, or gets room within the wait allowed for
 * it; a {@link FetchSpillStore} mapping of an on-disk file when the wait expires; and the fetch fails with
 * {@link BudgetExhaustedException} when the disk budget is exhausted as well. The wait is bounded, and the fetch holds
 * no other fetch buffer while it waits; that is what keeps the wait from deadlocking.
 *
 * <p>This valve is always active and independent of the decode-batch spill toggle ({@code spillEnabled} on the
 * runtime). That toggle controls whether already-decoded batches may spill; the current row group's fetch must complete
 * regardless of it.
 *
 * <p>Speculative prefetch reserves its span up front against the {@link FetchBudget} elsewhere and does not use this
 * allocator.
 */
public final class FetchBufferAllocator {

    private final SegmentPool segmentPool;
    private final FetchBudget fetchBudget;
    private final FetchSpillStore spillStore;
    private final Duration mandatoryBufferWait;

    /**
     * @param mandatoryBufferWait the longest wait of a mandatory fetch for room in the fetch budget before it spills;
     *     zero spills at once
     */
    public FetchBufferAllocator(
            @NonNull SegmentPool segmentPool,
            @NonNull FetchBudget fetchBudget,
            @NonNull FetchSpillStore spillStore,
            @NonNull Duration mandatoryBufferWait) {
        this.segmentPool = segmentPool;
        this.fetchBudget = fetchBudget;
        this.spillStore = spillStore;
        this.mandatoryBufferWait = mandatoryBufferWait;
    }

    /**
     * Acquires a buffer of exactly {@code size} bytes for a mandatory fetch: a RAM segment when the fetch budget has
     * room now or within the wait, a disk-backed mapping otherwise. Never returns {@code null}.
     *
     * @throws BudgetExhaustedException when the disk budget has no room for the buffer either
     */
    SegmentPool.Pooled acquireMandatory(long size) {
        if (fetchBudget.reserve(size, mandatoryBufferWait)) {
            return borrowRam(size);
        }
        return spillStore.tryMap(size).orElseThrow(() -> exhausted(size));
    }

    private BudgetExhaustedException exhausted(long size) {
        return new BudgetExhaustedException("No room for a mandatory fetch buffer of " + size
                + " bytes: the fetch budget (capacity " + fetchBudget.capacity() + ", available "
                + fetchBudget.available() + ") had no room within " + mandatoryBufferWait
                + ", and the disk budget is exhausted");
    }

    private SegmentPool.Pooled borrowRam(long size) {
        SegmentPool.Pooled borrowed;
        try {
            borrowed = segmentPool.borrow(size);
        } catch (RuntimeException e) {
            fetchBudget.release(size);
            throw e;
        }
        return new RamPooled(borrowed, fetchBudget, size);
    }

    /**
     * Owns a borrowed native segment plus its fetch-budget reservation. Closing returns the segment to the pool and
     * releases the reserved bytes, guarded to run once for the owning borrower.
     */
    private static final class RamPooled implements SegmentPool.Pooled {

        private final SegmentPool.Pooled borrowed;
        private final FetchBudget fetchBudget;
        private final long reservedBytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private RamPooled(SegmentPool.Pooled borrowed, FetchBudget fetchBudget, long reservedBytes) {
            this.borrowed = borrowed;
            this.fetchBudget = fetchBudget;
            this.reservedBytes = reservedBytes;
        }

        @Override
        public MemorySegment segment() {
            return borrowed.segment();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            borrowed.close();
            fetchBudget.release(reservedBytes);
        }
    }
}
