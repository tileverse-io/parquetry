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
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.FetchBudget;

import lombok.NonNull;

/**
 * The RAM-or-mmap valve for a mandatory fetch buffer. A mandatory fetch is needed for progress and must always get a
 * buffer; it never parks and never returns {@code null}. When the {@link FetchBudget} has room the buffer is a native
 * {@link SegmentPool} segment; otherwise it is a {@link FetchSpillStore} mmap of an on-disk file, which trades RAM for
 * disk while disk has headroom.
 *
 * <p>This valve is always active and independent of the decode-batch spill toggle ({@code spillEnabled} on the
 * runtime). That toggle controls whether already-decoded batches may spill; the current row group's fetch must complete
 * regardless of it, hence the mandatory fetch always takes RAM-or-mmap and never parks.
 *
 * <p>Speculative prefetch reserves its span up front against the {@link FetchBudget} elsewhere and does not use this
 * allocator.
 */
public final class FetchBufferAllocator {

    private final SegmentPool segmentPool;
    private final FetchBudget fetchBudget;
    private final FetchSpillStore spillStore;

    public FetchBufferAllocator(
            @NonNull SegmentPool segmentPool, @NonNull FetchBudget fetchBudget, @NonNull FetchSpillStore spillStore) {
        this.segmentPool = segmentPool;
        this.fetchBudget = fetchBudget;
        this.spillStore = spillStore;
    }

    /**
     * Acquires a buffer of exactly {@code size} bytes for a mandatory fetch. Prefers a RAM segment reserved against the
     * fetch budget; falls back to a disk-backed mmap when the budget has no room. Never returns {@code null}.
     */
    SegmentPool.Pooled acquireMandatory(long size) {
        if (fetchBudget.tryReserve(size)) {
            return borrowRam(size);
        }
        return spillStore.map(size);
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
