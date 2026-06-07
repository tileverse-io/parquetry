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

import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.parquetry.io.SegmentPool;

import lombok.NonNull;

/**
 * The RAM-or-mmap valve for a mandatory decode buffer. A mandatory decode buffer is needed for progress and must always
 * get a segment; it never parks and never returns {@code null}. When the {@link DecodeBudget} has room the buffer is a
 * native {@link SegmentPool} segment; otherwise it is a {@link FetchSpillStore} mmap of an on-disk file, which trades
 * RAM for disk while disk has headroom.
 *
 * <p>This mirrors {@link FetchBufferAllocator}: same RAM-or-mmap shape, same never-park guarantee, and the same shared
 * disk budget via the mmap spill store. The two differ only in which budget the RAM path reserves against - the decode
 * buffer reserves against the native decode budget rather than the fetch budget.
 */
public final class DecodeBufferAllocator {

    private final SegmentPool segmentPool;
    private final DecodeBudget decodeBudget;
    private final FetchSpillStore spillStore;

    public DecodeBufferAllocator(
            @NonNull SegmentPool segmentPool, @NonNull DecodeBudget decodeBudget, @NonNull FetchSpillStore spillStore) {
        this.segmentPool = segmentPool;
        this.decodeBudget = decodeBudget;
        this.spillStore = spillStore;
    }

    /**
     * Acquires a buffer of exactly {@code size} bytes for a mandatory decode. Prefers a RAM segment reserved against
     * the decode budget; falls back to a disk-backed mmap when the budget has no room. Never returns {@code null}.
     */
    SegmentPool.Pooled acquireMandatory(long size) {
        if (decodeBudget.tryReserve(size)) {
            return borrowRam(size);
        }
        return spillStore.map(size);
    }

    private SegmentPool.Pooled borrowRam(long size) {
        SegmentPool.Pooled borrowed;
        try {
            borrowed = segmentPool.borrow(size);
        } catch (RuntimeException e) {
            decodeBudget.release(size);
            throw e;
        }
        return new RamPooled(borrowed, decodeBudget, size);
    }

    /**
     * Owns a borrowed native segment plus its decode-budget reservation. Closing returns the segment to the pool and
     * releases the reserved bytes, guarded to run once for the owning borrower.
     */
    private static final class RamPooled implements SegmentPool.Pooled {

        private final SegmentPool.Pooled borrowed;
        private final DecodeBudget decodeBudget;
        private final long reservedBytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private RamPooled(SegmentPool.Pooled borrowed, DecodeBudget decodeBudget, long reservedBytes) {
            this.borrowed = borrowed;
            this.decodeBudget = decodeBudget;
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
            decodeBudget.release(reservedBytes);
        }
    }
}
