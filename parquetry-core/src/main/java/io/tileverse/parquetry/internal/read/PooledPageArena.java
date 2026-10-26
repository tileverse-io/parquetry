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

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.io.SegmentPool;

/**
 * The arena of one decompressed page. It is confined to the loading thread; its allocations are borrowed from the
 * read's {@link SegmentPool} and returned when it closes. The page readers allocate the decompressed bytes from the
 * arena handed to them and never see the pool.
 *
 * <p>A page buffer lives for one page load, or across the page's batches when a live-page backing reads from it. A
 * fresh native allocation per page would map and unmap memory thousands of times a second under load. Borrowing from
 * the pool reuses the buffers of earlier pages instead. A pooled buffer is not zeroed; a reader may only read bytes
 * written to it first.
 */
final class PooledPageArena implements Arena {

    private final Arena owner = Arena.ofConfined();
    private final SegmentPool pool;

    PooledPageArena(SegmentPool pool) {
        this.pool = pool;
    }

    /**
     * A pooled segment of {@code byteSize} bytes bound to the lifetime of this arena. A pooled backing starts on a page
     * or a malloc boundary; a stricter alignment is served by a plain allocation.
     */
    @Override
    public MemorySegment allocate(long byteSize, long byteAlignment) {
        SegmentPool.Pooled pooled = pool.borrow(byteSize);
        MemorySegment segment = pooled.segment();
        if (byteAlignment > 1 && segment.address() % byteAlignment != 0) {
            pooled.close();
            return owner.allocate(byteSize, byteAlignment);
        }
        return segment.reinterpret(byteSize, owner, _ -> pooled.close());
    }

    @Override
    public MemorySegment.Scope scope() {
        return owner.scope();
    }

    /** Returns the page's buffers to the pool. */
    @Override
    public void close() {
        owner.close();
    }
}
