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
import java.util.ArrayList;
import java.util.List;

import io.tileverse.parquetry.io.SegmentPool;

/**
 * The off-heap buffers of one batch, returned together when the batch closes. A batch window takes a few small buffers
 * per column: the values, the validity bitmap, the binary offsets. Carving them out of one slab, borrowed from the
 * decode valve, divides the pool traffic by the number of buffers in a slab. A buffer above {@link #MAX_CARVED_BYTES}
 * is borrowed on its own.
 *
 * <p>One thread assembles the batch. The batch closes all of its buffers at once, possibly on another thread. A slab
 * returns to the pool with the batch like any other buffer, hence carving needs no reference counts.
 */
final class BatchBuffers implements AutoCloseable {

    static final long SLAB_BYTES = 64L * 1024;
    static final long MAX_CARVED_BYTES = 16L * 1024;
    private static final long ALIGNMENT = 8;
    private static final MemorySegment EMPTY =
            Arena.global().allocate(ALIGNMENT).asSlice(0L, 0L);

    private final DecodeBufferAllocator allocator;
    private final List<AutoCloseable> owned = new ArrayList<>();
    private MemorySegment slab;
    private long slabOffset;

    BatchBuffers(DecodeBufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * A buffer of {@code byteSize} bytes that lives until the batch closes, aligned to eight bytes. The bytes are not
     * cleared.
     *
     * @throws io.tileverse.parquetry.runtime.BudgetExhaustedException when the decode valve has no room for it
     */
    MemorySegment allocate(long byteSize) {
        if (byteSize == 0) {
            return EMPTY;
        }
        if (byteSize > MAX_CARVED_BYTES) {
            return borrow(byteSize);
        }
        long alignedSize = (byteSize + ALIGNMENT - 1) & -ALIGNMENT;
        if (slab == null || slabOffset + alignedSize > SLAB_BYTES) {
            slab = borrow(SLAB_BYTES);
            slabOffset = 0;
        }
        MemorySegment carved = slab.asSlice(slabOffset, byteSize);
        slabOffset += alignedSize;
        return carved;
    }

    private MemorySegment borrow(long byteSize) {
        SegmentPool.Pooled pooled = allocator.acquireMandatory(byteSize);
        owned.add(pooled);
        return pooled.segment();
    }

    /** Hands {@code resource} to the batch, to be closed with the buffers. */
    void add(AutoCloseable resource) {
        owned.add(resource);
    }

    /** Closes the buffers in reverse order, best-effort: one failing close must not strand the others. Idempotent. */
    @Override
    public void close() {
        for (int i = owned.size() - 1; i >= 0; i--) {
            try {
                owned.get(i).close();
            } catch (Exception _) {
                // best-effort release; one failure must not strand the others
            }
        }
        owned.clear();
        slab = null;
    }
}
