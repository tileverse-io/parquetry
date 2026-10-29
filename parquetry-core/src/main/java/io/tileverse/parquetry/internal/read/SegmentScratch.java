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

import io.tileverse.parquetry.io.SegmentPool;

/**
 * A column reader's off-heap buffer, reused across pages. A view returned by {@link #acquire} stays valid until the
 * next {@link #acquire} on the same scratch: the reader's next page decode. The buffer comes from the decode valve
 * (budget-accounted, RAM-or-mmap), grows to the largest page seen (next power of two), and is released at
 * {@link #close()}: reader close, not page advance, because reuse across pages is the point.
 */
final class SegmentScratch implements AutoCloseable {

    private static final long MIN_CAPACITY_BYTES = 8192;

    private final DecodeBufferAllocator allocator;
    private SegmentPool.Pooled pooled;

    SegmentScratch(DecodeBufferAllocator allocator) {
        this.allocator = allocator;
    }

    /**
     * A view of {@code byteSize} bytes of the buffer. The bytes are not cleared: they hold what an earlier page or an
     * earlier borrower of the pooled buffer wrote.
     */
    MemorySegment acquire(long byteSize) {
        if (pooled == null || pooled.segment().byteSize() < byteSize) {
            close();
            pooled = allocator.acquireMandatory(Math.max(MIN_CAPACITY_BYTES, ceilPowerOfTwo(byteSize)));
        }
        return pooled.segment().asSlice(0L, byteSize);
    }

    private static long ceilPowerOfTwo(long byteSize) {
        return 1L << (64 - Long.numberOfLeadingZeros(byteSize - 1));
    }

    /** Returns the buffer to the pool; idempotent. */
    @Override
    public void close() {
        if (pooled != null) {
            pooled.close();
            pooled = null;
        }
    }
}
