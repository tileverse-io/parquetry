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

import io.tileverse.parquetry.columnar.Levels;
import io.tileverse.parquetry.internal.read.page.LevelDecoder;
import io.tileverse.parquetry.io.SegmentPool;

/**
 * A column reader's reusable off-heap buffer for one level stream (rep or def). {@link #decode} fills the buffer from
 * the loaded decoder and returns a {@link Levels} view over it; the view is valid until the next {@link #decode} call
 * on the same scratch, which is the reader's next page decode. The buffer comes from the decode valve
 * (budget-accounted, RAM-or-mmap), grows to the largest page seen (next power of two), and is released at
 * {@link #close()} - reader close, not page advance, because reuse across pages is the point.
 */
final class LevelScratch implements AutoCloseable {

    private static final long MIN_CAPACITY_BYTES = 8192;

    private final DecodeBufferAllocator allocator;
    private SegmentPool.Pooled pooled;

    LevelScratch(DecodeBufferAllocator allocator) {
        this.allocator = allocator;
    }

    /** Decodes {@code count} levels from the loaded {@code decoder} into this scratch; see the class contract. */
    Levels decode(LevelDecoder decoder, int count) {
        MemorySegment dst = ensureCapacity(4L * count);
        decoder.decodeInto(count, dst);
        return Levels.ofSegment(dst, count);
    }

    private MemorySegment ensureCapacity(long byteSize) {
        if (pooled != null && pooled.segment().byteSize() >= byteSize) {
            return pooled.segment();
        }
        close();
        pooled = allocator.acquireMandatory(Math.max(MIN_CAPACITY_BYTES, ceilPowerOfTwo(byteSize)));
        return pooled.segment();
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
