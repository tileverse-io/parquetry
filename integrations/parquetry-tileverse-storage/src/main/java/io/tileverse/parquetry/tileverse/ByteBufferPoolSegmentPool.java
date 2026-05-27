/*
 * Copyright (c) 2026 Tileverse.io
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
package io.tileverse.parquetry.tileverse;

import java.lang.foreign.MemorySegment;

import io.tileverse.parquetry.io.SegmentPool;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

/**
 * A {@link SegmentPool} backed by a tileverse {@link ByteBufferPool}. A co-resident reader that already leans on a
 * {@code ByteBufferPool} (for example a PMTiles store on the same instance) and parquetry then share one physical pool.
 * {@link #borrow} delegates to {@link ByteBufferPool#borrowDirect}, views the pooled direct buffer as a
 * {@link MemorySegment}, and {@link SegmentPool.Pooled#close()} returns the buffer to the backing pool. The pooled
 * {@code buffer()} is already sliced to the requested length, keeping the segment's {@code byteSize} exact.
 */
final class ByteBufferPoolSegmentPool implements SegmentPool {

    private final ByteBufferPool pool;

    ByteBufferPoolSegmentPool(ByteBufferPool pool) {
        this.pool = pool;
    }

    @Override
    public Pooled borrow(long byteSize) {
        if (byteSize < 0 || byteSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("byteSize must be in [0, Integer.MAX_VALUE], got " + byteSize);
        }
        PooledByteBuffer pooled = pool.borrowDirect((int) byteSize);
        MemorySegment segment = MemorySegment.ofBuffer(pooled.buffer());
        return new PooledBuffer(pooled, segment);
    }

    private static final class PooledBuffer implements Pooled {

        private final MemorySegment segment;
        private PooledByteBuffer pooled;

        PooledBuffer(PooledByteBuffer pooled, MemorySegment segment) {
            this.pooled = pooled;
            this.segment = segment;
        }

        @Override
        public MemorySegment segment() {
            return segment;
        }

        @Override
        public void close() {
            if (pooled == null) {
                return;
            }
            pooled.close();
            pooled = null;
        }
    }
}
