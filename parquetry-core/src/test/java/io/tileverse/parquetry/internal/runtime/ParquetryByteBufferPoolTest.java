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
package io.tileverse.parquetry.internal.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.ByteBuffer;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;

import io.tileverse.io.ByteBufferPool;
import io.tileverse.io.ByteBufferPool.PoolStatistics;
import io.tileverse.io.ByteBufferPool.PooledByteBuffer;

class ParquetryByteBufferPoolTest {

    private final ParquetryByteBufferPool pool = new ParquetryByteBufferPool();

    @Test
    void borrowDirectReturnsDirectBufferSizedToRequest() {
        int size = 4096;
        try (PooledByteBuffer pooled = pool.borrowDirect(size)) {
            ByteBuffer buffer = pooled.buffer();
            assertThat(buffer.capacity()).as("direct buffer capacity").isEqualTo(size);
            assertThat(buffer.isDirect()).as("buffer is direct").isTrue();
        }
    }

    @Test
    void borrowDirectBufferRoundTripsAByte() {
        try (PooledByteBuffer pooled = pool.borrowDirect(16)) {
            ByteBuffer buffer = pooled.buffer();
            buffer.put(0, (byte) 0x7f);
            assertThat(buffer.get(0)).as("round-tripped byte").isEqualTo((byte) 0x7f);
        }
    }

    @Test
    void closeIsIdempotent() {
        PooledByteBuffer pooled = pool.borrowDirect(64);
        pooled.close();
        assertThatCode(pooled::close).as("second close must not throw").doesNotThrowAnyException();
    }

    @Test
    void borrowHeapReturnsArrayBackedBufferSizedToRequest() {
        int size = 2048;
        try (PooledByteBuffer pooled = pool.borrowHeap(size)) {
            ByteBuffer buffer = pooled.buffer();
            assertThat(buffer.hasArray()).as("heap buffer is array-backed").isTrue();
            assertThat(buffer.arrayOffset())
                    .as("array offset seen by callers writing through array()")
                    .isZero();
            assertThat(buffer.capacity()).as("heap buffer capacity").isEqualTo(size);
            assertThat(buffer.position()).as("position").isZero();
            assertThat(buffer.limit()).as("limit").isEqualTo(size);
        }
    }

    @Test
    void heapBorrowsAreReusedBetweenBorrows() {
        int size = 64 * 1024;
        long reusedBefore = pool.getHeapPoolStatistics().reused();
        pool.borrowHeap(size).close();
        pool.borrowHeap(size).close();

        PoolStatistics stats = pool.getHeapPoolStatistics();
        assertThat(stats.reused())
                .as("second borrow of the same size served from the free list")
                .isGreaterThan(reusedBefore);
        assertThat(stats.returned()).as("returns retained in the free list").isPositive();
    }

    @Test
    void heapHandleClosesFromAnotherThread() throws InterruptedException {
        long returnedBefore = pool.getHeapPoolStatistics().returned();
        PooledByteBuffer pooled = pool.borrowHeap(64 * 1024);

        Thread closer = new Thread(pooled::close, "heap-handle-closer");
        closer.start();
        closer.join();

        assertThat(pool.getHeapPoolStatistics().returned())
                .as("a close on another thread returns the buffer to the free list")
                .isGreaterThan(returnedBefore);
    }

    @Test
    void aHeapBorrowAboveTheRetentionQuotaIsServedAndReleasedOnClose() {
        int size = Math.toIntExact(ParquetryByteBufferPool.HEAP_RETAINED_BYTES + 1);
        long discardedBefore = pool.getHeapPoolStatistics().discarded();

        try (PooledByteBuffer pooled = pool.borrowHeap(size)) {
            ByteBuffer buffer = pooled.buffer();
            assertThat(buffer.capacity())
                    .as("an over-quota borrow is still served")
                    .isEqualTo(size);
            buffer.put(size - 1, (byte) 1);
        }

        PoolStatistics stats = pool.getHeapPoolStatistics();
        assertThat(stats.discarded())
                .as("the over-quota buffer is released, not retained")
                .isGreaterThan(discardedBefore);
        assertThat(stats.bytesSize())
                .as("bytes retained in the free list")
                .isLessThanOrEqualTo(ParquetryByteBufferPool.HEAP_RETAINED_BYTES);
    }

    @Test
    void clearReleasesTheRetainedHeapBuffers() {
        pool.borrowHeap(64 * 1024).close();
        assertThat(pool.getHeapPoolStatistics().poolSize())
                .as("free list before clear")
                .isPositive();

        pool.clear();

        assertThat(pool.getHeapPoolStatistics().poolSize())
                .as("free list after clear")
                .isZero();
    }

    @Test
    void directStatisticsReportTheSegmentPoolRetention() {
        pool.borrowDirect(64 * 1024).close();

        PoolStatistics stats = pool.getDirectPoolStatistics();
        assertThat(stats.returned())
                .as("direct borrows returned to the segment pool")
                .isPositive();
        assertThat(stats.bytesSize()).as("bytes retained by the segment pool").isNotNegative();
    }

    @Test
    void manySequentialDirectBorrowsAllReturnUsableBuffers() {
        int size = 1 << 20;
        for (int i = 0; i < 256; i++) {
            try (PooledByteBuffer pooled = pool.borrowDirect(size)) {
                ByteBuffer buffer = pooled.buffer();
                assertThat(buffer.capacity()).as("borrow %d capacity", i).isEqualTo(size);
                buffer.put(0, (byte) i);
                assertThat(buffer.get(0)).as("borrow %d byte", i).isEqualTo((byte) i);
            }
        }
    }

    @Test
    void serviceLoaderDiscoversTheProvider() {
        ServiceLoader<ByteBufferPool> loader = ServiceLoader.load(ByteBufferPool.class);
        boolean found = loader.stream().anyMatch(provider -> provider.type() == ParquetryByteBufferPool.class);
        assertThat(found)
                .as("ServiceLoader must discover ParquetryByteBufferPool via META-INF/services")
                .isTrue();
    }

    @Test
    void getDefaultResolvesToThisProviderAndServesHeapBorrows() {
        ByteBufferPool shared = ByteBufferPool.getDefault();
        assertThat(shared)
                .as("getDefault() must win as the registered provider that vends parquetry's shared pool to tileverse")
                .isInstanceOf(ParquetryByteBufferPool.class);

        // A heap borrow through the discovered provider builds the heap pool outside the built-in pool's class
        // initialization, which is what the lazy holder exists for.
        try (PooledByteBuffer pooled = ByteBufferPool.heapBuffer(8192)) {
            assertThat(pooled.buffer().capacity()).isEqualTo(8192);
        }
    }
}
