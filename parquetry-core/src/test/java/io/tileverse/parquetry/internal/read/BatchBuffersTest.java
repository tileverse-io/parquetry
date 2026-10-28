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

import static io.tileverse.parquetry.internal.read.BatchBuffers.MAX_CARVED_BYTES;
import static io.tileverse.parquetry.internal.read.BatchBuffers.SLAB_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.BudgetExhaustedException;
import io.tileverse.parquetry.runtime.DecodeBudget;
import io.tileverse.parquetry.runtime.DiskBudget;

class BatchBuffersTest {

    @Test
    void carvesSmallBuffersFromOneSlab() {
        SegmentPool pool = SegmentPool.create();
        try (BatchBuffers buffers = new BatchBuffers(TestDecodeBuffers.ample(pool))) {
            MemorySegment first = buffers.allocate(100);
            MemorySegment second = buffers.allocate(3);
            MemorySegment third = buffers.allocate(1000);

            assertThat(first.byteSize()).isEqualTo(100);
            assertThat(second.byteSize()).isEqualTo(3);
            assertThat(third.byteSize()).isEqualTo(1000);
            assertThat(second.address()).isEqualTo(first.address() + 104);
            assertThat(third.address()).isEqualTo(second.address() + 8);
            assertThat(third.address() % 8).as("eight-byte aligned").isZero();
            assertThat(pool.stats().totalBorrows()).as("one slab").isEqualTo(1);
        }
    }

    @Test
    void zeroBytesBorrowNothing() {
        SegmentPool pool = SegmentPool.create();
        try (BatchBuffers buffers = new BatchBuffers(TestDecodeBuffers.ample(pool))) {
            MemorySegment empty = buffers.allocate(0);

            assertThat(empty.byteSize()).isZero();
            assertThat(empty.isNative()).isTrue();
            assertThat(pool.stats().totalBorrows()).isZero();
        }
    }

    @Test
    void largeBufferIsBorrowedAlone() {
        SegmentPool pool = SegmentPool.create();
        try (BatchBuffers buffers = new BatchBuffers(TestDecodeBuffers.ample(pool))) {
            MemorySegment small = buffers.allocate(16);
            MemorySegment large = buffers.allocate(MAX_CARVED_BYTES + 1);
            MemorySegment next = buffers.allocate(16);

            assertThat(large.byteSize()).isEqualTo(MAX_CARVED_BYTES + 1);
            assertThat(next.address())
                    .as("the slab keeps serving small buffers")
                    .isEqualTo(small.address() + 16);
            assertThat(pool.stats().totalBorrows()).isEqualTo(2);
        }
    }

    @Test
    void fullSlabStartsAnother() {
        SegmentPool pool = SegmentPool.create();
        try (BatchBuffers buffers = new BatchBuffers(TestDecodeBuffers.ample(pool))) {
            for (long carved = 0; carved < SLAB_BYTES; carved += MAX_CARVED_BYTES) {
                buffers.allocate(MAX_CARVED_BYTES);
            }
            assertThat(pool.stats().totalBorrows())
                    .as("four buffers fill the slab")
                    .isEqualTo(1);

            buffers.allocate(1);
            assertThat(pool.stats().totalBorrows()).isEqualTo(2);
        }
    }

    @Test
    void closeReturnsEveryBuffer() {
        SegmentPool pool = SegmentPool.create();
        AtomicInteger resourceCloses = new AtomicInteger();
        BatchBuffers buffers = new BatchBuffers(TestDecodeBuffers.ample(pool));
        buffers.allocate(64);
        buffers.allocate(MAX_CARVED_BYTES + 1);
        buffers.add(resourceCloses::incrementAndGet);

        buffers.close();
        buffers.close();

        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(resourceCloses).as("closed once").hasValue(1);
    }

    @Test
    void failedBorrowKeepsSlabUsable(@TempDir Path spillDir) {
        SegmentPool pool = SegmentPool.create();
        DecodeBudget budget = DecodeBudget.ofBytes(SLAB_BYTES);
        DecodeBufferAllocator allocator =
                new DecodeBufferAllocator(pool, budget, new FetchSpillStore(spillDir, DiskBudget.ofBytes(1)));
        try (BatchBuffers buffers = new BatchBuffers(allocator)) {
            MemorySegment first = buffers.allocate(8);

            assertThatThrownBy(() -> buffers.allocate(MAX_CARVED_BYTES + 1))
                    .isInstanceOf(BudgetExhaustedException.class);
            MemorySegment second = buffers.allocate(8);

            assertThat(second.address()).isEqualTo(first.address() + 8);
        }
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(budget.available()).isEqualTo(SLAB_BYTES);
    }
}
