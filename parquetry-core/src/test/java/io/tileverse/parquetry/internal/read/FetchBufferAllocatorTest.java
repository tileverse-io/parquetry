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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.BudgetExhaustedException;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.runtime.FetchBudget;

class FetchBufferAllocatorTest {

    @Test
    void usesRamWhenTheFetchBudgetAllows(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(1 << 20);
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ZERO);
        long fetchBefore = fetch.available();
        long diskBefore = disk.available();
        try (SegmentPool.Pooled buffer = allocator.acquireMandatory(4096)) {
            assertThat(buffer.segment().byteSize()).isEqualTo(4096);
            assertThat(fetch.available()).as("RAM reserved").isLessThan(fetchBefore);
            assertThat(disk.available()).as("disk untouched").isEqualTo(diskBefore);
        }
        assertThat(fetch.available()).as("RAM released on close").isEqualTo(fetchBefore);
    }

    @Test
    void releasesRamBudgetOnceOnDoubleClose(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(1 << 20);
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ZERO);
        long fetchBefore = fetch.available();
        SegmentPool.Pooled buffer = allocator.acquireMandatory(4096);
        buffer.close();
        buffer.close();
        assertThat(fetch.available()).as("fetch budget released exactly once").isEqualTo(fetchBefore);
    }

    @Test
    void spillsToMmapWhenTheFetchBudgetIsExhausted(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(1024); // too small for the request
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ZERO);
        long diskBefore = disk.available();
        try (SegmentPool.Pooled buffer = allocator.acquireMandatory(64 * 1024)) {
            assertThat(buffer.segment().byteSize()).isEqualTo(64 * 1024);
            assertThat(disk.available()).as("spilled to disk").isLessThan(diskBefore);
        }
        assertThat(disk.available()).as("disk released on close").isEqualTo(diskBefore);
    }

    @Test
    @Timeout(5)
    void waitsThenSpills(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(64 * 1024);
        fetch.tryReserve(64 * 1024);
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ofMillis(200));
        long start = System.nanoTime();
        try (SegmentPool.Pooled buffer = allocator.acquireMandatory(64 * 1024)) {
            assertThat(buffer.segment().byteSize()).isEqualTo(64 * 1024);
            assertThat(System.nanoTime() - start).isGreaterThan(TimeUnit.MILLISECONDS.toNanos(150));
            assertThat(disk.available()).as("spilled after the wait expired").isLessThan(disk.capacity());
        }
        assertThat(fetch.waitStats().expired()).isEqualTo(1);
    }

    @Test
    @Timeout(5)
    void getsRamWhenRoomFrees(@TempDir Path dir) throws InterruptedException {
        FetchBudget fetch = FetchBudget.ofBytes(64 * 1024);
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ofSeconds(2));
        SegmentPool.Pooled held = allocator.acquireMandatory(64 * 1024);
        Thread caller = Thread.currentThread();
        Thread releaser = new Thread(() -> {
            awaitParked(caller);
            held.close();
        });
        releaser.start();
        try (SegmentPool.Pooled buffer = allocator.acquireMandatory(64 * 1024)) {
            assertThat(buffer.segment().byteSize()).isEqualTo(64 * 1024);
            assertThat(disk.available()).as("RAM, not disk").isEqualTo(disk.capacity());
        }
        releaser.join();
        FetchBudget.WaitStats stats = fetch.waitStats();
        assertThat(stats.waits()).isEqualTo(1);
        assertThat(stats.expired()).isZero();
    }

    @Test
    @Timeout(5)
    void aboveCapacitySpillsAtOnce(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(1024);
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ofSeconds(5));
        long start = System.nanoTime();
        try (SegmentPool.Pooled buffer = allocator.acquireMandatory(64 * 1024)) {
            assertThat(buffer.segment().byteSize()).isEqualTo(64 * 1024);
            assertThat(System.nanoTime() - start).isLessThan(TimeUnit.SECONDS.toNanos(1));
            assertThat(disk.available()).isLessThan(disk.capacity());
        }
    }

    @Test
    void throwsWhenDiskIsGoneToo(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(1024);
        DiskBudget disk = DiskBudget.ofBytes(1);
        FetchBufferAllocator allocator = new FetchBufferAllocator(
                SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk), Duration.ZERO);
        assertThatThrownBy(() -> allocator.acquireMandatory(64 * 1024))
                .isInstanceOf(BudgetExhaustedException.class)
                .hasMessageContaining("65536");
    }

    /** Spins until {@code thread} parks inside its timed wait for budget room, or fails after two seconds. */
    private static void awaitParked(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.getState() != Thread.State.TIMED_WAITING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the caller never parked on the full fetch budget");
            }
            Thread.onSpinWait();
        }
    }
}
