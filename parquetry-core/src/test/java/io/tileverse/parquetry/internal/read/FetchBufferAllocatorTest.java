/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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

import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.DiskBudget;
import io.tileverse.parquetry.runtime.FetchBudget;

class FetchBufferAllocatorTest {

    @Test
    void usesRamWhenTheFetchBudgetAllows(@TempDir Path dir) {
        FetchBudget fetch = FetchBudget.ofBytes(1 << 20);
        DiskBudget disk = DiskBudget.ofBytes(1 << 20);
        FetchBufferAllocator allocator =
                new FetchBufferAllocator(SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk));
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
        FetchBufferAllocator allocator =
                new FetchBufferAllocator(SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk));
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
        FetchBufferAllocator allocator =
                new FetchBufferAllocator(SegmentPool.getDefault(), fetch, new FetchSpillStore(dir, disk));
        long diskBefore = disk.available();
        try (SegmentPool.Pooled buffer = allocator.acquireMandatory(64 * 1024)) {
            assertThat(buffer.segment().byteSize()).isEqualTo(64 * 1024);
            assertThat(disk.available()).as("spilled to disk").isLessThan(diskBefore);
        }
        assertThat(disk.available()).as("disk released on close").isEqualTo(diskBefore);
    }
}
