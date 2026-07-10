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

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.DecodeBudget;
import io.tileverse.parquetry.runtime.DiskBudget;

class DecodeBufferAllocatorTest {

    @Test
    void servesFromRamWhileBudgetHasRoom(@TempDir Path spillDir) {
        DecodeBudget budget = DecodeBudget.ofBytes(1_000_000L);
        DiskBudget diskBudget = DiskBudget.ofBytes(1_000_000L);
        FetchSpillStore spillStore = new FetchSpillStore(spillDir, diskBudget);
        DecodeBufferAllocator allocator = new DecodeBufferAllocator(SegmentPool.getDefault(), budget, spillStore);
        try (SegmentPool.Pooled pooled = allocator.acquireMandatory(64_000L)) {
            MemorySegment segment = pooled.segment();
            assertThat(segment.byteSize()).as("served the requested size").isGreaterThanOrEqualTo(64_000L);
            assertThat(budget.available())
                    .as("RAM path reserved against the decode budget")
                    .isLessThan(1_000_000L);
        }
        assertThat(budget.available()).as("close returns the reservation").isEqualTo(1_000_000L);
    }

    @Test
    void fallsBackToMmapWhenBudgetIsExhausted(@TempDir Path spillDir) {
        DecodeBudget budget = DecodeBudget.ofBytes(1_000L); // smaller than the request
        DiskBudget diskBudget = DiskBudget.ofBytes(10_000_000L);
        FetchSpillStore spillStore = new FetchSpillStore(spillDir, diskBudget);
        DecodeBufferAllocator allocator = new DecodeBufferAllocator(SegmentPool.getDefault(), budget, spillStore);
        try (SegmentPool.Pooled pooled = allocator.acquireMandatory(64_000L)) {
            assertThat(pooled.segment().byteSize())
                    .as("mmap path served the request")
                    .isEqualTo(64_000L);
            assertThat(budget.available())
                    .as("RAM budget untouched on the mmap path")
                    .isEqualTo(1_000L);
        }
    }
}
