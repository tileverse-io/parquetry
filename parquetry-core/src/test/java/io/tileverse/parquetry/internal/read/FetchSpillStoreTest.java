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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.runtime.DiskBudget;

class FetchSpillStoreTest {

    @Test
    void mappedBufferRoundTripsBytesThenFreesAndDeletesOnClose(@TempDir Path dir) {
        DiskBudget budget = DiskBudget.ofBytes(1 << 20);
        FetchSpillStore store = new FetchSpillStore(dir, budget);
        long before = budget.available();

        SegmentPool.Pooled mapped = store.map(4096);
        MemorySegment segment = mapped.segment();
        assertThat(segment.byteSize()).isEqualTo(4096);
        segment.set(ValueLayout.JAVA_LONG, 0, 0x0102030405060708L);
        assertThat(segment.get(ValueLayout.JAVA_LONG, 0)).isEqualTo(0x0102030405060708L);
        assertThat(budget.available()).as("disk reserved while mapped").isLessThan(before);

        mapped.close();
        assertThat(budget.available()).as("disk released on close").isEqualTo(before);
        assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void closeIsIdempotent(@TempDir Path dir) {
        DiskBudget budget = DiskBudget.ofBytes(1 << 20);
        FetchSpillStore store = new FetchSpillStore(dir, budget);
        long before = budget.available();
        SegmentPool.Pooled mapped = store.map(64);
        mapped.close();
        mapped.close();
        assertThat(budget.available()).as("disk released exactly once").isEqualTo(before);
    }

    @Test
    void sweepOrphansRemovesOtherPidSpillDirs(@TempDir Path dir) throws Exception {
        long deadPid = aDeadPid();
        Path orphan = Files.createDirectories(dir.resolve("parquetry-fetch-spill-" + deadPid));
        Files.writeString(orphan.resolve("0_0.spill"), "x");
        FetchSpillStore.sweepOrphans(dir);
        assertThat(Files.exists(orphan)).isFalse();
    }

    private static long aDeadPid() {
        long candidate = 999_999L;
        while (ProcessHandle.of(candidate).isPresent()) {
            candidate++;
        }
        return candidate;
    }

    @Test
    void sweepOrphansKeepsCurrentPidSpillDir(@TempDir Path dir) throws Exception {
        long pid = ProcessHandle.current().pid();
        Path live = Files.createDirectories(dir.resolve("parquetry-fetch-spill-" + pid));
        FetchSpillStore.sweepOrphans(dir);
        assertThat(Files.exists(live)).isTrue();
    }
}
