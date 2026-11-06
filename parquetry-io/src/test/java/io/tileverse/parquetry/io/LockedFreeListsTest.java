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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.DefaultSegmentPool.Backing;

class LockedFreeListsTest {

    private final Arena arena = Arena.ofShared();

    private final LockedFreeLists lists = new LockedFreeLists(Long.MAX_VALUE);

    @AfterEach
    void closeArena() {
        arena.close();
    }

    // The large class sees unbounded distinct capacities: an emptied group must not stay behind
    @Test
    void takeRemovesEmptiedGroups() {
        putBackings(1024, 2048, 4096);
        assertThat(lists.take(1024)).isNotNull();
        assertThat(lists.take(1024)).isNotNull();
        assertThat(lists.take(1024)).isNotNull();
        assertThat(lists.groupCount()).isZero();
        assertThat(lists.take(1024)).isNull();
    }

    @Test
    void reapRemovesEmptiedGroups() {
        putBackings(1024, 2048);
        assertThat(lists.reapStale(System.nanoTime() + 1, 1)).hasSize(2);
        assertThat(lists.groupCount()).isZero();
    }

    @Test
    void drainRemovesGroups() {
        putBackings(1024, 2048);
        assertThat(lists.drain()).hasSize(2);
        assertThat(lists.groupCount()).isZero();
    }

    private void putBackings(long... capacities) {
        for (long capacity : capacities) {
            assertThat(lists.put(new Backing(arena, arena.allocate(capacity)))).isTrue();
        }
        assertThat(lists.groupCount()).isEqualTo(capacities.length);
    }
}
