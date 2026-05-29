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
package io.tileverse.parquetry.tileverse;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.io.SegmentPool;

import io.tileverse.io.ByteBufferPool;

class ByteBufferPoolSegmentPoolTest {

    @Test
    void borrowYieldsSegmentOfRequestedSize() {
        SegmentPool pool = SegmentPools.backedBy(new ByteBufferPool());
        try (SegmentPool.Pooled pooled = pool.borrow(4096)) {
            assertThat(pooled.segment().byteSize()).isEqualTo(4096);
        }
    }

    @Test
    void closeReturnsBufferToTheBackingPool() {
        ByteBufferPool backing = new ByteBufferPool();
        SegmentPool pool = SegmentPools.backedBy(backing);
        pool.borrow(4096).close();
        pool.borrow(4096).close();
        assertThat(backing.getDirectPoolStatistics().reused()).isPositive();
    }
}
