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
package io.tileverse.parquetry.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SegmentPoolTest {

    @Test
    void getDefaultIsASingleton() {
        assertThat(SegmentPool.getDefault()).isSameAs(SegmentPool.getDefault());
    }

    @Test
    void borrowReturnsSegmentOfExactSize() {
        try (SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(1000)) {
            assertThat(pooled.segment().byteSize()).isEqualTo(1000);
        }
    }

    @Test
    void borrowZeroReturnsEmptySegment() {
        try (SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(0)) {
            assertThat(pooled.segment().byteSize()).isZero();
        }
    }

    @Test
    void negativeBorrowThrows() {
        SegmentPool pool = SegmentPool.getDefault();
        assertThatThrownBy(() -> pool.borrow(-1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void borrowedSegmentIsWritableAndReadable() {
        try (SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(8)) {
            MemorySegment segment = pooled.segment();
            segment.set(ValueLayout.JAVA_LONG, 0, 0x0102030405060708L);
            assertThat(segment.get(ValueLayout.JAVA_LONG, 0)).isEqualTo(0x0102030405060708L);
        }
    }

    @Test
    void outstandingBorrowsAreDistinct() {
        SegmentPool pool = SegmentPool.getDefault();
        try (SegmentPool.Pooled a = pool.borrow(64);
                SegmentPool.Pooled b = pool.borrow(64)) {
            assertThat(a.segment().address()).isNotEqualTo(b.segment().address());
        }
    }

    @Test
    void closeIsIdempotent() {
        SegmentPool.Pooled pooled = SegmentPool.getDefault().borrow(16);
        pooled.close();
        assertThatCode(pooled::close).doesNotThrowAnyException();
    }

    @Test
    void createReturnsIndependentPools() {
        SegmentPool a = SegmentPool.create();
        SegmentPool b = SegmentPool.create();
        assertThat(a).isNotSameAs(b);
        try (SegmentPool.Pooled _ = a.borrow(64)) {
            assertThat(a.stats().outstandingBorrows()).isEqualTo(1);
            assertThat(b.stats().outstandingBorrows()).isZero();
        }
    }

    @Test
    void statsTrackTheBorrowLifecycle() {
        SegmentPool pool = SegmentPool.create();
        assertThat(pool.stats()).isEqualTo(new SegmentPool.PoolStats(0, 0, 0, 0));

        SegmentPool.Pooled pooled = pool.borrow(64);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(1);
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);

        pooled.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);

        pooled.close();
        assertThat(pool.stats().outstandingBorrows())
                .as("an idempotent re-close must not decrement twice")
                .isZero();
    }

    @Test
    void statsCountLargeUnpooledBorrows() {
        DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1024);
        SegmentPool.Pooled large = pool.borrow(8192);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(1);
        large.close();
        large.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);
        assertThat(pool.stats().freeSegments()).isZero();
    }

    @Test
    void concurrentBorrowAndReturnStayConsistent() throws Exception {
        long maxPooledBytes = 8L * 8192;
        DefaultSegmentPool pool = new DefaultSegmentPool(1 << 20, maxPooledBytes, 8192);
        AtomicBoolean failed = new AtomicBoolean(false);
        try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
            for (int t = 0; t < 8; t++) {
                executor.execute(() -> {
                    for (int i = 0; i < 2000; i++) {
                        try (SegmentPool.Pooled pooled = pool.borrow(256)) {
                            MemorySegment segment = pooled.segment();
                            segment.set(ValueLayout.JAVA_INT, 0, i);
                            if (segment.get(ValueLayout.JAVA_INT, 0) != i) {
                                failed.set(true);
                            }
                        } catch (RuntimeException _) {
                            failed.set(true);
                        }
                    }
                });
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(60, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(failed).isFalse();
        assertThat(pool.stats().retainedBytes()).isLessThanOrEqualTo(maxPooledBytes);
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(8L * 2000);
    }

    /**
     * Retention policy: small buffers are pooled and reused under a byte cap; large buffers are never retained and are
     * freed deterministically when the borrower closes, instead of ratcheting native memory for the JVM lifetime.
     */
    @Nested
    class Retention {

        @Test
        void smallBuffersArePooledAndReused() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1024);
            SegmentPool.Pooled first = pool.borrow(512);
            long address = first.segment().address();
            first.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            try (SegmentPool.Pooled second = pool.borrow(512)) {
                assertThat(second.segment().address()).as("reused backing").isEqualTo(address);
                assertThat(pool.stats().freeSegments()).isZero();
            }
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
        }

        @Test
        void pooledRetentionIsBoundedByBytes() {
            long maxPooledBytes = 4096;
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, maxPooledBytes, 1024);
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                held.add(pool.borrow(512));
            }
            for (SegmentPool.Pooled pooled : held) {
                pooled.close();
            }
            assertThat(pool.stats().retainedBytes()).as("retained bytes").isLessThanOrEqualTo(maxPooledBytes);
            assertThat(pool.stats().freeSegments())
                    .as("1024-byte backings retained under a 4096 cap")
                    .isEqualTo(4);
        }

        @Test
        void largeBuffersAreNeverPooled() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1024);
            pool.borrow(8192).close();
            assertThat(pool.stats().freeSegments()).isZero();
            assertThat(pool.stats().retainedBytes()).isZero();
        }

        @Test
        void repeatedLargeBorrowsDoNotRatchet() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1024);
            for (int i = 0; i < 100; i++) {
                pool.borrow(64 * 1024).close();
                assertThat(pool.stats().freeSegments())
                        .as("retained after large borrow %d", i)
                        .isZero();
            }
            assertThat(pool.stats().retainedBytes()).isZero();
        }

        @Test
        void largeBufferIsFreedOnClose() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(8192);
            MemorySegment segment = pooled.segment();
            pooled.close();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0)).isInstanceOf(IllegalStateException.class);
        }

        @Test
        void largeBufferCloseIsIdempotent() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(8192);
            pooled.close();
            assertThatCode(pooled::close).doesNotThrowAnyException();
        }
    }
}
