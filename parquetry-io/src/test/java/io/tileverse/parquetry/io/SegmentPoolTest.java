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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.ref.WeakReference;
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
    void optionsRejectInvalidSizes() {
        assertThatThrownBy(() -> new SegmentPool.Options(0, 1024, 1024, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, -1, 1024, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, -1, 0, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, 1024, -1, 8192))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new SegmentPool.Options(1024, 1024, 1024, 0, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void elasticOptionsScaleTheRetentionCapWithinBounds() {
        SegmentPool.Options elastic = SegmentPool.Options.elastic();
        assertThat(elastic.largeBufferThreshold()).isEqualTo(4L << 20);
        assertThat(elastic.maxPooledBytes()).isBetween(16L << 20, 512L << 20);
        assertThat(elastic.maxLargePooledBytes()).isBetween(64L << 20, 1024L << 20);
        assertThat(elastic.idleRetentionNanos()).isEqualTo(TimeUnit.SECONDS.toNanos(60));
        assertThat(elastic.blockSize()).isEqualTo(8192);
    }

    @Test
    void megabyteBuffersArePooledUnderTheElasticThreshold() {
        SegmentPool pool = SegmentPool.create();
        SegmentPool.Pooled first = pool.borrow(1L << 20);
        long address = first.segment().address();
        first.close();
        assertThat(pool.stats().freeSegments()).isEqualTo(1);
        try (SegmentPool.Pooled second = pool.borrow(1L << 20)) {
            assertThat(second.segment().address()).as("reused backing").isEqualTo(address);
        }
    }

    @Test
    void createHonorsCustomOptions() {
        SegmentPool pool = SegmentPool.create(new SegmentPool.Options(512 * 1024, 1L << 20, 1L << 20, 0, 1024));
        pool.borrow(600 * 1024).close();
        assertThat(pool.stats().freeSegments())
                .as("above the threshold: retained in the large class")
                .isEqualTo(1);
        pool.borrow(400 * 1024).close();
        assertThat(pool.stats().freeSegments())
                .as("below the threshold: retained in the small class")
                .isEqualTo(2);
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
    void statsCountLargeBorrows() {
        DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1 << 20, 1024);
        SegmentPool.Pooled large = pool.borrow(8192);
        assertThat(pool.stats().outstandingBorrows()).isEqualTo(1);
        large.close();
        large.close();
        assertThat(pool.stats().outstandingBorrows()).isZero();
        assertThat(pool.stats().totalBorrows()).isEqualTo(1);
        assertThat(pool.stats().freeSegments())
                .as("the large buffer is retained for reuse")
                .isEqualTo(1);
    }

    @Test
    void concurrentBorrowAndReturnStayConsistent() throws Exception {
        long maxPooledBytes = 8L * 8192;
        DefaultSegmentPool pool = new DefaultSegmentPool(1 << 20, maxPooledBytes, 1 << 20, 8192);
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
     * Retention policy: each size class reuses returned buffers under its own byte cap; a return that would breach a
     * class's cap has its arena closed at once, freeing the native memory deterministically rather than parking it.
     */
    @Nested
    class Retention {

        @Test
        void smallBuffersArePooledAndReused() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
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
        void largeBuffersArePooledAndReused() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled first = pool.borrow(8192);
            long address = first.segment().address();
            first.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            try (SegmentPool.Pooled second = pool.borrow(8192)) {
                assertThat(second.segment().address())
                        .as("reused large backing")
                        .isEqualTo(address);
                assertThat(pool.stats().freeSegments()).isZero();
            }
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
        }

        @Test
        void pooledRetentionIsBoundedByBytes() {
            long maxPooledBytes = 4096;
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, maxPooledBytes, 1 << 20, 1024);
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
        void largeRetentionIsBoundedByTheLargeCap() {
            long maxLargePooledBytes = 4L * 8192;
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, maxLargePooledBytes, 8192);
            List<SegmentPool.Pooled> held = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                held.add(pool.borrow(8192));
            }
            for (SegmentPool.Pooled pooled : held) {
                pooled.close();
            }
            assertThat(pool.stats().retainedBytes())
                    .as("large retained bytes")
                    .isLessThanOrEqualTo(maxLargePooledBytes);
            assertThat(pool.stats().freeSegments())
                    .as("8192-byte large backings retained under a 32768 cap")
                    .isEqualTo(4);
        }

        @Test
        void smallAndLargeClassesRetainIndependently() {
            DefaultSegmentPool pool = new DefaultSegmentPool(4096, 1 << 20, 1 << 20, 1024);
            pool.borrow(2048).close();
            pool.borrow(16384).close();
            assertThat(pool.stats().freeSegments())
                    .as("one small + one large backing, each in its own class")
                    .isEqualTo(2);
        }

        @Test
        void evictedPooledBackingIsFreedDeterministically() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1024, 1024, 1024);
            SegmentPool.Pooled retained = pool.borrow(512);
            SegmentPool.Pooled evicted = pool.borrow(512);
            MemorySegment evictedSegment = evicted.segment();
            retained.close();
            evicted.close();
            assertThat(pool.stats().freeSegments())
                    .as("one 1024-byte backing fills the cap; the second return is evicted")
                    .isEqualTo(1);
            assertThatThrownBy(() -> evictedSegment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("an evicted pooled backing frees its arena instead of waiting for GC")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void closeFreesRetainedBackings() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(512);
            MemorySegment segment = pooled.segment();
            pooled.close();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);
            pool.close();
            assertThat(pool.stats().freeSegments()).isZero();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("pool close frees the retained backings")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void aBufferIsFreedWhenItsClassCannotRetainIt() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 0, 1024);
            SegmentPool.Pooled pooled = pool.borrow(8192);
            MemorySegment segment = pooled.segment();
            pooled.close();
            assertThat(pool.stats().freeSegments()).isZero();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("a large class with no retention room frees the buffer on return")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void closeIsIdempotentForARetainedBuffer() {
            DefaultSegmentPool pool = new DefaultSegmentPool(1024, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(8192);
            pooled.close();
            assertThatCode(pooled::close).doesNotThrowAnyException();
        }

        @Test
        void reapIdleFreesBackingsPastTheIdleWindow() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            SegmentPool.Pooled pooled = pool.borrow(512);
            MemorySegment segment = pooled.segment();
            pooled.close();
            long retainedAtNanos = System.nanoTime();
            assertThat(pool.stats().freeSegments()).isEqualTo(1);

            long window = TimeUnit.SECONDS.toNanos(60);
            pool.reapIdle(retainedAtNanos, window);
            assertThat(pool.stats().freeSegments())
                    .as("kept while still within the idle window")
                    .isEqualTo(1);

            pool.reapIdle(retainedAtNanos + window + 1, window);
            assertThat(pool.stats().freeSegments())
                    .as("freed once idle past the window")
                    .isZero();
            assertThatThrownBy(() -> segment.get(ValueLayout.JAVA_BYTE, 0))
                    .as("a reaped backing's arena is closed")
                    .isInstanceOf(IllegalStateException.class);
        }

        @Test
        void reapIdleWithZeroThresholdKeepsBackings() {
            DefaultSegmentPool pool = new DefaultSegmentPool(8192, 1 << 20, 1 << 20, 1024);
            pool.borrow(512).close();
            pool.reapIdle(System.nanoTime() + TimeUnit.SECONDS.toNanos(3600), 0);
            assertThat(pool.stats().freeSegments())
                    .as("a zero threshold disables reaping")
                    .isEqualTo(1);
        }

        @Test
        void abandonedPoolWithRunningReaperStaysCollectible() {
            WeakReference<SegmentPool> ref = poolWithRunningReaper();
            assertThat(awaitCollected(ref))
                    .as(
                            "a pool dropped without close() stays collectible because the reaper holds only a weak reference")
                    .isTrue();
        }

        private static WeakReference<SegmentPool> poolWithRunningReaper() {
            SegmentPool pool = SegmentPool.create(
                    new SegmentPool.Options(8192, 1 << 20, 1 << 20, TimeUnit.SECONDS.toNanos(60), 1024));
            pool.borrow(512).close();
            return new WeakReference<>(pool);
        }

        private static boolean awaitCollected(WeakReference<?> ref) {
            for (int attempt = 0; attempt < 50 && ref.get() != null; attempt++) {
                System.gc();
            }
            return ref.get() == null;
        }
    }
}
