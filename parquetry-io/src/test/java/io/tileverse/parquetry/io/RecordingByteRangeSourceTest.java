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

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;

class RecordingByteRangeSourceTest {

    /** Fixed-content in-memory source; short-reads past its end like a real file source. */
    private static ByteRangeSource sourceOf(int size) {
        MemorySegment data = MemorySegment.ofArray(new byte[size]);
        return new ByteRangeSource() {
            @Override
            public long size() {
                return size;
            }

            @Override
            public int read(long offset, MemorySegment dst) {
                if (offset >= size) {
                    return -1;
                }
                int toCopy = (int) Math.min(dst.byteSize(), size - offset);
                MemorySegment.copy(data, offset, dst, 0, toCopy);
                return toCopy;
            }

            @Override
            public void close() {}
        };
    }

    @Test
    void recordsEveryReadWithReturnedLengthNotRequestedLength() {
        try (RecordingByteRangeSource recording = new RecordingByteRangeSource(sourceOf(100))) {
            recording.read(0, MemorySegment.ofArray(new byte[40]));
            recording.read(90, MemorySegment.ofArray(new byte[40])); // short read: only 10 available
            assertThat(recording.ranges())
                    .containsExactly(
                            new RecordingByteRangeSource.Range(0, 40), new RecordingByteRangeSource.Range(90, 10));
            assertThat(recording.requestCount()).isEqualTo(2);
            assertThat(recording.bytesRead()).isEqualTo(50);
            assertThat(recording.maxReadEnd()).isEqualTo(100);
        }
    }

    @Test
    void eofProbePastTheEndDoesNotRaiseMaxReadEnd() {
        try (RecordingByteRangeSource recording = new RecordingByteRangeSource(sourceOf(100))) {
            recording.read(0, MemorySegment.ofArray(new byte[40]));
            assertThat(recording.read(500, MemorySegment.ofArray(new byte[40]))).isEqualTo(-1);
            assertThat(recording.ranges())
                    .containsExactly(
                            new RecordingByteRangeSource.Range(0, 40), new RecordingByteRangeSource.Range(500, 0));
            assertThat(recording.maxReadEnd()).isEqualTo(40);
            assertThat(recording.bytesRead()).isEqualTo(40);
        }
    }

    @Test
    void bytesInRangeSumsOnlyTheOverlap() {
        try (RecordingByteRangeSource recording = new RecordingByteRangeSource(sourceOf(100))) {
            recording.read(10, MemorySegment.ofArray(new byte[20])); // [10, 30)
            recording.read(50, MemorySegment.ofArray(new byte[10])); // [50, 60)
            assertThat(recording.bytesInRange(0, 100)).isEqualTo(30);
            assertThat(recording.bytesInRange(20, 55)).isEqualTo(15); // 10 from the first + 5 from the second
            assertThat(recording.bytesInRange(60, 100)).isZero();
        }
    }

    @Test
    void toleratesConcurrentAppendsAndReads() throws Exception {
        int writers = 8;
        int readsPerWriter = 4_000;
        int readers = 4;
        try (RecordingByteRangeSource recording = new RecordingByteRangeSource(sourceOf(1_000))) {
            AtomicBoolean appending = new AtomicBoolean(true);
            CountDownLatch readersLooping = new CountDownLatch(readers);
            CountDownLatch growthSeen = new CountDownLatch(1);
            try (ExecutorService pool = Executors.newFixedThreadPool(writers + readers)) {
                List<Future<Integer>> observeTasks = new ArrayList<>();
                for (int t = 0; t < readers; t++) {
                    observeTasks.add(
                            pool.submit(() -> observeWhileAppending(recording, appending, readersLooping, growthSeen)));
                }
                List<Future<?>> appendTasks = new ArrayList<>();
                for (int t = 0; t < writers; t++) {
                    appendTasks.add(pool.submit(() -> {
                        awaitReadersOrFail(readersLooping);
                        for (int i = 0; i < readsPerWriter; i++) {
                            if (i == readsPerWriter / 2) {
                                awaitGrowthSeenOrFail(growthSeen);
                            }
                            recording.read(0, MemorySegment.ofArray(new byte[1]));
                        }
                        return null;
                    }));
                }
                for (Future<?> appendTask : appendTasks) {
                    appendTask.get();
                }
                appending.set(false);
                int growthObservations = 0;
                for (Future<Integer> observeTask : observeTasks) {
                    growthObservations += observeTask.get();
                }
                // A reader that never saw the list grow never overlapped a writer, leaving nothing under test.
                assertThat(growthObservations).isPositive();
            }
            assertThat(recording.requestCount()).isEqualTo(writers * readsPerWriter);
            assertThat(recording.bytesRead()).isEqualTo(writers * readsPerWriter);
        }
    }

    /**
     * Holds the writers back until every reader is inside its snapshot loop; appends that land before the readers get
     * that far leave the concurrent walk untested. Bounded to keep a failing reader from hanging the test.
     */
    private static void awaitReadersOrFail(CountDownLatch readersLooping) throws InterruptedException {
        boolean allLooping = readersLooping.await(30, TimeUnit.SECONDS);
        assertThat(allLooping).as("readers reached their snapshot loop").isTrue();
    }

    /**
     * Holds every writer at the halfway point until some reader has observed the recorded list grow. Without this
     * handshake a starved scheduler (small CI runners) can run the whole append phase between two reader iterations:
     * every reader then sees only the empty list and the final assertion reports a vacuous run. Half the load is
     * already appended when writers arrive here, meaning one scheduled reader iteration is enough to release them.
     */
    private static void awaitGrowthSeenOrFail(CountDownLatch growthSeen) throws InterruptedException {
        boolean seen = growthSeen.await(30, TimeUnit.SECONDS);
        assertThat(seen).as("a reader observed the recorded list grow").isTrue();
    }

    /**
     * Snapshots and walks the recorded ranges until appending stops, returning how many times the walked list was
     * longer than the previous one. Handing out a live view of the recorded list instead of a snapshot would throw
     * {@link ConcurrentModificationException} mid-walk here.
     */
    private static int observeWhileAppending(
            RecordingByteRangeSource recording,
            AtomicBoolean appending,
            CountDownLatch readersLooping,
            CountDownLatch growthSeen) {
        int growthObservations = 0;
        int previousSize = 0;
        boolean signalledLooping = false;
        try {
            while (appending.get() || !signalledLooping) {
                List<RecordingByteRangeSource.Range> snapshot = recording.ranges();
                assertThat(snapshot.size()).isGreaterThanOrEqualTo(previousSize);
                if (snapshot.size() > previousSize) {
                    growthObservations++;
                    growthSeen.countDown();
                }
                previousSize = snapshot.size();
                long bytes = 0;
                for (RecordingByteRangeSource.Range range : snapshot) {
                    bytes += range.length();
                }
                assertThat(bytes).isEqualTo(snapshot.size()); // every appended read moved exactly one byte
                assertThat(recording.requestCount()).isGreaterThanOrEqualTo(snapshot.size());
                if (!signalledLooping) {
                    signalledLooping = true;
                    readersLooping.countDown();
                }
            }
        } finally {
            if (!signalledLooping) {
                readersLooping.countDown();
            }
        }
        return growthObservations;
    }
}
