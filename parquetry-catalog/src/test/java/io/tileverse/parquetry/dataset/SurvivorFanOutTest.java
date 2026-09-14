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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class SurvivorFanOutTest {

    private static final Duration AMPLE = Duration.ofSeconds(10);

    @Test
    void sumAddsEveryFilesResult() {
        long total = SurvivorFanOut.sum(5, index -> index * 10L, 8);

        assertThat(total).isEqualTo(100L);
    }

    @Test
    void zeroFilesSumToZeroWithoutRunningAnyTask() {
        long total = SurvivorFanOut.sum(
                0,
                index -> {
                    throw new AssertionError("no task must run for an empty survivor set");
                },
                8);

        assertThat(total).isZero();
    }

    @Test
    void aSingleFileRunsItsTaskOnTheCallingThread() {
        AtomicReference<Thread> summedOn = new AtomicReference<>();
        AtomicReference<Thread> visitedOn = new AtomicReference<>();

        long total = SurvivorFanOut.sum(
                1,
                index -> {
                    summedOn.set(Thread.currentThread());
                    return 7L;
                },
                4);
        SurvivorFanOut.forEach(1, index -> visitedOn.set(Thread.currentThread()), 4);

        assertThat(total).isEqualTo(7L);
        assertThat(summedOn).hasValue(Thread.currentThread());
        assertThat(visitedOn).hasValue(Thread.currentThread());
    }

    @Test
    void aSingleFileFailurePropagatesUnwrapped() {
        IllegalStateException failure = new IllegalStateException("the only file failed");

        Throwable thrown = catchThrowable(() -> SurvivorFanOut.sum(
                1,
                index -> {
                    throw failure;
                },
                4));

        assertThat(thrown).isSameAs(failure);
    }

    @Test
    void neverRunsMoreTasksAtOnceThanTheWidth() {
        int width = 3;
        int fileCount = 5 * width;
        CyclicBarrier fullWidth = new CyclicBarrier(width);
        AtomicInteger inFlight = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        Set<Integer> visited = ConcurrentHashMap.newKeySet();

        SurvivorFanOut.forEach(
                fileCount,
                index -> {
                    int running = inFlight.incrementAndGet();
                    peak.accumulateAndGet(running, Math::max);
                    awaitTheOtherRunningTasks(fullWidth);
                    visited.add(index);
                    inFlight.decrementAndGet();
                },
                width);

        assertThat(peak).hasValueBetween(2, width);
        assertThat(visited)
                .containsExactlyInAnyOrderElementsOf(
                        IntStream.range(0, fileCount).boxed().toList());
    }

    @Test
    void aFailingFilePropagatesItsExceptionUnwrapped() {
        assertThatThrownBy(() -> SurvivorFanOut.sum(
                        4,
                        index -> {
                            if (index == 2) {
                                throw new IllegalStateException("file 2 failed");
                            }
                            return 1L;
                        },
                        4))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("file 2 failed");
    }

    @Test
    void aFailingFilePropagatesItsErrorUnwrapped() {
        AssertionError raised = new AssertionError("file 1 failed hard");

        Throwable thrown = catchThrowable(() -> SurvivorFanOut.forEach(
                4,
                index -> {
                    if (index == 1) {
                        throw raised;
                    }
                },
                4));

        assertThat(thrown).isSameAs(raised);
    }

    @Test
    void aCheckedFailureArrivesWrappedOverItsCause() {
        IOException checked = new IOException("file 0 is truncated");

        assertThatThrownBy(() -> SurvivorFanOut.sum(
                        4,
                        index -> {
                            if (index == 0) {
                                throwUnchecked(checked);
                            }
                            return 1L;
                        },
                        4))
                .isInstanceOf(IllegalStateException.class)
                .cause()
                .isSameAs(checked);
    }

    @Test
    void aTaskInterruptedWaitingForItsTurnReportsAnInterruptedRead() {
        InterruptedException interrupted = new InterruptedException("waiting for a permit");

        Throwable thrown = catchThrowable(() -> SurvivorFanOut.forEach(2, index -> throwUnchecked(interrupted), 1));
        boolean flagRestored = Thread.interrupted();

        assertThat(thrown)
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isInstanceOf(InterruptedIOException.class)
                .cause()
                .isSameAs(interrupted);
        assertThat(flagRestored).isTrue();
    }

    @Test
    void interruptionRestoresTheFlagAndReportsAnInterruptedRead() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean flagRestored = new AtomicBoolean();
        Thread caller = new Thread(() -> {
            try {
                SurvivorFanOut.forEach(
                        2,
                        index -> {
                            started.countDown();
                            awaitQuietly(release);
                        },
                        2);
            } catch (RuntimeException e) {
                failure.set(e);
                flagRestored.set(Thread.currentThread().isInterrupted());
            }
        });
        caller.start();
        assertThat(started.await(AMPLE.toSeconds(), TimeUnit.SECONDS)).isTrue();

        caller.interrupt();
        boolean callerReturned = caller.join(AMPLE);
        release.countDown();

        assertThat(callerReturned).as("the interrupted caller must return").isTrue();
        assertThat(failure.get()).isInstanceOf(UncheckedIOException.class);
        assertThat(failure.get().getCause()).isInstanceOf(InterruptedIOException.class);
        assertThat(flagRestored).isTrue();
    }

    @Test
    void rejectsANonPositiveWidth() {
        assertThatThrownBy(() -> SurvivorFanOut.forEach(1, index -> {}, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsANegativeFileCount() {
        assertThatThrownBy(() -> SurvivorFanOut.sum(-1, index -> 1L, 4)).isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * Blocks until a full width of tasks is inside the task body, which pins the observed concurrency to the width
     * instead of leaving it to how the virtual threads happen to be scheduled.
     */
    private static void awaitTheOtherRunningTasks(CyclicBarrier barrier) {
        try {
            barrier.await(AMPLE.toSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the other tasks", e);
        } catch (BrokenBarrierException | TimeoutException e) {
            throw new AssertionError("the fold never ran a full width of tasks at once", e);
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException _) {
            Thread.currentThread().interrupt();
        }
    }

    /** Throws {@code checked} from a task body, where the functional interface declares no checked exception. */
    @SuppressWarnings("unchecked")
    private static <E extends Throwable> void throwUnchecked(Throwable checked) throws E {
        throw (E) checked;
    }
}
