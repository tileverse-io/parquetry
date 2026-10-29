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
package io.tileverse.parquetry.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DecodeBudgetTest {

    // reservations from many threads never exceed the capacity between them, and every release comes back
    @Test
    @Timeout(30)
    void concurrentReservesStayUnderCapacity() throws Exception {
        long capacity = 10_000;
        DecodeBudget budget = DecodeBudget.ofBytes(capacity);
        AtomicLong outstanding = new AtomicLong();
        AtomicBoolean overCommitted = new AtomicBoolean(false);
        try (ExecutorService executor = Executors.newFixedThreadPool(8)) {
            for (int t = 0; t < 8; t++) {
                executor.execute(() -> {
                    for (int i = 1; i <= 5000; i++) {
                        long bytes = 1 + (i % 700);
                        if (budget.tryReserve(bytes)) {
                            if (outstanding.addAndGet(bytes) > capacity) {
                                overCommitted.set(true);
                            }
                            outstanding.addAndGet(-bytes);
                            budget.release(bytes);
                        }
                    }
                });
            }
            executor.shutdown();
            assertThat(executor.awaitTermination(20, TimeUnit.SECONDS)).isTrue();
        }
        assertThat(overCommitted)
                .as("the reservations of all threads fit the capacity")
                .isFalse();
        assertThat(budget.available()).isEqualTo(capacity);
    }

    // a release made while a reserver parks wakes it, whichever of the two looks at the headroom first
    // a reservation the headroom covers must not fail because an oversized one is failing on another thread
    @Test
    @Timeout(30)
    void fittingReserveNeverFails() throws Exception {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        assertThat(budget.tryReserve(50)).isTrue();
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong spuriousFailures = new AtomicLong();
        Thread oversized = Thread.ofPlatform().start(() -> {
            while (!stop.get()) {
                budget.tryReserve(80);
            }
        });
        for (int i = 0; i < 200_000; i++) {
            if (budget.tryReserve(40)) {
                budget.release(40);
            } else {
                spuriousFailures.incrementAndGet();
            }
        }
        stop.set(true);
        oversized.join();
        assertThat(spuriousFailures.get())
                .as("40 bytes always fit the 50 bytes of headroom")
                .isZero();
    }

    @Test
    @Timeout(30)
    void parkedReserverWakesOnEveryRelease() throws Exception {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        for (int round = 0; round < 200; round++) {
            assertThat(budget.tryReserve(100)).isTrue();
            Thread waiter = new Thread(() -> budget.reserve(60, () -> false));
            waiter.start();
            budget.release(60);
            waiter.join(5_000);
            assertThat(waiter.isAlive())
                    .as("round " + round + ": the reserver got the released bytes")
                    .isFalse();
            assertThat(budget.available())
                    .as("the reserver holds the released bytes")
                    .isZero();
            budget.release(40);
            budget.release(60);
            assertThat(budget.available()).isEqualTo(100);
        }
    }

    @Test
    void reservesWhenCapacityAvailable() {
        DecodeBudget budget = DecodeBudget.ofBytes(1_000);
        assertThat(budget.tryReserve(600)).isTrue();
        assertThat(budget.available()).isEqualTo(400);
    }

    @Test
    void refusesReservationThatExceedsRemaining() {
        DecodeBudget budget = DecodeBudget.ofBytes(1_000);
        assertThat(budget.tryReserve(600)).isTrue();
        assertThat(budget.tryReserve(600)).isFalse();
        assertThat(budget.available()).isEqualTo(400);
    }

    @Test
    void releaseRestoresCapacity() {
        DecodeBudget budget = DecodeBudget.ofBytes(1_000);
        budget.tryReserve(1_000);
        budget.release(400);
        assertThat(budget.available()).isEqualTo(400);
        assertThat(budget.tryReserve(400)).isTrue();
    }

    @Test
    void releaseNeverInflatesAboveCapacity() {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        budget.tryReserve(40);
        budget.release(40);
        budget.release(40);
        assertThat(budget.available()).isEqualTo(100);
    }

    // Above half of Long.MAX_VALUE, the headroom plus a release can exceed the long range.
    @Test
    void maxCapacityReleaseNeverWraps() {
        DecodeBudget budget = DecodeBudget.ofBytes(Long.MAX_VALUE);
        budget.tryReserve(1);
        budget.release(2);
        assertThat(budget.available()).isEqualTo(Long.MAX_VALUE);
        assertThat(budget.tryReserve(1)).isTrue();
    }

    @Test
    void releaseLargerThanCapacityIsClampedNotOverflowed() {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        budget.tryReserve(100);
        budget.release(Long.MAX_VALUE);
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void reserveZeroBytesAlwaysSucceedsWithoutConsuming() {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        assertThat(budget.tryReserve(0)).isTrue();
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatIllegalArgumentException().isThrownBy(() -> DecodeBudget.ofBytes(0));
    }

    @Test
    void rejectsFractionOutsideUnitInterval() {
        assertThatIllegalArgumentException().isThrownBy(() -> DecodeBudget.ofMaxMemoryFraction(0));
        assertThatIllegalArgumentException().isThrownBy(() -> DecodeBudget.ofMaxMemoryFraction(1.5));
        assertThatIllegalArgumentException().isThrownBy(() -> DecodeBudget.ofMaxMemoryFraction(-0.5));
    }

    @Test
    void fractionBasisIsHeapMaxMemory() {
        double fraction = 0.25;
        long expected = Math.max(1L, (long) (Runtime.getRuntime().maxMemory() * fraction));
        DecodeBudget budget = DecodeBudget.ofMaxMemoryFraction(fraction);
        assertThat(budget.capacity()).isEqualTo(expected);
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void blockingReserveUnblocksOnRelease() throws InterruptedException {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        budget.tryReserve(100);
        CountDownLatch reserved = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            budget.reserve(60, () -> false);
            reserved.countDown();
        });
        waiter.start();
        assertThat(reserved.await(200, TimeUnit.MILLISECONDS))
                .as("reserve must block while the budget is full")
                .isFalse();
        budget.release(60);
        assertThat(reserved.await(2, TimeUnit.SECONDS))
                .as("reserve must complete once headroom is released")
                .isTrue();
        waiter.join();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void blockingReserveAbortsWhenGiveUpBecomesTrue() throws InterruptedException {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        budget.tryReserve(100);
        AtomicBoolean giveUp = new AtomicBoolean(false);
        AtomicBoolean result = new AtomicBoolean(true);
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            result.set(budget.reserve(60, giveUp::get));
            done.countDown();
        });
        waiter.start();
        awaitParkedOnTheFullBudget(waiter);
        giveUp.set(true);
        budget.wakeWaiters();
        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get()).as("aborted reserve returns false").isFalse();
        assertThat(budget.available()).as("an aborted reserve consumes nothing").isZero();
        waiter.join();
    }

    @Test
    void reserveSucceedsImmediatelyWhenHeadroomExists() {
        DecodeBudget budget = DecodeBudget.ofBytes(100);
        assertThat(budget.reserve(40, () -> false)).isTrue();
        assertThat(budget.available()).isEqualTo(60);
    }

    @Test
    void defaultBudgetHasPositiveCapacity() {
        assertThat(DecodeBudget.defaultBudget().capacity()).isPositive();
    }

    /**
     * Spins until {@code waiter} parks inside its reserve call. Raising the give-up flag before the thread parks would
     * take the fast exit and leave the wake path untested, and a fixed pause only guesses at when the park happens.
     */
    private static void awaitParkedOnTheFullBudget(Thread waiter) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (waiter.getState() != Thread.State.WAITING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the reserving thread never parked on the full budget");
            }
            Thread.onSpinWait();
        }
    }
}
