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
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.tileverse.parquetry.runtime.FetchBudget;

class FetchBudgetTest {

    @Test
    void reservesWhenCapacityAvailable() {
        FetchBudget budget = FetchBudget.ofBytes(1_000);
        assertThat(budget.tryReserve(600)).isTrue();
        assertThat(budget.available()).isEqualTo(400);
    }

    @Test
    void refusesReservationThatExceedsRemaining() {
        FetchBudget budget = FetchBudget.ofBytes(1_000);
        assertThat(budget.tryReserve(600)).isTrue();
        assertThat(budget.tryReserve(600)).isFalse();
        assertThat(budget.available()).isEqualTo(400);
    }

    @Test
    void releaseRestoresCapacity() {
        FetchBudget budget = FetchBudget.ofBytes(1_000);
        budget.tryReserve(1_000);
        budget.release(400);
        assertThat(budget.available()).isEqualTo(400);
        assertThat(budget.tryReserve(400)).isTrue();
    }

    @Test
    void reservationLargerThanCapacityNeverSucceeds() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        assertThat(budget.tryReserve(101)).isFalse();
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void rejectsNonPositiveCapacity() {
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofBytes(0));
    }

    @Test
    void rejectsFractionOutsideUnitInterval() {
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofMaxMemoryFraction(0));
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofMaxMemoryFraction(1.5));
    }

    @Test
    void fractionOfMaxMemoryYieldsPositiveCapacity() {
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        assertThat(budget.capacity()).isPositive();
    }

    @Test
    void reserveZeroBytesAlwaysSucceedsWithoutConsuming() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        assertThat(budget.tryReserve(0)).isTrue();
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void releaseNeverInflatesAboveCapacity() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(40);
        budget.release(40);
        budget.release(40);
        assertThat(budget.available()).isEqualTo(100);
    }

    @Test
    void releaseLargerThanCapacityIsClampedNotOverflowed() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        budget.release(Long.MAX_VALUE);
        assertThat(budget.available()).isEqualTo(100);
        assertThat(budget.tryReserve(100)).isTrue();
    }

    @Test
    void rejectsNegativeFraction() {
        assertThatIllegalArgumentException().isThrownBy(() -> FetchBudget.ofMaxMemoryFraction(-0.5));
    }

    @Test
    @Timeout(5)
    void reserveWaitsForRoom() throws InterruptedException {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        AtomicBoolean granted = new AtomicBoolean();
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            granted.set(budget.reserve(60, Duration.ofSeconds(2)));
            done.countDown();
        });
        waiter.start();
        assertThat(done.await(200, TimeUnit.MILLISECONDS))
                .as("waits while the budget is full")
                .isFalse();
        budget.release(60);
        assertThat(done.await(2, TimeUnit.SECONDS)).isTrue();
        assertThat(granted.get()).isTrue();
        assertThat(budget.available()).isZero();
    }

    @Test
    @Timeout(5)
    void reserveExpiresWithoutRoom() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        assertThat(budget.reserve(60, Duration.ofMillis(100))).isFalse();
        assertThat(budget.available()).isZero();
        FetchBudget.WaitStats stats = budget.waitStats();
        assertThat(stats.waits()).isEqualTo(1);
        assertThat(stats.expired()).isEqualTo(1);
        assertThat(stats.waitedNanos()).isGreaterThan(TimeUnit.MILLISECONDS.toNanos(50));
    }

    @Test
    @Timeout(5)
    void reserveAboveCapacityFailsAtOnce() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        long start = System.nanoTime();
        assertThat(budget.reserve(101, Duration.ofSeconds(2))).isFalse();
        assertThat(System.nanoTime() - start).isLessThan(TimeUnit.MILLISECONDS.toNanos(500));
        assertThat(budget.waitStats().waits()).isZero();
    }

    @Test
    @Timeout(5)
    void reserveZeroBytesSucceeds() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        assertThat(budget.reserve(0, Duration.ofSeconds(1))).isTrue();
        assertThat(budget.reserve(-1, Duration.ofSeconds(1))).isTrue();
        assertThat(budget.tryReserve(-1)).isTrue();
    }

    // a wait of "forever" is a valid setting: its nanoseconds saturate instead of overflowing
    @Test
    @Timeout(5)
    void hugeWaitDoesNotOverflow() throws InterruptedException {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        AtomicBoolean granted = new AtomicBoolean();
        Thread waiter = new Thread(() -> granted.set(budget.reserve(60, ChronoUnit.FOREVER.getDuration())));
        waiter.start();
        awaitParked(waiter);
        budget.release(60);
        waiter.join(2_000);
        assertThat(granted).isTrue();
    }

    @Test
    @Timeout(5)
    void zeroWaitReservesOnlyWhenRoom() {
        FetchBudget budget = FetchBudget.ofBytes(100);
        assertThat(budget.reserve(60, Duration.ZERO)).isTrue();
        assertThat(budget.reserve(60, Duration.ZERO)).isFalse();
        assertThat(budget.waitStats().waits()).isZero();
    }

    // strict FIFO: a 30-byte waiter behind a 60-byte waiter does not take 40 free bytes
    @Test
    @Timeout(10)
    void waitersAreServedInOrder() throws InterruptedException {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        Thread first = waiterFor(budget, 60);
        awaitParked(first);
        Thread second = waiterFor(budget, 30);
        awaitParked(second);
        budget.release(40);
        second.join(200);
        assertThat(first.isAlive()).as("the head needs 60").isTrue();
        assertThat(second.isAlive()).as("the second waits behind the head").isTrue();
        budget.release(20);
        first.join(2_000);
        assertThat(first.isAlive()).isFalse();
        assertThat(second.isAlive())
                .as("the 60 went to the head; nothing is left")
                .isTrue();
        budget.release(30);
        second.join(2_000);
        assertThat(second.isAlive()).isFalse();
    }

    @Test
    @Timeout(5)
    void tryReserveYieldsToWaiters() throws InterruptedException {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(95);
        Thread waiter = waiterFor(budget, 60);
        awaitParked(waiter);
        assertThat(budget.tryReserve(5))
                .as("5 bytes are free but a waiter is queued")
                .isFalse();
        budget.release(95);
        waiter.join(2_000);
        assertThat(budget.tryReserve(5)).isTrue();
    }

    @Test
    @Timeout(5)
    void reserveReturnsFalseOnInterrupt() throws InterruptedException {
        FetchBudget budget = FetchBudget.ofBytes(100);
        budget.tryReserve(100);
        AtomicBoolean result = new AtomicBoolean(true);
        AtomicBoolean flagRestored = new AtomicBoolean();
        Thread waiter = new Thread(() -> {
            result.set(budget.reserve(60, Duration.ofSeconds(5)));
            flagRestored.set(Thread.currentThread().isInterrupted());
        });
        waiter.start();
        awaitParked(waiter);
        waiter.interrupt();
        waiter.join(2_000);
        assertThat(result.get()).isFalse();
        assertThat(flagRestored.get()).isTrue();
    }

    /** Starts a thread that reserves {@code bytes} with a five-second wait. */
    private static Thread waiterFor(FetchBudget budget, long bytes) {
        Thread waiter = new Thread(() -> budget.reserve(bytes, Duration.ofSeconds(5)));
        waiter.start();
        return waiter;
    }

    /** Spins until {@code waiter} parks inside its timed wait, or fails after two seconds. */
    private static void awaitParked(Thread waiter) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (waiter.getState() != Thread.State.TIMED_WAITING) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("the reserving thread never parked on the full budget");
            }
            Thread.onSpinWait();
        }
    }
}
