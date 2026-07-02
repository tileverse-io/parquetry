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
package io.tileverse.parquetry.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class DecodeBudgetTest {

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
        Thread.sleep(100);
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
}
