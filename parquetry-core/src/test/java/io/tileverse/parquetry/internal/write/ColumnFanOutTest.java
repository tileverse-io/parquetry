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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.runtime.ComputeExecutor;

/**
 * Pins the fan-out contract {@link RowGroupWriter} relies on: every unit runs exactly once, the call returns only after
 * all units have finished, a failure propagates after quiescence, and an exhausted pool degrades to inline execution on
 * the caller.
 */
class ColumnFanOutTest {

    private final List<ComputeExecutor> pools = new ArrayList<>();

    @AfterEach
    void shutdownPools() {
        pools.forEach(ComputeExecutor::shutdownNow);
    }

    private ComputeExecutor pool(int parallelism) {
        ComputeExecutor pool = ComputeExecutor.ofParallelism(parallelism);
        pools.add(pool);
        return pool;
    }

    @Test
    void runsEveryUnitAndReleasesEverySlot() {
        ComputeExecutor pool = pool(4);
        ColumnFanOut fanOut = new ColumnFanOut(pool);
        int unitCount = 32;
        List<AtomicBoolean> ran = new ArrayList<>(unitCount);
        List<Runnable> units = new ArrayList<>(unitCount);
        for (int i = 0; i < unitCount; i++) {
            AtomicBoolean flag = new AtomicBoolean();
            ran.add(flag);
            units.add(() -> flag.set(true));
        }

        fanOut.run(units);

        assertThat(ran).allMatch(AtomicBoolean::get);
        assertThat(pool.availableSlots()).isEqualTo(4);
    }

    @Test
    void emptyUnitListIsANoOp() {
        ComputeExecutor pool = pool(2);
        new ColumnFanOut(pool).run(List.of());
        assertThat(pool.availableSlots()).isEqualTo(2);
    }

    @Test
    void singleUnitRunsOnTheCallerWithoutDispatch() {
        ComputeExecutor pool = pool(2);
        Thread caller = Thread.currentThread();
        AtomicReference<Thread> executedOn = new AtomicReference<>();

        new ColumnFanOut(pool).run(List.of(() -> executedOn.set(Thread.currentThread())));

        assertThat(executedOn.get()).isSameAs(caller);
        assertThat(pool.availableSlots()).isEqualTo(2);
    }

    @Test
    void exhaustedPoolRunsEveryUnitOnTheCallerThread() {
        ComputeExecutor pool = pool(2);
        assertThat(pool.tryAcquire()).isTrue();
        assertThat(pool.tryAcquire()).isTrue();
        try {
            Thread caller = Thread.currentThread();
            List<Thread> executedOn = new ArrayList<>();
            List<Runnable> units = List.of(
                    () -> executedOn.add(Thread.currentThread()),
                    () -> executedOn.add(Thread.currentThread()),
                    () -> executedOn.add(Thread.currentThread()));

            new ColumnFanOut(pool).run(units);

            assertThat(executedOn).hasSize(3).allMatch(thread -> thread == caller);
        } finally {
            pool.release();
            pool.release();
        }
    }

    @Test
    void inlineFailureStopsDispatchAndPropagates() {
        ComputeExecutor pool = pool(2);
        assertThat(pool.tryAcquire()).isTrue();
        assertThat(pool.tryAcquire()).isTrue();
        try {
            AtomicInteger survivors = new AtomicInteger();
            List<Runnable> units = List.of(
                    survivors::incrementAndGet,
                    () -> {
                        throw new IllegalStateException("unit failed");
                    },
                    survivors::incrementAndGet);

            ColumnFanOut fanOut = new ColumnFanOut(pool);
            assertThatThrownBy(() -> fanOut.run(units))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage("unit failed");
            assertThat(survivors).hasValue(1);
        } finally {
            pool.release();
            pool.release();
        }
    }

    @Test
    void pooledUnitFailurePropagatesToTheCaller() {
        ComputeExecutor pool = pool(4);
        List<Runnable> units = List.of(
                () -> {},
                () -> {
                    throw new IllegalStateException("pooled unit failed");
                },
                () -> {},
                () -> {});

        ColumnFanOut fanOut = new ColumnFanOut(pool);
        assertThatThrownBy(() -> fanOut.run(units))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("pooled unit failed");
        assertThat(pool.availableSlots()).isEqualTo(4);
    }

    // S2925: the sleep is the simulated unit workload, keeping units in flight while the caller joins; nothing
    // synchronizes on its duration.
    @SuppressWarnings("java:S2925")
    @Test
    void interruptDuringJoinReachesQuiescenceAndRestoresTheFlag() {
        ComputeExecutor pool = pool(2);
        AtomicInteger finished = new AtomicInteger();
        Runnable slowUnit = () -> {
            try {
                Thread.sleep(25);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(e);
            }
            finished.incrementAndGet();
        };

        Thread.currentThread().interrupt();
        new ColumnFanOut(pool).run(List.of(slowUnit, slowUnit));

        assertThat(Thread.interrupted()).isTrue();
        assertThat(finished).hasValue(2);
    }
}
