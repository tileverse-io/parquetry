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
package io.tileverse.parquetry.internal.write;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import io.tileverse.parquetry.data.ParquetWriteException;
import io.tileverse.parquetry.runtime.ComputeExecutor;

import lombok.NonNull;

/**
 * Runs independent per-column append units on the shared {@link ComputeExecutor}, falling back to the caller thread
 * when no slot is free. {@link #run(List)} returns only after every unit has finished, which keeps the batch's column
 * writers quiescent before the caller touches them again.
 *
 * <p>The submit-or-inline shape mirrors the read path's decode dispatch: the caller never blocks waiting for a slot,
 * the deadlock-safety invariant for a decode-to-encode pipeline running on one shared pool. A unit failure propagates
 * after every already-submitted unit has completed; an interrupt received while joining is restored on the caller
 * thread rather than aborting the join.
 */
final class ColumnFanOut {

    private final ComputeExecutor pool;

    ColumnFanOut(@NonNull ComputeExecutor pool) {
        this.pool = pool;
    }

    /**
     * Runs every unit to completion. A single unit runs directly on the caller; the pool buys nothing for one unit.
     * Throws the first unit failure once all submitted units have finished, with later failures suppressed.
     */
    void run(@NonNull List<Runnable> units) {
        if (units.isEmpty()) {
            return;
        }
        if (units.size() == 1) {
            units.get(0).run();
            return;
        }
        List<Future<?>> submitted = new ArrayList<>(units.size());
        RuntimeException failure = dispatch(units, submitted);
        failure = awaitQuiescence(submitted, failure);
        if (failure != null) {
            throw failure;
        }
    }

    /**
     * Dispatches units submit-or-inline in list order until an inline unit fails, then stops handing out work: the
     * batch is already lost and the writer transitions to failed. Returns the inline failure, or {@code null}.
     */
    private RuntimeException dispatch(List<Runnable> units, List<Future<?>> submitted) {
        for (Runnable unit : units) {
            if (submitOnFreeSlot(unit, submitted)) {
                continue;
            }
            try {
                unit.run();
            } catch (RuntimeException e) {
                return e;
            }
        }
        return null;
    }

    /** Tries to run the unit on the pool; {@code false} means the caller must run it inline. */
    private boolean submitOnFreeSlot(Runnable unit, List<Future<?>> submitted) {
        if (!pool.tryAcquire()) {
            return false;
        }
        try {
            submitted.add(pool.submitAcquired(() -> {
                unit.run();
                return null;
            }));
            return true;
        } catch (RejectedExecutionException _) {
            // submitAcquired released the slot; the caller runs the unit inline.
            return false;
        }
    }

    /**
     * Waits for every submitted unit, even after a failure: a unit still running while the caller unwinds could race
     * the row group's cleanup. An interrupt while waiting is remembered and the flag restored after the join; the
     * writer's next record-emitting call reports it. Returns the primary failure with later ones suppressed.
     */
    // S2142: the interrupt is deliberately remembered and restored once, after the join; re-interrupting inside the
    // catch would make every retried get() throw immediately and spin until the unit completes.
    @SuppressWarnings("java:S2142")
    private static RuntimeException awaitQuiescence(List<Future<?>> submitted, RuntimeException inlineFailure) {
        RuntimeException failure = inlineFailure;
        boolean interrupted = false;
        for (Future<?> future : submitted) {
            boolean done = false;
            while (!done) {
                try {
                    future.get();
                    done = true;
                } catch (InterruptedException _) {
                    // Units are short CPU-bound tasks; keep waiting to reach quiescence.
                    interrupted = true;
                } catch (ExecutionException e) {
                    failure = accumulate(failure, asRuntime(e.getCause()));
                    done = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
        return failure;
    }

    private static RuntimeException accumulate(RuntimeException primary, RuntimeException next) {
        if (primary == null) {
            return next;
        }
        primary.addSuppressed(next);
        return primary;
    }

    private static RuntimeException asRuntime(Throwable cause) {
        if (cause instanceof Error error) {
            throw error;
        }
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        return new ParquetWriteException("Parallel column append failed", cause);
    }
}
