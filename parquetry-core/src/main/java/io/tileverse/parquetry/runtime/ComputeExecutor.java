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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared, process-wide pool of platform threads for CPU-bound work. The pool is sized to the core count and admits
 * tasks through a slot semaphore, keeping total concurrency near the core count rather than spreading it over unbounded
 * virtual threads. The abstraction is workload-neutral; its consumer today is row-group decode.
 *
 * <p>The slot semaphore controls submission: callers {@link #tryAcquire()} a slot, then
 * {@link #submitAcquired(Callable)} a task that releases the slot when it completes. Because slots equal pool threads,
 * the pool never queues - a caller that cannot acquire a slot runs the task inline instead, the never-break fallback.
 */
public final class ComputeExecutor {

    private final ExecutorService pool;
    private final Semaphore slots;
    private final int parallelism;

    private ComputeExecutor(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
        }
        this.parallelism = parallelism;
        this.slots = new Semaphore(parallelism);
        this.pool = Executors.newFixedThreadPool(parallelism, daemonFactory());
    }

    /** A pool with {@code parallelism} worker threads. */
    public static ComputeExecutor ofParallelism(int parallelism) {
        return new ComputeExecutor(parallelism);
    }

    /** The shared default pool, sized to the available processor count. */
    public static ComputeExecutor shared() {
        return SharedHolder.INSTANCE;
    }

    public int parallelism() {
        return parallelism;
    }

    /** Slots currently free (for tests and observability). */
    public int availableSlots() {
        return slots.availablePermits();
    }

    /** Tries to reserve one slot without blocking. */
    public boolean tryAcquire() {
        return slots.tryAcquire();
    }

    /** Returns a previously acquired slot without submitting (used when the follow-up fetch fails). */
    public void release() {
        slots.release();
    }

    /**
     * Submits a task using a slot the caller has already acquired via {@link #tryAcquire()}. The slot is released when
     * the task completes. If the pool cannot accept the task, the slot is released and the exception is rethrown for
     * the caller to fall back to inline execution.
     */
    public <T> Future<T> submitAcquired(Callable<T> task) {
        try {
            return pool.submit(() -> {
                try {
                    return task.call();
                } finally {
                    slots.release();
                }
            });
        } catch (RejectedExecutionException e) {
            slots.release();
            throw e;
        }
    }

    /** Shuts the pool down; for tests and custom (non-shared) instances. The shared singleton is never shut down. */
    public void shutdownNow() {
        pool.shutdownNow();
    }

    private static ThreadFactory daemonFactory() {
        AtomicLong counter = new AtomicLong();
        return runnable -> {
            Thread thread = new Thread(runnable, "parquetry-compute-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static final class SharedHolder {
        private static final ComputeExecutor INSTANCE =
                ComputeExecutor.ofParallelism(Runtime.getRuntime().availableProcessors());
    }
}
