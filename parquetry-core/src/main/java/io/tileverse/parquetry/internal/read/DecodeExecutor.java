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
package io.tileverse.parquetry.internal.read;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Shared, process-wide pool of platform threads for CPU-bound row-group decode. Decode is CPU work, so total
 * concurrency across all reads is bounded near the core count rather than spread over unbounded virtual threads.
 *
 * <p>A slot semaphore sized to the pool gates submission: callers {@link #tryAcquire()} a slot, then
 * {@link #submitAcquired(Callable)} a task that releases the slot when it completes. Because slots equal pool threads,
 * the pool never queues - a caller that cannot acquire a slot decodes inline instead, the never-break fallback.
 */
public final class DecodeExecutor {

    private final ExecutorService pool;
    private final Semaphore slots;
    private final int parallelism;

    private DecodeExecutor(int parallelism) {
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
        }
        this.parallelism = parallelism;
        this.slots = new Semaphore(parallelism);
        this.pool = Executors.newFixedThreadPool(parallelism, daemonFactory());
    }

    /** A decode pool with {@code parallelism} worker threads. */
    public static DecodeExecutor ofParallelism(int parallelism) {
        return new DecodeExecutor(parallelism);
    }

    /** The shared default pool, sized to the available processor count. */
    public static DecodeExecutor shared() {
        return SharedHolder.INSTANCE;
    }

    public int parallelism() {
        return parallelism;
    }

    /** Slots currently free (for tests and observability). */
    public int availableSlots() {
        return slots.availablePermits();
    }

    /** Tries to reserve one decode slot without blocking. */
    public boolean tryAcquire() {
        return slots.tryAcquire();
    }

    /** Returns a previously acquired slot without submitting (used when the follow-up fetch fails). */
    public void release() {
        slots.release();
    }

    /**
     * Submits a task using a slot the caller has already acquired via {@link #tryAcquire()}. The slot is released when
     * the task completes. If the pool cannot accept the task, the slot is released and the exception is rethrown so the
     * caller can fall back to inline decode.
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
            Thread thread = new Thread(runnable, "parquetry-decode-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        };
    }

    private static final class SharedHolder {
        private static final DecodeExecutor INSTANCE =
                DecodeExecutor.ofParallelism(Runtime.getRuntime().availableProcessors());
    }
}
