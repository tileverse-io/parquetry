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

import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.StructuredTaskScope.Subtask;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntToLongFunction;

/**
 * The bounded per-file fan-out shared by the catalog datasets for their metadata-shaped queries (count, bounds). Every
 * dense index in {@code [0, fileCount)} runs its task on its own virtual thread, and at most {@code maxConcurrentFiles}
 * tasks run at once: when each task opens a file, no more than that many readers are alive across the fan-out. A single
 * file is the exception: its task runs on the calling thread, with no scope and no extra thread. The first failure
 * cancels the tasks still pending and is rethrown as the task threw it: a {@link RuntimeException} or an {@link Error}
 * unchanged, anything else wrapped in {@link IllegalStateException}; the single-file path, with no scope to unwrap,
 * rethrows whatever the task threw. An interruption, of the calling thread or of a task waiting for its turn, restores
 * the calling thread's interrupt flag and throws {@link UncheckedIOException} over an {@link InterruptedIOException}.
 *
 * <p>Public for the same reason as {@link ConcurrentSurvivorReads}: the STAC and Iceberg datasets live in their own
 * modules.
 */
public final class SurvivorFanOut {

    private SurvivorFanOut() {}

    /** Runs {@code perFile} for every dense index and returns the sum of what the tasks return. */
    public static long sum(int fileCount, IntToLongFunction perFile, int maxConcurrentFiles) {
        List<Long> perFileTotals = runAll(fileCount, perFile::applyAsLong, maxConcurrentFiles);
        long total = 0L;
        for (Long fileTotal : perFileTotals) {
            total += fileTotal;
        }
        return total;
    }

    /**
     * Runs {@code perFile} for every dense index. A task folds its outcome into state owned by the caller, which must
     * be safe to update from several virtual threads at once.
     */
    public static void forEach(int fileCount, IntConsumer perFile, int maxConcurrentFiles) {
        runAll(
                fileCount,
                index -> {
                    perFile.accept(index);
                    return null;
                },
                maxConcurrentFiles);
    }

    /**
     * Runs {@code task} for every dense index and returns what the tasks returned, in index order. One file runs inline
     * on the calling thread; two or more fan out under the bounded scope.
     */
    private static <R> List<R> runAll(int fileCount, IntFunction<R> task, int maxConcurrentFiles) {
        if (fileCount < 0) {
            throw new IllegalArgumentException("fileCount must be >= 0, got " + fileCount);
        }
        if (maxConcurrentFiles <= 0) {
            throw new IllegalArgumentException("maxConcurrentFiles must be > 0, got " + maxConcurrentFiles);
        }
        if (fileCount == 1) {
            // singletonList, not List.of: a task run for its effect alone returns null.
            return Collections.singletonList(task.apply(0));
        }
        return forkAll(fileCount, task, maxConcurrentFiles);
    }

    private static <R> List<R> forkAll(int fileCount, IntFunction<R> task, int maxConcurrentFiles) {
        Semaphore permits = new Semaphore(maxConcurrentFiles);
        try (StructuredTaskScope<R, Void> scope = StructuredTaskScope.open()) {
            List<Subtask<R>> subtasks = new ArrayList<>(fileCount);
            for (int index = 0; index < fileCount; index++) {
                int file = index;
                subtasks.add(scope.fork(() -> runWithPermit(file, task, permits)));
            }
            scope.join();
            return resultsOf(subtasks);
        } catch (InterruptedException e) {
            throw interruptedVisit(e);
        } catch (StructuredTaskScope.FailedException e) {
            throw asUnchecked(e.getCause());
        }
    }

    private static <R> List<R> resultsOf(List<Subtask<R>> subtasks) {
        List<R> results = new ArrayList<>(subtasks.size());
        for (Subtask<R> subtask : subtasks) {
            results.add(subtask.get());
        }
        return results;
    }

    private static <R> R runWithPermit(int file, IntFunction<R> task, Semaphore permits) throws InterruptedException {
        permits.acquire();
        try {
            return task.apply(file);
        } finally {
            permits.release();
        }
    }

    private static RuntimeException asUnchecked(Throwable cause) {
        if (cause instanceof InterruptedException interrupted) {
            return interruptedVisit(interrupted);
        }
        if (cause instanceof RuntimeException runtime) {
            return runtime;
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new IllegalStateException("Visiting a dataset file failed", cause);
    }

    private static UncheckedIOException interruptedVisit(InterruptedException cause) {
        Thread.currentThread().interrupt();
        InterruptedIOException interrupted = new InterruptedIOException("Interrupted while visiting dataset files");
        interrupted.initCause(cause);
        return new UncheckedIOException(interrupted);
    }
}
