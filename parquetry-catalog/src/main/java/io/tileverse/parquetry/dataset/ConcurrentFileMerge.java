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

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import com.google.errorprone.annotations.MustBeClosed;

import lombok.NonNull;

/**
 * Drains up to K survivor files concurrently and merges their elements into one pull-based stream. A
 * {@link StructuredTaskScope} owns min(K, N) producer virtual threads; each claims the next unclaimed file index, opens
 * that file's stream on its own thread, pushes elements into a bounded tagged queue, and moves to the next index. The
 * returned stream pulls from the queue; closing it cancels the producers, discards every undelivered element, and joins
 * the scope.
 *
 * <p>Elements must own their data: a {@code ParquetRecordBatch} transfers ownership across the hand-off the same way
 * the row-group decode hand-off does; flyweight row views must never cross. {@code discardElement} releases an element
 * the consumer never received (for batches, {@code ParquetRecordBatch::close}).
 *
 * <p>{@link Emission#UNORDERED} emits elements as any file produces them, the maximum-overlap default.
 * {@link Emission#SURVIVOR_ORDER} emits file 0's elements, then file 1's, and holds later files' ready elements in a
 * reorder buffer; the per-producer permit bounds that buffer at {@code 2 * K} elements while later files keep producing
 * ahead. A file's failure is rethrown at the consumer: immediately when unordered, at that file's emission turn when
 * ordered, which preserves sequential equivalence for windowed reads.
 */
final class ConcurrentFileMerge {

    /** Undelivered elements one producer may have in flight; bounds queue plus reorder buffer at 2 * K. */
    private static final int PER_PRODUCER_PERMITS = 2;

    private static final long POLL_MILLIS = 50L;

    private ConcurrentFileMerge() {}

    enum Emission {
        UNORDERED,
        SURVIVOR_ORDER
    }

    /**
     * Merges {@code fileCount} per-file streams into one. {@code openFile} runs on a producer virtual thread; the
     * per-file stream is opened, drained, and closed there. The returned stream must be closed.
     */
    @MustBeClosed
    static <T> Stream<T> stream(
            int fileCount,
            @NonNull IntFunction<Stream<T>> openFile,
            @NonNull Consumer<T> discardElement,
            int maxConcurrentFiles,
            @NonNull Emission emission) {
        if (fileCount < 0) {
            throw new IllegalArgumentException("fileCount must be >= 0, got " + fileCount);
        }
        if (maxConcurrentFiles <= 0) {
            throw new IllegalArgumentException("maxConcurrentFiles must be > 0, got " + maxConcurrentFiles);
        }
        if (fileCount == 0) {
            return Stream.empty();
        }
        MergeSession<T> session = new MergeSession<>(fileCount, openFile, discardElement, maxConcurrentFiles, emission);
        session.start();
        Spliterator<T> spliterator =
                Spliterators.spliteratorUnknownSize(session, Spliterator.ORDERED | Spliterator.NONNULL);
        return StreamSupport.stream(spliterator, /*parallel*/ false).onClose(session::close);
    }

    /** One element, end, or failure of one file, tagged with the file's index. */
    private sealed interface Item<T> {
        record Element<T>(int file, T value, Semaphore permit) implements Item<T> {}

        record FileEnd<T>(int file) implements Item<T> {}

        record FileFailed<T>(int file, Throwable cause) implements Item<T> {}
    }

    /**
     * The live state of one merge: producers, queue, and the consuming iterator. The consumer thread is the only caller
     * of {@link #hasNext()} / {@link #next()} / {@link #close()}.
     */
    private static final class MergeSession<T> implements Iterator<T>, AutoCloseable {

        private final int fileCount;
        private final IntFunction<Stream<T>> openFile;
        private final Consumer<T> discardElement;
        private final int producerCount;
        private final Emission emission;

        private final BlockingQueue<Item<T>> queue;
        private final AtomicInteger nextFileToClaim = new AtomicInteger();
        private final AtomicBoolean cancelled = new AtomicBoolean();

        private final StructuredTaskScope<Void, Void> scope;

        // Consumer-side state; touched only by the consumer thread.
        private final Map<Integer, Queue<Item.Element<T>>> reorderBuffer = new HashMap<>();
        private final Map<Integer, Throwable> pendingFailures = new HashMap<>();
        private final Set<Integer> endedFiles = new HashSet<>();
        private int emissionCursor;
        private int endedCount;
        private T lookahead;
        private boolean exhausted;

        MergeSession(
                int fileCount,
                IntFunction<Stream<T>> openFile,
                Consumer<T> discardElement,
                int maxConcurrentFiles,
                Emission emission) {
            this.fileCount = fileCount;
            this.openFile = openFile;
            this.discardElement = discardElement;
            this.producerCount = Math.min(maxConcurrentFiles, fileCount);
            this.emission = emission;
            // Elements are permit-bounded at PER_PRODUCER_PERMITS per producer; end/failure markers are one per
            // file. The queue is sized to hold everything, which keeps marker enqueue non-blocking.
            this.queue = new LinkedBlockingQueue<>(producerCount * PER_PRODUCER_PERMITS + fileCount);
            this.scope = StructuredTaskScope.open();
        }

        void start() {
            for (int producer = 0; producer < producerCount; producer++) {
                Semaphore permits = new Semaphore(PER_PRODUCER_PERMITS);
                scope.fork(() -> {
                    runProducer(permits);
                    return null;
                });
            }
        }

        /**
         * Claims ascending file indices until none remain, draining each file's stream into the queue. Every failure,
         * including one thrown by {@code openFile} itself, becomes a tagged marker: the scope always joins clean and
         * the consumer decides when the failure emerges.
         */
        private void runProducer(Semaphore permits) {
            int file = nextFileToClaim.getAndIncrement();
            while (file < fileCount && !cancelled.get()) {
                produceOneFile(file, permits);
                file = nextFileToClaim.getAndIncrement();
            }
        }

        private void produceOneFile(int file, Semaphore permits) {
            try (Stream<T> perFile = openFile.apply(file)) {
                Iterator<T> elements = perFile.iterator();
                while (elements.hasNext()) {
                    T element = elements.next();
                    if (!offerElement(file, element, permits)) {
                        return;
                    }
                }
                enqueueMarker(new Item.FileEnd<>(file));
            } catch (Throwable failure) {
                enqueueMarker(new Item.FileFailed<>(file, failure));
            }
        }

        /**
         * Enqueues one element under the producer's permit bound, parking while the consumer is behind. Returns
         * {@code false} when the merge was cancelled, discarding the element; the caller abandons the file.
         */
        private boolean offerElement(int file, T element, Semaphore permits) {
            while (!cancelled.get()) {
                if (acquireQuietly(permits)) {
                    if (cancelled.get()) {
                        permits.release();
                        break;
                    }
                    queue.add(new Item.Element<>(file, element, permits));
                    if (cancelled.get()) {
                        // Cancellation raced the add: the consumer's drain may already have run. Sweep the queue
                        // from this side too; the queue and the discard hook are both safe off the consumer thread.
                        drainQueueAndDiscard();
                    }
                    return true;
                }
            }
            discardQuietly(element);
            return false;
        }

        private static boolean acquireQuietly(Semaphore permits) {
            try {
                return permits.tryAcquire(POLL_MILLIS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        private void enqueueMarker(Item<T> marker) {
            // The queue reserves a slot per file for its single end-or-failure marker; this add never blocks.
            queue.add(marker);
        }

        @Override
        public boolean hasNext() {
            if (lookahead != null) {
                return true;
            }
            if (exhausted) {
                return false;
            }
            T next = emission == Emission.UNORDERED ? pullUnordered() : pullSurvivorOrder();
            if (next == null) {
                exhausted = true;
                return false;
            }
            lookahead = next;
            return true;
        }

        @Override
        public T next() {
            if (!hasNext()) {
                throw new NoSuchElementException("merge exhausted");
            }
            T element = lookahead;
            lookahead = null;
            return element;
        }

        /** Emits elements in arrival order; a failure emerges at the pull that dequeues it. */
        private T pullUnordered() {
            while (endedCount < fileCount) {
                Item<T> item = takeQuietly();
                switch (item) {
                    case Item.Element<T>(int _, T value, Semaphore permit) -> {
                        permit.release();
                        return value;
                    }
                    case Item.FileEnd<T>(int _) -> endedCount++;
                    case Item.FileFailed<T>(int _, Throwable cause) -> throw unwrapped(cause);
                }
            }
            return null;
        }

        /**
         * Emits file {@code emissionCursor}'s elements, buffering later files' arrivals. Buffered elements hold their
         * producer's permit until delivered, which bounds the buffer at the permit total. A buffered failure emerges
         * only when its file becomes current, matching what a sequential read would have delivered first.
         */
        private T pullSurvivorOrder() {
            while (emissionCursor < fileCount) {
                T buffered = deliverBuffered();
                if (buffered != null) {
                    return buffered;
                }
                if (currentFileDone()) {
                    continue;
                }
                Item<T> item = takeQuietly();
                switch (item) {
                    case Item.Element<T> element -> {
                        if (element.file() == emissionCursor) {
                            element.permit().release();
                            return element.value();
                        }
                        reorderBuffer
                                .computeIfAbsent(element.file(), _ -> new ArrayDeque<>())
                                .add(element);
                    }
                    case Item.FileEnd<T>(int file) -> endedFiles.add(file);
                    case Item.FileFailed<T>(int file, Throwable cause) -> pendingFailures.put(file, cause);
                }
            }
            return null;
        }

        /** The current file's next buffered element, or {@code null} when none is buffered. */
        private T deliverBuffered() {
            Queue<Item.Element<T>> buffered = reorderBuffer.get(emissionCursor);
            if (buffered == null || buffered.isEmpty()) {
                return null;
            }
            Item.Element<T> element = buffered.poll();
            element.permit().release();
            return element.value();
        }

        /**
         * Advances the emission cursor past the current file when it has ended, rethrowing its failure at exactly this
         * point. Returns {@code true} when the cursor advanced.
         */
        private boolean currentFileDone() {
            Throwable failure = pendingFailures.get(emissionCursor);
            if (failure != null) {
                throw unwrapped(failure);
            }
            boolean bufferedRemain = reorderBuffer.containsKey(emissionCursor)
                    && !reorderBuffer.get(emissionCursor).isEmpty();
            if (!bufferedRemain && endedFiles.contains(emissionCursor)) {
                reorderBuffer.remove(emissionCursor);
                emissionCursor++;
                return true;
            }
            return false;
        }

        private Item<T> takeQuietly() {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while awaiting a merged element", e);
            }
        }

        private static RuntimeException unwrapped(Throwable cause) {
            if (cause instanceof RuntimeException runtime) {
                return runtime;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            return new IllegalStateException("Reading a dataset file failed", cause);
        }

        /**
         * Cancels the merge: producers stop at their next permit poll or claim, every undelivered element is discarded,
         * and the scope is joined before returning. Join-before-close is the scope's structural requirement; producers
         * trap their own failures, hence the join never throws a task failure.
         */
        @Override
        public void close() {
            cancelled.set(true);
            drainAndDiscard();
            joinScopeQuietly();
            scope.close();
            drainAndDiscard();
            if (lookahead != null) {
                discardQuietly(lookahead);
                lookahead = null;
            }
        }

        private void joinScopeQuietly() {
            try {
                scope.join();
            } catch (InterruptedException _) {
                Thread.currentThread().interrupt();
            } catch (StructuredTaskScope.FailedException _) {
                // Producers trap failures into markers; a scope-level failure only means a producer died after
                // cancellation, when its markers no longer matter.
            }
        }

        /** Empties the shared queue only; safe from any thread. */
        private void drainQueueAndDiscard() {
            Item<T> item = queue.poll();
            while (item != null) {
                discardItem(item);
                item = queue.poll();
            }
        }

        private void drainAndDiscard() {
            drainQueueAndDiscard();
            for (Queue<Item.Element<T>> buffered : reorderBuffer.values()) {
                Item.Element<T> element = buffered.poll();
                while (element != null) {
                    discardItem(element);
                    element = buffered.poll();
                }
            }
            reorderBuffer.clear();
        }

        private void discardItem(Item<T> item) {
            if (item instanceof Item.Element<T>(int _, T value, Semaphore permit)) {
                discardQuietly(value);
                permit.release();
            }
        }

        private void discardQuietly(T element) {
            try {
                discardElement.accept(element);
            } catch (RuntimeException _) {
                // best-effort; a chronic leak shows up in pool and budget accounting
            }
        }
    }
}
