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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.dataset.ConcurrentFileMerge.Emission;

class ConcurrentFileMergeTest {

    private static IntFunction<Stream<String>> filesOf(List<List<String>> perFile) {
        return index -> perFile.get(index).stream();
    }

    @Test
    void unorderedDeliversEveryElementExactlyOnce() {
        List<List<String>> files = List.of(List.of("a0", "a1", "a2"), List.of("b0"), List.of(), List.of("d0", "d1"));
        List<String> seen;
        try (Stream<String> merged =
                ConcurrentFileMerge.stream(files.size(), filesOf(files), element -> {}, 3, Emission.UNORDERED)) {
            seen = merged.toList();
        }
        assertThat(seen).containsExactlyInAnyOrder("a0", "a1", "a2", "b0", "d0", "d1");
    }

    @Test
    void survivorOrderEmitsFilesInIndexOrderRegardlessOfProductionSpeed() {
        CountDownLatch releaseFileZero = new CountDownLatch(1);
        IntFunction<Stream<String>> openFile = index -> {
            if (index == 0) {
                return Stream.of("slow0", "slow1").map(value -> {
                    awaitQuietly(releaseFileZero);
                    return value;
                });
            }
            return Stream.of("f" + index);
        };
        List<String> seen;
        try (Stream<String> merged =
                ConcurrentFileMerge.stream(4, openFile, element -> {}, 4, Emission.SURVIVOR_ORDER)) {
            releaseFileZero.countDown();
            seen = merged.toList();
        }
        assertThat(seen).containsExactly("slow0", "slow1", "f1", "f2", "f3");
    }

    @Test
    void survivorOrderIsDeterministicAcrossRepeats() {
        List<List<String>> files = new ArrayList<>();
        for (int f = 0; f < 12; f++) {
            List<String> rows = new ArrayList<>();
            for (int r = 0; r < 5; r++) {
                rows.add("f" + f + "r" + r);
            }
            files.add(rows);
        }
        List<String> first = drain(files, Emission.SURVIVOR_ORDER, 3);
        for (int repeat = 0; repeat < 10; repeat++) {
            assertThat(drain(files, Emission.SURVIVOR_ORDER, 3)).isEqualTo(first);
        }
    }

    @Test
    void producerFailurePropagatesUnwrapped() {
        IntFunction<Stream<String>> openFile = index -> {
            if (index == 1) {
                throw new IllegalStateException("file 1 failed to open");
            }
            return Stream.of("f" + index);
        };
        assertThatThrownBy(() -> {
                    try (Stream<String> merged =
                            ConcurrentFileMerge.stream(3, openFile, element -> {}, 3, Emission.UNORDERED)) {
                        merged.toList();
                    }
                })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("file 1 failed to open");
    }

    @Test
    void survivorOrderDeliversEarlierFilesBeforeALaterFailure() {
        IntFunction<Stream<String>> openFile = index -> {
            if (index == 2) {
                throw new IllegalStateException("file 2 failed");
            }
            return Stream.of("f" + index);
        };
        List<String> delivered = new ArrayList<>();
        assertThatThrownBy(() -> {
                    try (Stream<String> merged =
                            ConcurrentFileMerge.stream(3, openFile, element -> {}, 3, Emission.SURVIVOR_ORDER)) {
                        merged.forEach(delivered::add);
                    }
                })
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("file 2 failed");
        assertThat(delivered).containsExactly("f0", "f1");
    }

    @Test
    void earlyCloseDiscardsUndeliveredElementsAndStopsProducers() throws Exception {
        int fileCount = 8;
        Set<String> discarded = ConcurrentHashMap.newKeySet();
        Set<String> produced = ConcurrentHashMap.newKeySet();
        IntFunction<Stream<String>> openFile =
                index -> Stream.of("f" + index + "e0", "f" + index + "e1").peek(produced::add);
        List<String> delivered = new ArrayList<>();
        try (Stream<String> merged =
                ConcurrentFileMerge.stream(fileCount, openFile, discarded::add, 4, Emission.UNORDERED)) {
            Iterator<String> iterator = merged.iterator();
            delivered.add(iterator.next());
            delivered.add(iterator.next());
        }
        for (String element : produced) {
            boolean accounted = delivered.contains(element) || discarded.contains(element);
            assertThat(accounted)
                    .as("element %s must be delivered or discarded, never dropped", element)
                    .isTrue();
        }
    }

    @Test
    void perFileStreamsAreClosedOnTheirProducer() {
        AtomicInteger closes = new AtomicInteger();
        IntFunction<Stream<String>> openFile = index -> Stream.of("f" + index).onClose(closes::incrementAndGet);
        try (Stream<String> merged = ConcurrentFileMerge.stream(5, openFile, element -> {}, 2, Emission.UNORDERED)) {
            assertThat(merged.count()).isEqualTo(5);
        }
        assertThat(closes).hasValue(5);
    }

    @Test
    void kOneBehavesSequentially() {
        List<List<String>> files = List.of(List.of("a0", "a1"), List.of("b0"), List.of("c0"));
        assertThat(drain(files, Emission.UNORDERED, 1)).containsExactly("a0", "a1", "b0", "c0");
    }

    private static List<String> drain(List<List<String>> files, Emission emission, int k) {
        try (Stream<String> merged =
                ConcurrentFileMerge.stream(files.size(), filesOf(files), element -> {}, k, emission)) {
            return merged.toList();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            boolean released = latch.await(10, TimeUnit.SECONDS);
            if (!released) {
                throw new IllegalStateException("latch was not released in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
