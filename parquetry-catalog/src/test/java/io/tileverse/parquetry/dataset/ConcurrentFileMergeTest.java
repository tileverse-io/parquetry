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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

class ConcurrentFileMergeTest {

    private static IntFunction<Stream<String>> filesOf(List<List<String>> perFile) {
        return index -> perFile.get(index).stream();
    }

    @Test
    void unorderedDeliversEveryElementExactlyOnce() {
        List<List<String>> files = List.of(List.of("a0", "a1", "a2"), List.of("b0"), List.of(), List.of("d0", "d1"));
        List<String> seen;
        try (Stream<String> merged = ConcurrentFileMerge.stream(files.size(), filesOf(files), element -> {}, 3)) {
            seen = merged.toList();
        }
        assertThat(seen).containsExactlyInAnyOrder("a0", "a1", "a2", "b0", "d0", "d1");
    }

    @Test
    void oneFileIsDrainedOnTheCallingThreadWithoutMerging() {
        AtomicInteger closes = new AtomicInteger();
        Stream<String> perFile = Stream.of("a0", "a1").onClose(closes::incrementAndGet);
        AtomicReference<Thread> openedOn = new AtomicReference<>();
        IntFunction<Stream<String>> openFile = index -> {
            openedOn.set(Thread.currentThread());
            return perFile;
        };

        List<String> drained;
        try (Stream<String> merged = ConcurrentFileMerge.stream(1, openFile, element -> {}, 4)) {
            boolean thePerFileStreamItself = merged == perFile;
            assertThat(thePerFileStreamItself)
                    .as("a one-file merge returns the per-file stream itself")
                    .isTrue();
            drained = merged.toList();
        }

        assertThat(drained).containsExactly("a0", "a1");
        assertThat(openedOn).hasValue(Thread.currentThread());
        assertThat(closes)
                .as("closing the returned stream closes the per-file stream")
                .hasValue(1);
    }

    @Test
    void producerFailurePropagatesUnwrapped() {
        IntFunction<Stream<String>> openFile = index -> {
            if (index == 1) {
                throw new IllegalStateException("file 1 failed to open");
            }
            return Stream.of("f" + index);
        };
        assertThatThrownBy(() -> drain(3, openFile, 3))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("file 1 failed to open");
    }

    @Test
    void earlyCloseDiscardsUndeliveredElementsAndStopsProducers() {
        int fileCount = 8;
        Set<String> discarded = ConcurrentHashMap.newKeySet();
        Set<String> produced = ConcurrentHashMap.newKeySet();
        IntFunction<Stream<String>> openFile =
                index -> Stream.of("f" + index + "e0", "f" + index + "e1").peek(produced::add);
        List<String> delivered = new ArrayList<>();
        try (Stream<String> merged = ConcurrentFileMerge.stream(fileCount, openFile, discarded::add, 4)) {
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
        try (Stream<String> merged = ConcurrentFileMerge.stream(5, openFile, element -> {}, 2)) {
            assertThat(merged.count()).isEqualTo(5);
        }
        assertThat(closes).hasValue(5);
    }

    @Test
    void kOneBehavesSequentially() {
        List<List<String>> files = List.of(List.of("a0", "a1"), List.of("b0"), List.of("c0"));
        assertThat(drain(files, 1)).containsExactly("a0", "a1", "b0", "c0");
    }

    private static List<String> drain(List<List<String>> files, int k) {
        return drain(files.size(), filesOf(files), k);
    }

    private static List<String> drain(int fileCount, IntFunction<Stream<String>> openFile, int k) {
        List<String> drained = new ArrayList<>();
        drainInto(fileCount, openFile, k, drained);
        return drained;
    }

    private static void drainInto(int fileCount, IntFunction<Stream<String>> openFile, int k, List<String> sink) {
        try (Stream<String> merged = ConcurrentFileMerge.stream(fileCount, openFile, element -> {}, k)) {
            merged.forEach(sink::add);
        }
    }
}
