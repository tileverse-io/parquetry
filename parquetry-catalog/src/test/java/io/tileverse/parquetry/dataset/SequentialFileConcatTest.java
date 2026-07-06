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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

/**
 * Pins the sequential per-file concat's lifecycle: exactly one per-file stream open at a time, opened lazily, closed
 * only when the concat advances to the next file or the returned stream closes - never while the consumer may still
 * read the last element it was handed.
 */
class SequentialFileConcatTest {

    private static final class FileStreams {
        final List<Integer> opened = new ArrayList<>();
        final List<Integer> closed = new ArrayList<>();

        Stream<String> open(int file, int elements) {
            opened.add(file);
            Stream<String> stream =
                    java.util.stream.IntStream.range(0, elements).mapToObj(i -> file + ":" + i);
            return stream.onClose(() -> closed.add(file));
        }
    }

    @Test
    void opensLazilyAndClosesOnlyWhenAdvancingToTheNextFile() {
        FileStreams files = new FileStreams();
        try (Stream<String> all = SequentialFileConcat.stream(3, f -> files.open(f, 2))) {
            Iterator<String> it = all.iterator();
            assertThat(files.opened).isEmpty();

            assertThat(it.next()).isEqualTo("0:0");
            assertThat(files.opened).containsExactly(0);

            assertThat(it.next()).isEqualTo("0:1");
            assertThat(files.closed)
                    .as("file 0's last element was delivered; its stream must still be open")
                    .isEmpty();

            assertThat(it.next()).isEqualTo("1:0");
            assertThat(files.closed).as("advancing into file 1 closes file 0").containsExactly(0);

            while (it.hasNext()) {
                it.next();
            }
            assertThat(files.closed).containsExactly(0, 1, 2);
        }
    }

    @Test
    void closingTheStreamEarlyClosesTheOpenFileOnly() {
        FileStreams files = new FileStreams();
        Stream<String> all = SequentialFileConcat.stream(3, f -> files.open(f, 2));
        Iterator<String> it = all.iterator();
        it.next();
        all.close();
        assertThat(files.opened).containsExactly(0);
        assertThat(files.closed).containsExactly(0);
    }

    @Test
    void emptyFilesAreSkippedAndClosed() {
        FileStreams files = new FileStreams();
        try (Stream<String> all = SequentialFileConcat.stream(3, f -> files.open(f, f == 1 ? 0 : 1))) {
            assertThat(all.toList()).containsExactly("0:0", "2:0");
        }
        assertThat(files.closed).containsExactly(0, 1, 2);
    }

    @Test
    void zeroFilesYieldAnEmptyStream() {
        FileStreams files = new FileStreams();
        try (Stream<String> all = SequentialFileConcat.stream(0, f -> files.open(f, 1))) {
            assertThat(all.toList()).isEmpty();
        }
        assertThat(files.opened).isEmpty();
    }
}
