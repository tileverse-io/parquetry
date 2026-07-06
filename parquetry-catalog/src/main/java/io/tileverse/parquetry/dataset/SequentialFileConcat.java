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
package io.tileverse.parquetry.dataset;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Lazy one-at-a-time concatenation of per-file streams. Exactly one file's stream is open at any moment: it opens on
 * the pull that first needs it and closes when the concat advances to the next file or the returned stream closes.
 * Elements that are flyweight views (rows over a batch the per-file stream owns) therefore stay valid until the stream
 * advances past them, the same one-pull lifetime the batch-to-row flatten guarantees.
 */
final class SequentialFileConcat {

    private SequentialFileConcat() {}

    static <T> Stream<T> stream(int fileCount, IntFunction<Stream<T>> openFile) {
        ConcatSpliterator<T> spliterator = new ConcatSpliterator<>(fileCount, openFile);
        return StreamSupport.stream(spliterator, /* parallel */ false).onClose(spliterator::closeCurrent);
    }

    private static final class ConcatSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

        private final int fileCount;
        private final IntFunction<Stream<T>> openFile;

        private int nextFile;
        private Stream<T> current;
        private Iterator<T> currentElements;

        ConcatSpliterator(int fileCount, IntFunction<Stream<T>> openFile) {
            super(Long.MAX_VALUE, Spliterator.ORDERED);
            this.fileCount = fileCount;
            this.openFile = openFile;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            while (true) {
                if (current == null) {
                    if (nextFile >= fileCount) {
                        return false;
                    }
                    current = openFile.apply(nextFile);
                    currentElements = current.iterator();
                    nextFile++;
                }
                if (!currentElements.hasNext()) {
                    closeCurrent();
                    continue;
                }
                action.accept(currentElements.next());
                return true;
            }
        }

        void closeCurrent() {
            Stream<T> closing = current;
            current = null;
            currentElements = null;
            if (closing != null) {
                closing.close();
            }
        }
    }
}
