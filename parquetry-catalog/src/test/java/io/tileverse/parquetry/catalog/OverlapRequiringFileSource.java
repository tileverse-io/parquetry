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
package io.tileverse.parquetry.catalog;

import java.lang.foreign.MemorySegment;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

/**
 * A test {@link FileSource} that blocks the chosen phase of every file until that phase is in flight for each listed
 * file at once. A caller working the files concurrently proceeds as soon as all of them overlap; a caller working them
 * one at a time never releases the latch and fails with an {@link IllegalStateException} after a short timeout.
 */
final class OverlapRequiringFileSource implements FileSource {

    /** The per-file phase whose calls must overlap across all listed files. */
    enum Phase {
        /** Every file's {@link FileEntry#open()} must be in flight at once. */
        OPEN,
        /** Every file's first {@link ByteRangeSource#read} must be in flight at once. */
        FIRST_READ
    }

    private static final long OVERLAP_TIMEOUT_SECONDS = 5;

    private final Path dir;
    private final List<String> names;
    private final Phase phase;
    private final CountDownLatch phasePerFile;

    OverlapRequiringFileSource(Path dir, List<String> names, Phase phase) {
        this.dir = dir;
        this.names = names;
        this.phase = phase;
        this.phasePerFile = new CountDownLatch(names.size());
    }

    @Override
    public URI root() {
        return dir.toUri();
    }

    @Override
    public Stream<FileEntry> list() {
        return names.stream().map(this::entry);
    }

    private FileEntry entry(String name) {
        return new FileEntry() {
            @Override
            public String relativePath() {
                return name;
            }

            @Override
            public long sizeBytes() {
                return -1;
            }

            @Override
            public ByteRangeSource open() {
                if (phase == Phase.OPEN) {
                    phasePerFile.countDown();
                    awaitEveryFile("opens");
                }
                return new OverlapAwaitingSource(ByteRangeSource.ofFile(dir.resolve(name)));
            }
        };
    }

    private void awaitEveryFile(String what) {
        try {
            if (!phasePerFile.await(OVERLAP_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "file " + what + " did not overlap: the catalog works the files sequentially");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted awaiting overlapping file " + what, e);
        }
    }

    @Override
    public void close() {
        // nothing to release; the byte sources belong to the catalog under test
    }

    /** A {@link ByteRangeSource} delegate whose reads wait until every file's first read is in flight. */
    private final class OverlapAwaitingSource implements ByteRangeSource {

        private final ByteRangeSource delegate;
        private final AtomicBoolean firstReadCounted = new AtomicBoolean();

        OverlapAwaitingSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            if (phase == Phase.FIRST_READ) {
                if (firstReadCounted.compareAndSet(false, true)) {
                    phasePerFile.countDown();
                }
                awaitEveryFile("footer reads");
            }
            return delegate.read(offset, dst);
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
