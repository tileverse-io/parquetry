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
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

/**
 * A test {@link FileSource} that injects per-file open faults: a named file's {@link FileEntry#open()} throws a
 * {@link RuntimeException} or an {@link Error}, or blocks on a latch until the test releases (or interrupts) it. Every
 * successfully opened source is close-tracked and counts its reads. The bookkeeping is concurrent because the catalog
 * opens files on concurrent virtual threads.
 */
final class FaultInjectingFileSource implements FileSource {

    private final Path dir;
    private final List<String> names;
    private final Set<String> failingNames;
    private final Set<String> errorNames;
    private final Set<String> blockingNames;
    private final CountDownLatch openReleased = new CountDownLatch(1);
    private final List<CloseTracking> handedOut = new CopyOnWriteArrayList<>();
    private final AtomicLong totalReads = new AtomicLong();

    FaultInjectingFileSource(Path dir, List<String> names, Set<String> failingNames, Set<String> blockingNames) {
        this(dir, names, failingNames, Set.of(), blockingNames);
    }

    FaultInjectingFileSource(
            Path dir, List<String> names, Set<String> failingNames, Set<String> errorNames, Set<String> blockingNames) {
        this.dir = dir;
        this.names = names;
        this.failingNames = failingNames;
        this.errorNames = errorNames;
        this.blockingNames = blockingNames;
    }

    int openedCount() {
        return handedOut.size();
    }

    long totalReads() {
        return totalReads.get();
    }

    boolean allOpenedSourcesClosed() {
        return handedOut.stream().allMatch(source -> source.closed);
    }

    void releaseBlockedOpens() {
        openReleased.countDown();
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
                if (failingNames.contains(name)) {
                    throw new IllegalStateException("injected open failure: " + name);
                }
                if (errorNames.contains(name)) {
                    throw new AssertionError("injected open error: " + name);
                }
                if (blockingNames.contains(name)) {
                    awaitRelease(name);
                }
                CloseTracking source = new CloseTracking(ByteRangeSource.ofFile(dir.resolve(name)));
                handedOut.add(source);
                return source;
            }
        };
    }

    private void awaitRelease(String name) {
        try {
            openReleased.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while blocked opening " + name, e);
        }
    }

    @Override
    public void close() {
        // nothing to release; the byte sources belong to the catalog under test
    }

    /** A {@link ByteRangeSource} delegate that counts reads and records whether it was closed. */
    private final class CloseTracking implements ByteRangeSource {

        private final ByteRangeSource delegate;
        private volatile boolean closed;

        CloseTracking(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            totalReads.incrementAndGet();
            return delegate.read(offset, dst);
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }
    }
}
