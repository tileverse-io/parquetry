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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

/**
 * A test {@link FileSource} over files in a directory that counts per-file opens and tracks handed-out sources. The
 * catalog opens files on concurrent virtual threads; the bookkeeping collections are concurrent for that reason.
 */
final class CountingFileSource implements FileSource {

    private final Path dir;
    private final List<String> names;
    private final ConcurrentMap<String, Integer> opens = new ConcurrentHashMap<>();
    private final List<CloseTrackingSource> handedOut = new CopyOnWriteArrayList<>();
    boolean closed;

    CountingFileSource(Path dir, List<String> names) {
        this.dir = dir;
        this.names = names;
    }

    int openCount(String name) {
        return opens.getOrDefault(name, 0);
    }

    int totalOpens() {
        return opens.values().stream().mapToInt(Integer::intValue).sum();
    }

    boolean allOpenedSourcesClosed() {
        return handedOut.stream().allMatch(source -> source.closed);
    }

    @Override
    public URI root() {
        return dir.toUri();
    }

    @Override
    public Stream<FileEntry> list() {
        return names.stream().map(this::countingEntry);
    }

    private FileEntry countingEntry(String name) {
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
                opens.merge(name, 1, Integer::sum);
                CloseTrackingSource source = new CloseTrackingSource(ByteRangeSource.ofFile(dir.resolve(name)));
                handedOut.add(source);
                return source;
            }
        };
    }

    @Override
    public void close() {
        closed = true;
    }

    /** A {@link ByteRangeSource} delegate that records whether it was closed. */
    private static final class CloseTrackingSource implements ByteRangeSource {

        private final ByteRangeSource delegate;
        private volatile boolean closed;

        CloseTrackingSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            return delegate.read(offset, dst);
        }

        @Override
        public void close() {
            closed = true;
            delegate.close();
        }
    }
}
