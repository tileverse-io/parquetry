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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * The metadata text reads, the version hint and the metadata document, hand the storage backend a buffer that the
 * backend may fill from its own I/O thread, as the streaming cloud readers do. A thread-confined buffer fails on that
 * thread, and the Azure SDK turns the failure into a silent timeout of a minute per read.
 */
class IcebergMetadataTextReadTest {

    private static final String TABLE = "v3_minimal";

    @TempDir
    Path tempDir;

    private Path tableDir;
    private String tableLocation;

    @BeforeEach
    void extractTableWithAVersionHint() throws IOException {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        tableDir = root.resolve(TABLE);
        Files.writeString(tableDir.resolve("metadata").resolve("version-hint.text"), "1");
        tableLocation = tableDir.toUri().toString();
    }

    @Test
    void resolvesTheVersionHintThroughABackendFillingTheBufferFromAnotherThread() {
        try (ThreadHoppingFileIO io = openTableIo()) {
            String resolved = IcebergMetadataResolver.resolve(io, tableLocation, IcebergOptions.defaults());
            assertThat(resolved).endsWith("/metadata/v1.metadata.json");
            assertThat(io.failures).isEmpty();
        }
    }

    @Test
    void readsTheMetadataDocumentThroughABackendFillingTheBufferFromAnotherThread() {
        try (ThreadHoppingFileIO io = openTableIo()) {
            IcebergTableMetadata metadata =
                    IcebergTableCatalog.resolveMetadata(io, tableLocation, IcebergOptions.defaults());
            assertThat(metadata.fields()).isNotEmpty();
            assertThat(io.failures).isEmpty();
        }
    }

    private ThreadHoppingFileIO openTableIo() {
        IcebergFileIO local = StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), tableLocation);
        return new ThreadHoppingFileIO(local);
    }

    /**
     * A file IO whose sources fill the caller's buffer from a separate thread, as a streaming backend does. A failure
     * on that thread is recorded and rethrown, where a real backend may swallow it.
     */
    private static final class ThreadHoppingFileIO implements IcebergFileIO {

        private final IcebergFileIO delegate;
        final List<Throwable> failures = new CopyOnWriteArrayList<>();

        ThreadHoppingFileIO(IcebergFileIO delegate) {
            this.delegate = delegate;
        }

        @Override
        public ByteRangeSource open(String location) {
            return new ThreadHoppingSource(delegate.open(location));
        }

        @Override
        public List<String> list(String prefix) {
            return delegate.list(prefix);
        }

        @Override
        public List<String> listMetadataFiles(String rootPrefix) {
            return delegate.listMetadataFiles(rootPrefix);
        }

        @Override
        public void close() {
            delegate.close();
        }

        private final class ThreadHoppingSource implements ByteRangeSource {

            private final ByteRangeSource source;

            ThreadHoppingSource(ByteRangeSource source) {
                this.source = source;
            }

            @Override
            public long size() {
                return source.size();
            }

            @Override
            public int read(long offset, MemorySegment dst) {
                AtomicInteger count = new AtomicInteger();
                AtomicReference<Throwable> failure = new AtomicReference<>();
                Thread filler = new Thread(
                        () -> {
                            try {
                                count.set(source.read(offset, dst));
                            } catch (Throwable t) {
                                failure.set(t);
                            }
                        },
                        "buffer-filler");
                filler.start();
                join(filler);
                if (failure.get() != null) {
                    failures.add(failure.get());
                    throw new IllegalStateException("read failed on the filling thread", failure.get());
                }
                return count.get();
            }

            @Override
            public void close() {
                source.close();
            }

            private static void join(Thread thread) {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("interrupted waiting for the filling thread", e);
                }
            }
        }
    }
}
