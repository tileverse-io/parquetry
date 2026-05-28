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
package io.tileverse.parquetry.tileverse;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Stream;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

/**
 * A {@link FileSource} backed by a tileverse {@link Storage}. Lists blobs matching a shell-style glob pattern and opens
 * per-blob byte sources via the Storage's range reader.
 *
 * <p>Two creation paths:
 *
 * <ul>
 *   <li>{@link #over(Storage, String)} - borrows an existing Storage; {@link #close()} is a no-op (the caller closes
 *       the Storage).
 *   <li>{@link #open(URI, String, Properties)} - opens a Storage for the given URI; {@link #close()} closes it.
 * </ul>
 */
public final class StorageFileSource implements FileSource {

    private final Storage storage;
    private final String pattern;
    private final boolean ownsStorage;

    private StorageFileSource(Storage storage, String pattern, boolean ownsStorage) {
        this.storage = storage;
        this.pattern = pattern;
        this.ownsStorage = ownsStorage;
    }

    /**
     * Returns a source over an existing {@code storage}. {@link #close()} does NOT close it; the caller retains
     * ownership of the Storage lifecycle.
     */
    public static StorageFileSource over(Storage storage, String pattern) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(pattern, "pattern");
        return new StorageFileSource(storage, pattern, false);
    }

    /**
     * Opens a {@link Storage} for {@code baseUri} with {@code props} and returns a source over it. {@link #close()}
     * closes the Storage.
     */
    public static StorageFileSource open(URI baseUri, String pattern, Properties props) {
        Objects.requireNonNull(baseUri, "baseUri");
        Objects.requireNonNull(pattern, "pattern");
        Storage storage = StorageFactory.open(baseUri, props);
        return new StorageFileSource(storage, pattern, true);
    }

    @Override
    public URI root() {
        return storage.baseUri();
    }

    @Override
    public Stream<FileEntry> list() {
        return storage.list(pattern)
                .filter(StorageEntry.File.class::isInstance)
                .map(StorageEntry.File.class::cast)
                .map(entry -> new StorageFileEntry(storage, entry.key(), entry.size()));
    }

    /**
     * Releases backend resources when this source owns the Storage (opened via {@link #open}). When the Storage was
     * passed in via {@link #over}, this is a no-op.
     */
    @Override
    public void close() {
        if (!ownsStorage) {
            return;
        }
        try {
            storage.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to close Storage", e);
        }
    }

    private record StorageFileEntry(Storage storage, String key, long sizeBytes) implements FileEntry {

        @Override
        public String relativePath() {
            return key;
        }

        @Override
        public ByteRangeSource open() {
            return ByteRangeSources.from(storage.openRangeReader(key));
        }
    }
}
