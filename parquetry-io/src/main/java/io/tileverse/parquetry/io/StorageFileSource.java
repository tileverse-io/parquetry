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
package io.tileverse.parquetry.io;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageConfig;
import io.tileverse.storage.StorageEntry;
import io.tileverse.storage.StorageException;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.StoragePattern;
import io.tileverse.storage.file.FileStorageProvider;

/**
 * The {@link FileSource} over a tileverse {@link Storage}. A local directory, a local file and a remote URI are listed
 * through a Storage opened here; {@link FileSource#over} and {@link FileSource#object} are listed through a Storage
 * opened by the caller.
 *
 * <p>Whoever opens the Storage closes it. The entries of a local Storage opened here own their readers; the file is
 * released when the byte source closes. The entries of a Storage opened by the caller, and of a remote Storage opened
 * here, borrow their readers, because closing a caching remote reader drops the process-shared range cache for that
 * object.
 */
final class StorageFileSource implements FileSource {

    private final Storage storage;
    private final Listing listing;
    private final Ownership ownership;

    private StorageFileSource(Storage storage, Listing listing, Ownership ownership) {
        this.storage = storage;
        this.listing = listing;
        this.ownership = ownership;
    }

    /** @see FileSource#directory(Path, String) */
    static FileSource directory(Path directory, String glob) {
        Objects.requireNonNull(directory, "directory");
        requireListableGlob(glob);
        Path root = directory.toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            return new AbsentRoot(root.toUri());
        }
        return new StorageFileSource(openLocalStorage(root), new Listing.Matching(glob), Ownership.OPENED_LOCAL);
    }

    /** @see FileSource#file(Path) */
    static FileSource file(Path file) {
        Objects.requireNonNull(file, "file");
        Path absolute = file.toAbsolutePath().normalize();
        Path parent = absolute.getParent();
        if (parent == null) {
            throw new IllegalArgumentException("file has no parent directory: " + file);
        }
        if (!Files.isDirectory(parent)) {
            return new AbsentRoot(parent.toUri());
        }
        Listing named = new Listing.NamedObject(absolute.getFileName().toString());
        return new StorageFileSource(openLocalStorage(parent), named, Ownership.OPENED_LOCAL);
    }

    /** @see FileSource#over(Storage, String) */
    static FileSource over(Storage storage, String pattern) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(pattern, "pattern");
        return new StorageFileSource(storage, new Listing.Matching(pattern), Ownership.SUPPLIED);
    }

    /** @see FileSource#object(Storage, String) */
    static FileSource object(Storage storage, String key) {
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(key, "key");
        return new StorageFileSource(storage, new Listing.NamedObject(key), Ownership.SUPPLIED);
    }

    /** @see FileSource#open(URI, String, Properties) */
    static FileSource open(URI baseUri, String glob, Properties properties) {
        Objects.requireNonNull(baseUri, "baseUri");
        Objects.requireNonNull(glob, "glob");
        Objects.requireNonNull(properties, "properties");
        if (isLocal(baseUri)) {
            return directory(toLocalPath(baseUri), glob);
        }
        Storage storage = StorageFactory.open(baseUri, properties);
        return overOpened(storage, glob);
    }

    /** @see FileSource#openObject(URI, Properties) */
    static FileSource openObject(URI objectUri, Properties properties) {
        Objects.requireNonNull(objectUri, "objectUri");
        Objects.requireNonNull(properties, "properties");
        if (isLocal(objectUri)) {
            return file(toLocalPath(objectUri));
        }
        String key = objectKey(objectUri);
        URI container = objectUri.resolve(".");
        Storage storage = StorageFactory.open(container, properties);
        return objectOpened(storage, key);
    }

    /**
     * A source over the entries of {@code storage} matching {@code pattern}, for a Storage opened here for a remote
     * URI. Package-private for its test.
     */
    static FileSource overOpened(Storage storage, String pattern) {
        return new StorageFileSource(storage, new Listing.Matching(pattern), Ownership.OPENED_REMOTE);
    }

    /**
     * A source over the single object of {@code storage} at {@code key}, for a Storage opened here for a remote URI.
     * Package-private for its test.
     */
    static FileSource objectOpened(Storage storage, String key) {
        return new StorageFileSource(storage, new Listing.NamedObject(key), Ownership.OPENED_REMOTE);
    }

    @Override
    public URI root() {
        return storage.baseUri();
    }

    @Override
    public Stream<FileEntry> list() {
        return switch (listing) {
            case Listing.Matching(String pattern) -> listMatching(pattern);
            case Listing.NamedObject(String key) -> listNamedObject(key);
        };
    }

    private Stream<FileEntry> listMatching(String pattern) {
        return storage.list(pattern)
                .filter(StorageEntry.File.class::isInstance)
                .map(StorageEntry.File.class::cast)
                .map(this::toFileEntry);
    }

    /** The one entry at {@code key}, resolved by a metadata lookup rather than by listing the prefix around it. */
    private Stream<FileEntry> listNamedObject(String key) {
        Optional<StorageEntry.File> found = storage.stat(key);
        return found.map(this::toFileEntry).stream();
    }

    private FileEntry toFileEntry(StorageEntry.File entry) {
        return new StorageFileEntry(storage, entry, ownership.entriesOwnReaders());
    }

    @Override
    public void close() {
        if (!ownership.closesStorage()) {
            return;
        }
        try {
            storage.close();
        } catch (IOException e) {
            throw new UncheckedIOException("Closing the storage at " + storage.baseUri(), e);
        }
    }

    private static boolean isLocal(URI uri) {
        String scheme = uri.getScheme();
        return scheme == null || "file".equals(scheme);
    }

    private static Path toLocalPath(URI uri) {
        if ("file".equals(uri.getScheme())) {
            return Path.of(uri);
        }
        String path = uri.getPath();
        return Path.of(path != null ? path : uri.toString());
    }

    /**
     * The object key of a remote URI: its final path segment, percent-decoded (e.g. {@code %20} back to a space) to the
     * real key expected by the Storage backend. Package-private for its test.
     */
    static String objectKey(URI objectUri) {
        String path = objectUri.getPath();
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("object URI has no path segment: " + objectUri);
        }
        return path.substring(path.lastIndexOf('/') + 1);
    }

    /**
     * Rejects a glob refused by the listing, when the source is built rather than when it first lists: an empty glob,
     * an absolute one, one with a {@code .} or {@code ..} segment or a NUL character, and one with a character class
     * rejected by the parser. A storage listing takes the empty pattern as a request for the whole root; a directory
     * source names a glob, and an empty one is a mistake.
     */
    private static void requireListableGlob(String glob) {
        Objects.requireNonNull(glob, "glob");
        if (glob.isEmpty()) {
            throw new IllegalArgumentException("glob must not be empty");
        }
        Storage.requireSafePattern(glob);
        // Parsing compiles the character classes and reports an illegal range here instead of at the first listing.
        // The listing parses the glob again, from the string kept by this source.
        StoragePattern.parse(glob);
    }

    /**
     * A Storage over {@code directory}. Its readers hold their file from the first read until they close. The
     * provider's default releases the file after a minute of inactivity and reopens it by real path on the next read,
     * serving the bytes of whatever file sits at that path by then.
     */
    private static Storage openLocalStorage(Path directory) {
        return new FileStorageProvider().createStorage(localStorageConfig(directory));
    }

    /** The configuration of the Storage opened over a local {@code directory}. Package-private for its test. */
    static StorageConfig localStorageConfig(Path directory) {
        StorageConfig config = new StorageConfig(directory.toUri());
        return config.setParameter(FileStorageProvider.FILE_IDLE_TIMEOUT, Duration.ZERO);
    }

    /** Who opened the {@link Storage} behind a source: this decides who closes it and who closes its readers. */
    private enum Ownership {
        /** A local Storage opened by the source: its close releases the Storage, and its entries own their readers. */
        OPENED_LOCAL,
        /**
         * A remote Storage opened by the source: its close releases the Storage, and its entries borrow their readers.
         */
        OPENED_REMOTE,
        /** Opened by the caller: its close leaves the Storage open, and its entries borrow their readers. */
        SUPPLIED;

        boolean closesStorage() {
            return this != SUPPLIED;
        }

        boolean entriesOwnReaders() {
            return this == OPENED_LOCAL;
        }
    }

    /** What {@link #list()} reports: the entries matching a pattern, or the one entry at a known key. */
    private sealed interface Listing {

        record Matching(String pattern) implements Listing {}

        record NamedObject(String key) implements Listing {}
    }

    /**
     * A source over a local root that is not an existing directory. It lists nothing, as a storage listing answers for
     * a prefix naming nothing.
     */
    private record AbsentRoot(URI root) implements FileSource {

        @Override
        public Stream<FileEntry> list() {
            return Stream.empty();
        }

        @Override
        public void close() {
            // no Storage was opened
        }
    }

    private record StorageFileEntry(Storage storage, StorageEntry.File entry, boolean ownsReader) implements FileEntry {

        @Override
        public String relativePath() {
            return entry.key();
        }

        @Override
        public long sizeBytes() {
            return entry.size();
        }

        @Override
        public ByteRangeSource open() {
            RangeReader reader = openReader();
            return ownsReader ? ByteRangeSource.owning(reader) : ByteRangeSource.of(reader);
        }

        /** The storage's reader for this entry, with its failures reported as the I/O failures promised here. */
        private RangeReader openReader() {
            try {
                return storage.openRangeReader(entry);
            } catch (StorageException e) {
                throw RangeReaderByteRangeSource.translate(e, "Could not open " + entry.key(), entry.key());
            }
        }
    }
}
