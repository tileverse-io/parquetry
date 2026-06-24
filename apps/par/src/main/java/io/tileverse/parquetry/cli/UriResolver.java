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
package io.tileverse.parquetry.cli;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;
import io.tileverse.storage.WriteOptions;

import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.tileverse.ByteRangeSources;
import io.tileverse.parquetry.tileverse.ParquetStorage;

/**
 * Resolves a CLI path or URI argument into a {@link RangeReader}. A bare path is normalised to an absolute
 * {@code file://} URI; the storage container is the parent directory and the key is the final segment, matching
 * {@link Storage#openRangeReader(URI)}.
 */
public final class UriResolver {

    private UriResolver() {}

    /**
     * Holds the open {@link ByteRangeSource} and, for a remote URI, the tileverse-storage handles backing it. A local
     * file is read through parquetry's native source, which owns its channel; a remote URI is read through a
     * tileverse-storage {@link RangeReader} that the source borrows. Closing releases whichever backs this source.
     */
    public static final class OpenFile implements AutoCloseable {

        private final ByteRangeSource source;
        private final Storage storage;
        private final RangeReader reader;

        private OpenFile(ByteRangeSource source, Storage storage, RangeReader reader) {
            this.source = source;
            this.storage = storage;
            this.reader = reader;
        }

        /** A local file read through parquetry's native source; {@link #close()} closes the source's channel. */
        static OpenFile ofLocal(ByteRangeSource source) {
            return new OpenFile(source, null, null);
        }

        /**
         * A remote URI read through tileverse-storage; the source borrows {@code reader}, closed with {@code storage}.
         */
        static OpenFile ofStorage(Storage storage, RangeReader reader) {
            return new OpenFile(ByteRangeSources.from(reader), storage, reader);
        }

        /** The open source, adapted to the parquetry read SPI. */
        public ByteRangeSource source() {
            return source;
        }

        @Override
        public void close() throws IOException {
            if (reader == null) {
                source.close();
                return;
            }
            try {
                reader.close();
            } finally {
                storage.close();
            }
        }
    }

    /**
     * Holds the open output stream and its backing storage, with a commit-on-success contract. Closing the stream is
     * what commits the write (the local backend renames its temp file into place, cloud backends finish their multipart
     * upload). The caller commits ONLY after the writer has finalized the footer, by calling {@link #commit()}. On any
     * failure the caller calls {@link #abort()}, which closes the storage without committing the stream, leaving no
     * visible partial object or file.
     *
     * <p>{@link #close()} is an abort: it is the safe default when a try-with-resources unwinds without an explicit
     * commit (for instance because the write loop threw).
     */
    public static final class OpenSink implements AutoCloseable {

        private final Storage storage;
        private final OutputStream out;
        private boolean committed;
        private boolean closed;

        OpenSink(Storage storage, OutputStream out) {
            this.storage = storage;
            this.out = out;
        }

        public OutputStream out() {
            return out;
        }

        /**
         * Commits the write: closes the stream (which makes the destination visible), then closes the storage. Call
         * only after the writer has successfully finalized the file.
         */
        public void commit() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            committed = true;
            try {
                out.close();
            } finally {
                storage.close();
            }
        }

        /**
         * Aborts the write: closes the storage without committing the stream, leaving no visible destination object or
         * file. Best-effort and idempotent.
         */
        public void abort() {
            if (closed) {
                return;
            }
            closed = true;
            closeQuietly(storage);
        }

        /** Defaults to {@link #abort()} so an unwinding try-with-resources never commits a footerless destination. */
        @Override
        public void close() {
            if (committed) {
                return;
            }
            abort();
        }
    }

    public static OpenFile open(String pathOrUri) {
        return open(pathOrUri, new Properties());
    }

    /**
     * Opens the path or URI. A local {@code file} URI is read through parquetry's native filesystem source; a remote
     * URI is opened through tileverse-storage with the given {@code storage.*} properties (and parquetry's cache
     * tuning, see {@link ParquetStorage}). {@code storage.uri} is set by the factory and must not be included.
     */
    public static OpenFile open(String pathOrUri, Properties storageProperties) {
        URI target = toAbsoluteUri(pathOrUri);
        if ("file".equals(target.getScheme())) {
            Path path = Path.of(target);
            requireNotDirectory(path);
            return OpenFile.ofLocal(ByteRangeSource.ofFile(path));
        }
        URI container = target.resolve(".");
        Storage storage = ParquetStorage.open(container, storageProperties);
        try {
            RangeReader reader = storage.openRangeReader(target);
            return OpenFile.ofStorage(storage, reader);
        } catch (RuntimeException e) {
            closeQuietly(storage);
            throw e;
        }
    }

    /**
     * Guards the single-file read commands against a directory argument, which would otherwise fail with a low-level
     * read error when the directory's bytes are parsed as a Parquet footer.
     */
    private static void requireNotDirectory(Path path) {
        if (Files.isDirectory(path)) {
            throw new IllegalArgumentException("this command operates on a single file, not a directory: " + path);
        }
    }

    /**
     * Opens a writable destination for the named source file, with the given storage properties forwarded to
     * {@link StorageFactory}. A bare path is normalised to an absolute {@code file://} URI; a trailing-slash or
     * existing local directory destination writes {@code sourceFileName} inside it, otherwise the final path segment is
     * the object key. Closing the returned {@link OpenSink} commits the write.
     *
     * @param dst the destination path or URI
     * @param sourceFileName the key to use when {@code dst} denotes a directory
     * @param overwrite whether an existing destination object may be replaced
     * @param dstProps storage properties for the destination, or {@code null} for none
     * @throws IllegalArgumentException if the destination is read-only, or already exists and {@code overwrite} is
     *     false
     */
    public static OpenSink openForWrite(String dst, String sourceFileName, boolean overwrite, Properties dstProps) {
        Target target = deriveTarget(dst, sourceFileName);
        Properties properties = dstProps != null ? dstProps : new Properties();
        Storage storage = StorageFactory.open(target.container(), properties);
        try {
            requireWritable(storage, dst);
            failIfDestinationExists(storage, target.key(), dst, overwrite);
            OutputStream out = storage.openOutputStream(target.key(), writeOptions(overwrite));
            return new OpenSink(storage, out);
        } catch (RuntimeException e) {
            closeQuietly(storage);
            throw e;
        }
    }

    private static void requireWritable(Storage storage, String dst) {
        if (!storage.capabilities().writes()) {
            throw new IllegalArgumentException("destination is read-only: " + dst);
        }
    }

    private static void failIfDestinationExists(Storage storage, String key, String dst, boolean overwrite) {
        if (!overwrite && storage.exists(key)) {
            throw new IllegalArgumentException("destination exists (use -f to overwrite): " + dst);
        }
    }

    private static WriteOptions writeOptions(boolean overwrite) {
        return WriteOptions.builder()
                .ifNotExists(!overwrite)
                .contentType("application/octet-stream")
                .build();
    }

    /**
     * Normalises a CLI path or URI argument to an absolute URI, the same way {@link #open} and {@link #openForWrite}
     * do. A bare path becomes an absolute {@code file://} URI; an argument that already has a scheme is returned as
     * given. Exposed so callers can compare a source and a destination on equal footing.
     */
    public static URI normalizeToUri(String pathOrUri) {
        return toAbsoluteUri(pathOrUri);
    }

    /**
     * Returns the absolute URI of the object {@code openForWrite} would write for the given destination, with
     * {@code sourceFileName} used as the key when {@code dst} denotes a directory. This is the destination container
     * resolved against the object key, on the same footing as {@link #normalizeToUri}, letting a caller detect a
     * self-copy across schemes without reopening any backend.
     */
    public static URI resolvedUri(String dst, String sourceFileName) {
        Target target = deriveTarget(dst, sourceFileName);
        return target.container().resolve(target.key());
    }

    /** A resolved write destination: the storage container and the object key within it. */
    record Target(URI container, String key) {}

    /**
     * Resolves {@code dst} into a storage container and object key, without opening any storage backend. A destination
     * that names a directory (its path ends with {@code /}, or it is an existing local directory) writes
     * {@code sourceFileName} inside that directory; any other destination names the object directly and the final path
     * segment is the key.
     */
    static Target deriveTarget(String dst, String sourceFileName) {
        URI target = toAbsoluteUri(dst);
        if (isDirectory(target)) {
            return new Target(target, sourceFileName);
        }
        return new Target(target.resolve("."), finalSegment(target));
    }

    private static boolean isDirectory(URI target) {
        if (target.getPath() != null && target.getPath().endsWith("/")) {
            return true;
        }
        return isExistingLocalDirectory(target);
    }

    private static boolean isExistingLocalDirectory(URI target) {
        if (!"file".equals(target.getScheme())) {
            return false;
        }
        return Files.isDirectory(Path.of(target));
    }

    private static String finalSegment(URI target) {
        if ("file".equals(target.getScheme())) {
            // Decode percent-encoding (e.g. %20) back to the real filesystem name before Storage sees it.
            return Path.of(target).getFileName().toString();
        }
        String path = target.getPath();
        if (path == null || path.isEmpty()) {
            // An opaque URI such as "s3:bucket/key" (a missing "//") has no hierarchical path to take a key from.
            throw new IllegalArgumentException("destination URI has no path segment: " + target);
        }
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static URI toAbsoluteUri(String pathOrUri) {
        try {
            URI uri = new URI(pathOrUri);
            if (uri.getScheme() != null) {
                return uri;
            }
        } catch (URISyntaxException e) {
            // not a URI; treat it as a filesystem path below
        }
        return Path.of(pathOrUri).toAbsolutePath().normalize().toUri();
    }

    private static void closeQuietly(Storage storage) {
        try {
            storage.close();
        } catch (Exception ignored) {
            // best effort during failed open
        }
    }
}
