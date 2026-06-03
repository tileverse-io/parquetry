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

/**
 * Resolves a CLI path or URI argument into a {@link RangeReader}. A bare path is normalised to an absolute
 * {@code file://} URI; the storage container is the parent directory and the key is the final segment, matching
 * {@link Storage#openRangeReader(URI)}.
 */
public final class UriResolver {

    private UriResolver() {}

    /** Holds the open reader and its backing storage; closing this closes both, reader first. */
    public static final class OpenFile implements AutoCloseable {

        private final Storage storage;
        private final RangeReader reader;
        private ByteRangeSource source;

        OpenFile(Storage storage, RangeReader reader) {
            this.storage = storage;
            this.reader = reader;
        }

        public RangeReader reader() {
            return reader;
        }

        /**
         * Adapts the open reader to the parquetry read SPI. The returned source borrows the reader; this
         * {@link OpenFile} keeps ownership and closes the reader.
         */
        public ByteRangeSource source() {
            if (source == null) {
                source = ByteRangeSources.from(reader);
            }
            return source;
        }

        @Override
        public void close() throws IOException {
            try {
                reader.close();
            } finally {
                storage.close();
            }
        }
    }

    /**
     * Holds the open output stream and its backing storage; closing this closes both. The stream is closed first
     * because closing it commits the write (the local backend renames its temp file into place, cloud backends finish
     * their multipart upload); the storage is then closed even when the stream close fails.
     */
    public static final class OpenSink implements AutoCloseable {

        private final Storage storage;
        private final OutputStream out;

        OpenSink(Storage storage, OutputStream out) {
            this.storage = storage;
            this.out = out;
        }

        public OutputStream out() {
            return out;
        }

        @Override
        public void close() throws IOException {
            try {
                out.close();
            } finally {
                storage.close();
            }
        }
    }

    public static OpenFile open(String pathOrUri) {
        return open(pathOrUri, new Properties());
    }

    /**
     * Opens the path or URI with the given storage properties forwarded to {@link StorageFactory}. The caller supplies
     * only the {@code storage.*} keys it cares about; {@code storage.uri} is set by the factory and must not be
     * included.
     */
    public static OpenFile open(String pathOrUri, Properties storageProperties) {
        URI target = toAbsoluteUri(pathOrUri);
        URI container = target.resolve(".");
        Storage storage = StorageFactory.open(container, storageProperties);
        try {
            RangeReader reader = openReader(storage, target);
            return new OpenFile(storage, reader);
        } catch (RuntimeException e) {
            closeQuietly(storage);
            throw e;
        }
    }

    private static RangeReader openReader(Storage storage, URI target) {
        if ("file".equals(target.getScheme())) {
            // Decode percent-encoding (e.g. %20) back to the real filesystem name before Storage sees it.
            String key = Path.of(target).getFileName().toString();
            return storage.openRangeReader(key);
        }
        return storage.openRangeReader(target);
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
