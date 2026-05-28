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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Properties;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

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
         * Adapts the open reader to the parquetry read SPI. The returned source borrows the reader, so this
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
