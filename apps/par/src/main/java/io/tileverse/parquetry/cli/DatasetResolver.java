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
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.dataset.GeoParquetDataset;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.iceberg.IcebergCatalog;
import io.tileverse.parquetry.iceberg.IcebergOptions;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.tileverse.ParquetFileSources;
import io.tileverse.parquetry.tileverse.ParquetStorage;

/**
 * Resolves a CLI path or URI into an open {@link ParquetDataset}, auto-detecting the input kind: a single Parquet file
 * or a directory/glob of same-schema files become one merged {@link FilesetCatalog} dataset; a directory or remote
 * prefix whose {@code metadata/} holds an Iceberg table metadata file becomes an {@link IcebergCatalog} dataset. The
 * returned {@link OpenDataset} owns the catalog and releases it on {@link OpenDataset#close()}.
 *
 * <p>Detection probes the filesystem for local paths and uses the trailing-slash convention for remote prefixes. A
 * remote prefix is probed for an Iceberg metadata marker over its {@link Storage} and opened as an Iceberg table when
 * present; otherwise it is read as a merged Fileset. A single remote object is never an Iceberg table and stays a
 * Fileset.
 */
public final class DatasetResolver {

    private static final String DIRECTORY_GLOB = "**/*.parquet";

    private DatasetResolver() {}

    /** An open dataset and the catalog backing it; {@link #close()} releases the catalog's byte sources. */
    public static final class OpenDataset implements AutoCloseable {

        private final DatasetCatalog catalog;
        private final ParquetDataset dataset;

        private OpenDataset(DatasetCatalog catalog) {
            this.catalog = catalog;
            List<String> names = catalog.datasets();
            this.dataset = catalog.dataset(names.get(0));
        }

        public ParquetDataset dataset() {
            return dataset;
        }

        @Override
        public void close() {
            catalog.close();
        }
    }

    /** Opens the dataset at {@code pathOrUri}, forwarding {@code storageProperties} to remote backends. */
    public static OpenDataset open(String pathOrUri, Properties storageProperties) {
        InputKind kind = classify(pathOrUri);
        DatasetCatalog catalog = buildCatalog(kind, storageProperties);
        try {
            return new OpenDataset(catalog);
        } catch (RuntimeException e) {
            catalog.close();
            throw e;
        }
    }

    /** The GeoParquet metadata of a dataset, or empty when the dataset has none. */
    public static Optional<GeoParquetMetadata> geoMetadataOf(ParquetDataset dataset) {
        if (dataset instanceof GeoParquetDataset geo) {
            return geo.geoMetadata();
        }
        return Optional.empty();
    }

    private static DatasetCatalog buildCatalog(InputKind kind, Properties storageProperties) {
        return switch (kind) {
            case InputKind.IcebergLocal local -> IcebergCatalog.openLocal(local.tableDir(), IcebergOptions.defaults());
            case InputKind.LocalFile local ->
                FilesetCatalog.open(LocalFileSource.file(local.file()), CatalogOptions.defaults());
            case InputKind.Fileset fileset ->
                FilesetCatalog.open(
                        ParquetFileSources.open(fileset.baseUri(), fileset.glob(), storageProperties),
                        CatalogOptions.defaults());
            case InputKind.RemoteObject remote ->
                FilesetCatalog.open(
                        ParquetFileSources.openObject(remote.uri(), storageProperties), CatalogOptions.defaults());
            case InputKind.RemotePrefix remote -> openRemotePrefix(remote.uri(), storageProperties);
        };
    }

    private static DatasetCatalog openRemotePrefix(URI uri, Properties storageProperties) {
        Storage storage = ParquetStorage.open(uri, storageProperties);
        boolean iceberg;
        try {
            iceberg = hasIcebergMetadata(storage);
        } catch (RuntimeException probeFailure) {
            closeQuietly(storage, probeFailure);
            throw probeFailure;
        }
        if (iceberg) {
            // openStorage takes ownership of storage and closes it on failure or on catalog.close().
            return IcebergCatalog.openStorage(tableLocation(uri), storage, IcebergOptions.defaults());
        }
        closeQuietly(storage);
        return FilesetCatalog.open(
                ParquetFileSources.open(uri, DIRECTORY_GLOB, storageProperties), CatalogOptions.defaults());
    }

    /** Whether the prefix holds an Iceberg table: its {@code metadata/} directory has a {@code *.metadata.json}. */
    static boolean hasIcebergMetadata(Storage storage) {
        try (Stream<StorageEntry> entries = storage.list("metadata/*.metadata.json")) {
            return entries.anyMatch(StorageEntry.File.class::isInstance);
        }
    }

    /** The table location passed to IcebergCatalog.openStorage: the prefix URI without a trailing slash. */
    private static String tableLocation(URI uri) {
        String text = uri.toString();
        return text.endsWith("/") ? text.substring(0, text.length() - 1) : text;
    }

    private static InputKind classify(String pathOrUri) {
        if (looksLikeGlob(pathOrUri) && !isExistingRegularFile(pathOrUri)) {
            return globFileset(pathOrUri);
        }
        URI uri = UriResolver.normalizeToUri(pathOrUri);
        if (isLocal(uri)) {
            return classifyLocal(uri);
        }
        return classifyRemote(uri);
    }

    private static InputKind classifyLocal(URI uri) {
        Path path = Path.of(uri);
        if (Files.isRegularFile(path)) {
            return new InputKind.LocalFile(path);
        }
        if (Files.isDirectory(path)) {
            if (isIcebergTableDir(path)) {
                return new InputKind.IcebergLocal(path);
            }
            return new InputKind.Fileset(directoryUri(uri), DIRECTORY_GLOB);
        }
        throw new IllegalArgumentException("no such file or directory: " + path);
    }

    private static InputKind classifyRemote(URI uri) {
        String uriPath = uri.getPath();
        if (uriPath != null && uriPath.endsWith("/")) {
            return new InputKind.RemotePrefix(uri);
        }
        // A single remote object opens directly by key (GET only), without listing its parent prefix (which would
        // require LIST permission a GET-only credential lacks).
        return new InputKind.RemoteObject(uri);
    }

    private static InputKind globFileset(String pathOrUri) {
        int meta = firstGlobMetaIndex(pathOrUri);
        // Split on the last separator of either kind. A Windows argument such as C:\data\*.parquet uses backslashes;
        // the base must stay free of glob characters for normalizeToUri to resolve it to a real path URI.
        int lastSeparator = Math.max(pathOrUri.lastIndexOf('/', meta), pathOrUri.lastIndexOf('\\', meta));
        String base = lastSeparator < 0 ? "." : pathOrUri.substring(0, lastSeparator);
        String glob = lastSeparator < 0 ? pathOrUri : pathOrUri.substring(lastSeparator + 1);
        // GlobMatcher matches against forward-slash-canonical relative paths; canonicalize the glob suffix to match.
        return new InputKind.Fileset(UriResolver.normalizeToUri(base), glob.replace('\\', '/'));
    }

    private static boolean isIcebergTableDir(Path dir) {
        Path metadata = dir.resolve("metadata");
        if (!Files.isDirectory(metadata)) {
            return false;
        }
        try (Stream<Path> entries = Files.list(metadata)) {
            return entries.anyMatch(entry -> entry.getFileName().toString().endsWith(".metadata.json"));
        } catch (IOException e) {
            throw new UncheckedIOException("listing " + metadata, e);
        }
    }

    private static boolean isLocal(URI uri) {
        String scheme = uri.getScheme();
        return scheme == null || "file".equals(scheme);
    }

    private static boolean looksLikeGlob(String value) {
        return firstGlobMetaIndex(value) >= 0;
    }

    private static int firstGlobMetaIndex(String value) {
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '*' || c == '?' || c == '[' || c == '{') {
                return i;
            }
        }
        return -1;
    }

    private static boolean isExistingRegularFile(String pathOrUri) {
        try {
            URI uri = UriResolver.normalizeToUri(pathOrUri);
            return isLocal(uri) && Files.isRegularFile(Path.of(uri));
        } catch (RuntimeException notAPath) {
            return false;
        }
    }

    private static URI directoryUri(URI uri) {
        String text = uri.toString();
        return text.endsWith("/") ? uri : URI.create(text + "/");
    }

    private static void closeQuietly(Storage storage) {
        try {
            storage.close();
        } catch (Exception ignored) {
            // Best-effort: the probe failed cleanly and the storage is being discarded; a close error is noise here.
        }
    }

    private static void closeQuietly(Storage storage, RuntimeException probeFailure) {
        try {
            storage.close();
        } catch (Exception closeFailure) {
            probeFailure.addSuppressed(closeFailure);
        }
    }

    private sealed interface InputKind {
        record Fileset(URI baseUri, String glob) implements InputKind {}

        record LocalFile(Path file) implements InputKind {}

        record IcebergLocal(Path tableDir) implements InputKind {}

        record RemoteObject(URI uri) implements InputKind {}

        record RemotePrefix(URI uri) implements InputKind {}
    }
}
