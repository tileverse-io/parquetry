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
package io.tileverse.parquetry.geotools.iceberg;

import java.io.IOException;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageEntry;

import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.geotools.data.StorageParams;
import io.tileverse.parquetry.iceberg.IcebergFormatException;
import io.tileverse.parquetry.iceberg.IcebergOptions;
import io.tileverse.parquetry.iceberg.IcebergTableCatalog;
import io.tileverse.parquetry.iceberg.IcebergWarehouseCatalog;
import io.tileverse.parquetry.tileverse.ParquetStorage;

/** Opens a read-only {@link DataStore} over an Iceberg table or a warehouse of tables. */
public final class IcebergDataStoreFactory implements DataStoreFactorySpi {

    public static final Param ICEBERG_URI = new Param(
            "iceberg",
            String.class,
            "URI of an Iceberg table directory or a warehouse root (local, S3, Azure, GCS, HTTP)",
            true);
    public static final Param NAMESPACE = new Param("namespace", String.class, "Feature type namespace", false);
    public static final Param FID = new Param(
            "fid",
            String.class,
            "Column to use as the feature id (defaults to a column named 'id' when present; otherwise feature ids"
                    + " are synthetic and Id filters are rejected)",
            false);

    @Override
    public String getDisplayName() {
        return "Apache Iceberg";
    }

    @Override
    public String getDescription() {
        return "Read-only Apache Iceberg tables, a single table or a whole warehouse (local, S3, Azure, GCS, HTTP)";
    }

    @Override
    public Param[] getParametersInfo() {
        return StorageParams.withStorageParams(ICEBERG_URI, NAMESPACE, FID);
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        try {
            return ICEBERG_URI.lookUp(params) != null;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        String uriText = (String) ICEBERG_URI.lookUp(params);
        String namespace = (String) NAMESPACE.lookUp(params);
        String fidColumn = (String) FID.lookUp(params);

        DatasetCatalog catalog = openCatalog(uriText, StorageParams.toProperties(params));
        IcebergDataStore store = new IcebergDataStore(catalog);
        if (namespace != null) {
            store.setNamespaceURI(namespace);
        }
        if (fidColumn != null) {
            store.setFidColumn(fidColumn);
        }
        return store;
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException("Iceberg is read-only");
    }

    /**
     * A directory with {@code metadata/*.metadata.json} is one table; anything else is treated as a warehouse root
     * whose tables are discovered recursively. A directory that is neither is reported as such.
     */
    private static DatasetCatalog openCatalog(String uriText, Properties storageProps) throws IOException {
        URI uri = URI.create(uriText);
        if (isLocal(uri)) {
            return openLocal(localPath(uri, uriText), uriText);
        }
        return openRemote(uri, uriText, storageProps);
    }

    private static DatasetCatalog openLocal(Path dir, String uriText) throws IOException {
        if (!Files.isDirectory(dir)) {
            throw new IOException("not a directory: " + uriText);
        }
        if (hasTableMetadata(dir)) {
            return IcebergTableCatalog.openLocal(dir, IcebergOptions.defaults());
        }
        return openLocalWarehouse(dir, uriText);
    }

    /**
     * The warehouse discovery throws when the directory holds no tables; that becomes the "neither a table nor a
     * warehouse" report, naming both interpretations tried.
     */
    private static DatasetCatalog openLocalWarehouse(Path dir, String uriText) throws IOException {
        try {
            return IcebergWarehouseCatalog.openLocal(dir);
        } catch (IcebergFormatException emptyOrUnlistable) {
            throw neitherTableNorWarehouse(uriText, emptyOrUnlistable);
        }
    }

    /**
     * The storage bytes are served over the URI's scheme. Both {@link IcebergTableCatalog#openStorage} and
     * {@link IcebergWarehouseCatalog#open} take ownership of {@code storage} and close it on their own failure; this
     * method closes {@code storage} only when the listing that chooses between them fails first.
     */
    private static DatasetCatalog openRemote(URI uri, String uriText, Properties storageProps) throws IOException {
        Storage storage = ParquetStorage.open(uri, storageProps);
        boolean singleTable;
        try {
            singleTable = hasTableMetadata(storage);
        } catch (RuntimeException listingFailure) {
            closeQuietly(storage, listingFailure);
            throw listingFailure;
        }
        if (singleTable) {
            return IcebergTableCatalog.openStorage(uriText, storage, IcebergOptions.defaults());
        }
        return openRemoteWarehouse(uriText, storage);
    }

    private static DatasetCatalog openRemoteWarehouse(String uriText, Storage storage) throws IOException {
        try {
            return IcebergWarehouseCatalog.open(uriText, storage);
        } catch (IcebergFormatException emptyOrUnlistable) {
            throw neitherTableNorWarehouse(uriText, emptyOrUnlistable);
        }
    }

    private static IOException neitherTableNorWarehouse(String uriText, Throwable cause) {
        return new IOException(
                "neither an Iceberg table (no metadata/*.metadata.json) nor a warehouse containing tables: " + uriText,
                cause);
    }

    private static boolean hasTableMetadata(Path dir) throws IOException {
        Path metadata = dir.resolve("metadata");
        if (!Files.isDirectory(metadata)) {
            return false;
        }
        try (DirectoryStream<Path> entries = Files.newDirectoryStream(metadata, "*.metadata.json")) {
            return entries.iterator().hasNext();
        }
    }

    private static boolean hasTableMetadata(Storage storage) {
        try (Stream<StorageEntry> entries = storage.list("metadata/*.metadata.json")) {
            return entries.anyMatch(StorageEntry.File.class::isInstance);
        }
    }

    private static void closeQuietly(Storage storage, RuntimeException failure) {
        try {
            storage.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    private static boolean isLocal(URI uri) {
        String scheme = uri.getScheme();
        return scheme == null || "file".equals(scheme);
    }

    private static Path localPath(URI uri, String uriText) {
        return uri.getScheme() == null ? Path.of(uriText) : Path.of(uri);
    }
}
