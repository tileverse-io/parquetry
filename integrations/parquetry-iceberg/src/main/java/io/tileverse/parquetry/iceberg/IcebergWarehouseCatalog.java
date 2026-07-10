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

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.SequencedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.catalog.CatalogCapabilities;
import io.tileverse.parquetry.catalog.CatalogCapabilities.SchemaSource;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.dataset.ParquetDataset;

/**
 * Reads MANY Iceberg tables under one warehouse root as a multi-dataset catalog. Discovery is one recursive listing for
 * {@code <table>/metadata/*.metadata.json} keys - no metadata is read until a table is first accessed. A table found at
 * {@code <root>/ns1/sales/orders} presents as the dataset {@code ns1.sales.orders}. Each {@link #dataset(String)} opens
 * (once, cached) an {@link IcebergTableCatalog} pinned at that table's current snapshot. The explicit-registry
 * factories skip discovery for backends that cannot list or for curated subsets. The warehouse root itself is never
 * treated as a table; a single table dir opens through {@link IcebergTableCatalog}. Dotted dataset names mean a
 * directory literally named {@code ns1.tableA} and a nested {@code ns1/tableA} would collide (the layout convention
 * makes this pathological; last discovered wins). Every table's bytes ride a tileverse {@link Storage}: the local
 * factories ({@link #openLocal} and {@link #ofLocalTables}) open a file-backed Storage over the directory, and the
 * Storage-backed ones ({@link #open(String, Storage)} and {@link #ofTables}) take a caller-supplied Storage.
 * {@link #openLocal} and {@link #open(String, Storage)} own one shared Storage for the whole warehouse and close it in
 * {@link #close()}; {@link #ofLocalTables} gives each table its own file-backed Storage, owned by that table's catalog
 * and closed when the warehouse closes its tables.
 */
public final class IcebergWarehouseCatalog implements DatasetCatalog {

    /** Opens one table under this catalog's dotted dataset name. */
    private interface TableOpener {
        IcebergTableCatalog open(String datasetName);
    }

    private final SequencedMap<String, TableOpener> tables;
    private final ConcurrentHashMap<String, IcebergTableCatalog> opened = new ConcurrentHashMap<>();
    private final Optional<Storage> ownedStorage;
    private final AtomicBoolean closed = new AtomicBoolean();

    private IcebergWarehouseCatalog(SequencedMap<String, TableOpener> tables, Optional<Storage> ownedStorage) {
        this.tables = tables;
        this.ownedStorage = ownedStorage;
    }

    /** Opens a local warehouse directory over a file-backed Storage, discovering every table under it. */
    public static IcebergWarehouseCatalog openLocal(Path warehouseDir) {
        Objects.requireNonNull(warehouseDir, "warehouseDir");
        return open(stripTrailingSlash(warehouseDir.toUri().toString()), StorageFactory.open(warehouseDir.toUri()));
    }

    /**
     * An explicit registry of local table directories; names verbatim, {@code datasets()} in map order. Each table
     * opens its own file-backed Storage on first access, owned by that table's catalog and closed when this warehouse
     * closes its tables.
     */
    public static IcebergWarehouseCatalog ofLocalTables(SequencedMap<String, Path> nameToDir) {
        Objects.requireNonNull(nameToDir, "nameToDir");
        if (nameToDir.isEmpty()) {
            throw new IllegalArgumentException("the table registry must name at least one table");
        }
        SequencedMap<String, TableOpener> tables = new LinkedHashMap<>();
        nameToDir.forEach((name, dir) -> tables.put(
                name,
                datasetName -> IcebergTableCatalog.openStorage(
                        datasetName,
                        stripTrailingSlash(dir.toUri().toString()),
                        StorageFactory.open(dir.toUri()),
                        IcebergOptions.defaults())));
        return new IcebergWarehouseCatalog(tables, Optional.empty());
    }

    /**
     * Opens a warehouse whose bytes are served by {@code storage} rooted at the warehouse's physical location,
     * discovering every table under it. The catalog owns {@code storage} and closes it in {@link #close()}. Ownership
     * transfers on successful return: a null-argument failure leaves the caller owning {@code storage}, while a
     * discovery failure closes it before rethrowing.
     */
    public static IcebergWarehouseCatalog open(String warehouseLocation, Storage storage) {
        Objects.requireNonNull(warehouseLocation, "warehouseLocation");
        Objects.requireNonNull(storage, "storage");
        String root = stripTrailingSlash(warehouseLocation);
        try {
            SequencedMap<String, TableOpener> tables = new LinkedHashMap<>();
            try (StorageIcebergFileIO scanner = StorageIcebergFileIO.over(storage, root)) {
                for (String tablePath : tableDirsOf(scanner.listMetadataFiles(root), root)) {
                    tables.put(datasetName(tablePath), name -> openStorageTable(name, root, tablePath, storage));
                }
            }
            requireTables(tables, root);
            return new IcebergWarehouseCatalog(tables, Optional.of(storage));
        } catch (RuntimeException failure) {
            closeQuietly(storage, failure);
            throw failure;
        }
    }

    /**
     * An explicit registry of tables under {@code storage}: names verbatim, values are WAREHOUSE-RELATIVE table paths
     * (for example {@code ns1/tableA}), {@code datasets()} in map order; no listing is performed, which also serves
     * backends that cannot list. The catalog owns {@code storage} and closes it in {@link #close()}. Ownership
     * transfers on successful return; when argument validation throws, the caller still owns {@code storage}.
     */
    public static IcebergWarehouseCatalog ofTables(
            SequencedMap<String, String> nameToTablePath, String warehouseLocation, Storage storage) {
        Objects.requireNonNull(nameToTablePath, "nameToTablePath");
        Objects.requireNonNull(warehouseLocation, "warehouseLocation");
        Objects.requireNonNull(storage, "storage");
        if (nameToTablePath.isEmpty()) {
            throw new IllegalArgumentException("the table registry must name at least one table");
        }
        String root = stripTrailingSlash(warehouseLocation);
        SequencedMap<String, TableOpener> tables = new LinkedHashMap<>();
        nameToTablePath.forEach((name, tablePath) ->
                tables.put(name, datasetName -> openStorageTable(datasetName, root, tablePath, storage)));
        return new IcebergWarehouseCatalog(tables, Optional.of(storage));
    }

    private static IcebergTableCatalog openStorageTable(
            String datasetName, String warehouseLocation, String tablePath, Storage storage) {
        String relativePath = stripTrailingSlash(stripLeadingSlash(tablePath));
        String physical = warehouseLocation + "/" + relativePath;
        StorageIcebergFileIO bootstrap = StorageIcebergFileIO.over(storage, physical, relativePath);
        IcebergTableMetadata metadata =
                IcebergTableCatalog.resolveMetadata(bootstrap, physical, IcebergOptions.defaults());
        StorageIcebergFileIO io = StorageIcebergFileIO.over(storage, metadata.tableLocation(), relativePath);
        return IcebergTableCatalog.openWithMetadata(datasetName, metadata, io);
    }

    private static void closeQuietly(Storage storage, RuntimeException failure) {
        try {
            storage.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /** The distinct warehouse-relative table paths behind the discovered metadata files, sorted. */
    private static SequencedSet<String> tableDirsOf(List<String> metadataFiles, String warehouseLocation) {
        String base = stripTrailingSlash(warehouseLocation) + "/";
        SequencedSet<String> dirs = new TreeSet<>();
        for (String location : metadataFiles) {
            String relative = location.substring(base.length());
            int metadataAt = relative.lastIndexOf("/metadata/");
            if (metadataAt > 0) {
                dirs.add(relative.substring(0, metadataAt));
            }
        }
        return dirs;
    }

    private static String datasetName(String tablePath) {
        return tablePath.replace('/', '.');
    }

    private static void requireTables(SequencedMap<String, TableOpener> tables, String warehouseLocation) {
        if (tables.isEmpty()) {
            throw new IcebergFormatException("the warehouse at " + warehouseLocation
                    + " is empty or cannot be listed; no <table>/metadata/*.metadata.json found");
        }
    }

    @Override
    public CatalogCapabilities capabilities() {
        return CatalogCapabilities.builder()
                .enumeratesDatasets(true)
                .timeTravel(false)
                .schemaSource(SchemaSource.TABLE_METADATA)
                .build();
    }

    @Override
    public List<String> datasets() {
        return List.copyOf(tables.keySet());
    }

    @Override
    public ParquetDataset dataset(String name) {
        Objects.requireNonNull(name, "name");
        if (closed.get()) {
            throw new IllegalStateException("the catalog is closed");
        }
        TableOpener opener = tables.get(name);
        if (opener == null) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have " + datasets() + ")");
        }
        return opened.computeIfAbsent(name, opener::open).dataset(name);
    }

    /**
     * Closes every opened table catalog and the owned storage, if any. Closing while a {@link #dataset(String)} call is
     * in flight is not supported; callers quiesce reads first.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        RuntimeException failure = null;
        for (IcebergTableCatalog table : opened.values()) {
            failure = closeChaining(table, failure);
        }
        opened.clear();
        if (ownedStorage.isPresent()) {
            failure = closeChaining(ownedStorage.get(), failure);
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeChaining(AutoCloseable closeable, RuntimeException accumulated) {
        try {
            closeable.close();
            return accumulated;
        } catch (RuntimeException e) {
            return chain(accumulated, e);
        } catch (Exception e) {
            return chain(accumulated, new IllegalStateException("closing " + closeable, e));
        }
    }

    private static RuntimeException chain(RuntimeException accumulated, RuntimeException next) {
        if (accumulated == null) {
            return next;
        }
        accumulated.addSuppressed(next);
        return accumulated;
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static String stripLeadingSlash(String value) {
        return value.startsWith("/") ? value.substring(1) : value;
    }
}
