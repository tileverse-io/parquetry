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
package io.tileverse.parquetry.iceberg;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.catalog.CatalogCapabilities;
import io.tileverse.parquetry.catalog.CatalogCapabilities.SchemaSource;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;

/**
 * Reads one Iceberg table at a pinned snapshot as a {@link ParquetDataset}. The filesystem entry point reads
 * {@code <tableDir>/metadata/<vN>.metadata.json}; data and manifest bytes come through an {@link IcebergFileIO}. The
 * catalog pre-opens every data file's byte source once and owns them for its lifetime; each query opens a short-lived
 * {@link ParquetSource} over only the survivor subset and never closes the shared sources. The catalog closes all
 * sources and the IO in {@link #close()}. Identity-partitioned tables prune files by partition value and reconstruct
 * omitted partition columns; merge-on-read positional and equality deletes are applied per data file, leaving the read
 * to return only the live rows.
 */
public final class IcebergTableCatalog implements DatasetCatalog {

    private final String tableName;
    private final IcebergDataset dataset;
    private final List<ByteRangeSource> openSources;
    private final IcebergFileIO io;

    private IcebergTableCatalog(
            String tableName, IcebergDataset dataset, List<ByteRangeSource> openSources, IcebergFileIO io) {
        this.tableName = tableName;
        this.dataset = dataset;
        this.openSources = openSources;
        this.io = io;
    }

    /**
     * Opens an Iceberg table from a local table directory, resolving the current {@code vN.metadata.json}. The bytes
     * ride a file-backed tileverse {@link Storage}, the same path the object-store backends use.
     */
    public static IcebergTableCatalog openLocal(Path tableDir, IcebergOptions options) {
        Objects.requireNonNull(tableDir, "tableDir");
        Objects.requireNonNull(options, "options");
        String physicalTableLocation = stripTrailingSlash(tableDir.toUri().toString());
        return openStorage(tableName(tableDir), physicalTableLocation, StorageFactory.open(tableDir.toUri()), options);
    }

    /**
     * Opens an Iceberg table whose bytes are served by {@code io} (for example a {@link StorageIcebergFileIO} over
     * object storage). {@code tableLocation} must match the table location recorded in the metadata and the root
     * {@code io} maps. The returned catalog owns {@code io} and closes it in {@link #close()} (a no-op for an IO over a
     * borrowed Storage).
     */
    public static IcebergTableCatalog open(String tableLocation, IcebergFileIO io, IcebergOptions options) {
        Objects.requireNonNull(tableLocation, "tableLocation");
        Objects.requireNonNull(io, "io");
        Objects.requireNonNull(options, "options");
        IcebergTableMetadata metadata = resolveMetadata(io, tableLocation, options);
        return openWithMetadata(tableNameFromLocation(tableLocation), metadata, io);
    }

    /**
     * Opens an Iceberg table whose bytes are served by {@code storage} rooted at the table's physical location,
     * resolving the current {@code vN.metadata.json} and reconciling the table's recorded logical location against the
     * physical storage the same way {@link #openLocal} does for local tables. The returned catalog owns {@code storage}
     * and closes it in {@link #close()}.
     */
    public static IcebergTableCatalog openStorage(String physicalLocation, Storage storage, IcebergOptions options) {
        Objects.requireNonNull(physicalLocation, "physicalLocation");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(options, "options");
        IcebergTableMetadata metadata;
        StorageIcebergFileIO io;
        try {
            StorageIcebergFileIO bootstrap = StorageIcebergFileIO.over(storage, physicalLocation);
            metadata = resolveMetadata(bootstrap, physicalLocation, options);
            io = StorageIcebergFileIO.owning(storage, metadata.tableLocation());
        } catch (RuntimeException failure) {
            closeStorageQuietly(storage, failure);
            throw failure;
        }
        return openWithMetadata(tableNameFromLocation(metadata.tableLocation()), metadata, io);
    }

    /** Opens the table as {@code openStorage} does, presenting it under the given dataset name. */
    static IcebergTableCatalog openStorage(
            String datasetName, String physicalLocation, Storage storage, IcebergOptions options) {
        IcebergTableMetadata metadata;
        StorageIcebergFileIO io;
        try {
            StorageIcebergFileIO bootstrap = StorageIcebergFileIO.over(storage, physicalLocation);
            metadata = resolveMetadata(bootstrap, physicalLocation, options);
            io = StorageIcebergFileIO.owning(storage, metadata.tableLocation());
        } catch (RuntimeException failure) {
            closeStorageQuietly(storage, failure);
            throw failure;
        }
        return openWithMetadata(datasetName, metadata, io);
    }

    private static void closeStorageQuietly(Storage storage, RuntimeException failure) {
        try {
            storage.close();
        } catch (Exception closeFailure) {
            failure.addSuppressed(closeFailure);
        }
    }

    /** Opens a table given parsed metadata and a file IO. The IO is owned by the returned catalog. */
    static IcebergTableCatalog openWithMetadata(String tableName, IcebergTableMetadata metadata, IcebergFileIO io) {
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(metadata, "metadata");
        Objects.requireNonNull(io, "io");
        List<ByteRangeSource> opened = new ArrayList<>();
        try {
            IcebergManifests.Snapshot manifestFiles =
                    IcebergManifests.readSnapshot(metadata.manifestListLocation(), io);
            List<IcebergManifests.DataFileRef> dataFiles = manifestFiles.dataFiles();
            if (dataFiles.isEmpty()) {
                throw new IcebergFormatException("snapshot " + metadata.currentSnapshotId() + " has no data files");
            }
            List<IcebergField> fields = metadata.fields();
            IcebergPartitionSpec partitionSpec = metadata.partitionSpec();
            List<FileStats> fileStats = fileStatsFor(dataFiles, fields, partitionSpec);
            for (IcebergManifests.DataFileRef ref : dataFiles) {
                opened.add(io.open(ref.location()));
            }
            IcebergSchema icebergSchema = IcebergSchema.of(fields);
            IcebergNameMapping nameMapping =
                    metadata.nameMapping().orElseGet(() -> IcebergNameMapping.fromSchema(fields));
            CatalogSnapshot snapshot =
                    new CatalogSnapshot(metadata.currentSnapshotId(), metadata.currentSnapshotTimestampMs());
            IcebergDeletePlan deletePlan =
                    IcebergDeletePlan.of(manifestFiles.deleteFiles(), io, icebergSchema, nameMapping);
            IcebergDataset dataset = new IcebergDataset(
                    tableName,
                    snapshot,
                    icebergSchema,
                    partitionSpec,
                    dataFiles,
                    fileStats,
                    opened,
                    deletePlan,
                    metadata.formatVersion(),
                    nameMapping,
                    OpenOptions.DEFAULTS);
            return new IcebergTableCatalog(tableName, dataset, opened, io);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(opened, io);
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /** Resolves and parses the table's current metadata document through {@code bootstrap}. */
    static IcebergTableMetadata resolveMetadata(
            IcebergFileIO bootstrap, String physicalLocation, IcebergOptions options) {
        String metadataLocation = IcebergMetadataResolver.resolve(bootstrap, physicalLocation, options);
        String json = readJson(bootstrap, metadataLocation);
        return IcebergTableMetadata.read(json, options);
    }

    @Override
    public CatalogCapabilities capabilities() {
        return CatalogCapabilities.builder()
                .enumeratesDatasets(false)
                .timeTravel(true)
                .schemaSource(SchemaSource.TABLE_METADATA)
                .build();
    }

    @Override
    public List<String> datasets() {
        return List.of(tableName);
    }

    @Override
    public ParquetDataset dataset(String name) {
        if (!tableName.equals(name)) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have '" + tableName + "')");
        }
        return dataset;
    }

    @Override
    public void close() {
        RuntimeException failure = closeAll(openSources, io);
        if (failure != null) {
            throw failure;
        }
    }

    private static List<FileStats> fileStatsFor(
            List<IcebergManifests.DataFileRef> dataFiles, List<IcebergField> fields, IcebergPartitionSpec spec) {
        List<FileStats> stats = new ArrayList<>(dataFiles.size());
        for (IcebergManifests.DataFileRef ref : dataFiles) {
            Map<Integer, Value> partitionConstants = IcebergPartitionValues.constantsFor(spec, ref.partitionValues());
            stats.add(IcebergFileStats.from(ref, fields, partitionConstants));
        }
        return stats;
    }

    private static String readJson(IcebergFileIO io, String location) {
        try (ByteRangeSource source = io.open(location)) {
            long size = source.size();
            try (Arena arena = Arena.ofConfined()) {
                MemorySegment segment = arena.allocate(size);
                source.readFully(0, segment);
                byte[] bytes = segment.toArray(ValueLayout.JAVA_BYTE);
                return new String(bytes, StandardCharsets.UTF_8);
            }
        }
    }

    private static String tableName(Path tableDir) {
        Path fileName = tableDir.getFileName();
        return fileName == null ? "table" : fileName.toString();
    }

    private static String tableNameFromLocation(String tableLocation) {
        String trimmed = stripTrailingSlash(tableLocation);
        int lastSlash = trimmed.lastIndexOf('/');
        String lastSegment = lastSlash < 0 ? trimmed : trimmed.substring(lastSlash + 1);
        return lastSegment.isEmpty() ? "table" : lastSegment;
    }

    private static String stripTrailingSlash(String value) {
        return value.endsWith("/") ? value.substring(0, value.length() - 1) : value;
    }

    private static RuntimeException closeAll(List<ByteRangeSource> sources, IcebergFileIO io) {
        RuntimeException failure = null;
        for (ByteRangeSource source : sources) {
            failure = closeChaining(source, failure);
        }
        return closeChaining(io, failure);
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
}
