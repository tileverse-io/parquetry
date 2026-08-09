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
package io.tileverse.parquetry.catalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.FilesetDataset;
import io.tileverse.parquetry.dataset.FilesetReader;
import io.tileverse.parquetry.dataset.GeoMetadataAggregator;
import io.tileverse.parquetry.dataset.HivePartitionResolver;
import io.tileverse.parquetry.dataset.HivePartitioning;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * The file-based {@link DatasetCatalog}: it resolves datasets from a {@link FileSource} in one of two groupings.
 * {@link #open} merges every matched file into one {@link FilesetDataset} (all files must share a schema by equality),
 * whether that source is a single file, a directory glob, or a Hive-partitioned tree. {@link #openPerFile} publishes
 * each listed file as its own single-file dataset. Both open forms read no file bytes: dataset names derive from the
 * listing (and {@link CatalogOptions#datasetName()}) alone, and a dataset is built, its footers fetched and decoded,
 * only when {@link #dataset(String)} first resolves it.
 *
 * <p>Resolving the merged dataset reads each file's footer in a gather pass (schema, GeoParquet {@code geo} metadata,
 * row count, and Hive partition values) and again when the all-files dataset opens; composing the already-opened
 * single-file readers into the multi-file dataset is a possible follow-up. From the gathered metadata the partition
 * keys are bound ({@link HivePartitioning#bind}: a key matching a physical column prunes that column, a path-only key
 * is synthesized into an appended column), the per-file {@code geo} metadata is unioned
 * ({@link GeoMetadataAggregator#aggregate}), and one {@link FilesetDataset} is built with per-file partition
 * {@link FileStats} for pruning.
 *
 * <p>Hive {@code key=value} segments are a physical-column pruning aid, never a dataset discriminator: the whole tree
 * is one dataset. The footer reads at merged-dataset resolution are the known scale ceiling, acceptable for moderate
 * file counts.
 *
 * <p>The catalog owns the byte sources it opens and the {@link FileSource}; {@link #close()} releases both.
 */
public final class FilesetCatalog implements DatasetCatalog {

    private static final System.Logger LOGGER = System.getLogger(FilesetCatalog.class.getName());

    private final FileSource source;
    private final List<ByteRangeSource> openSources = new CopyOnWriteArrayList<>();
    private final List<String> datasetNames;
    private final Map<String, PendingDataset> pendingByName;
    private final ConcurrentMap<String, ParquetDataset> datasetsByName = new ConcurrentHashMap<>();
    private final CatalogCapabilities capabilities;

    /** The files a named dataset resolves from, held until its first {@link #dataset(String)} resolution builds it. */
    private record PendingDataset(List<FileEntry> files, CatalogOptions options) {}

    private FilesetCatalog(
            FileSource source,
            List<String> datasetNames,
            Map<String, PendingDataset> pendingByName,
            boolean enumeratesDatasets) {
        this.source = source;
        this.datasetNames = List.copyOf(datasetNames);
        this.pendingByName = Map.copyOf(pendingByName);
        this.capabilities = CatalogCapabilities.builder()
                .enumeratesDatasets(enumeratesDatasets)
                .schemaSource(CatalogCapabilities.SchemaSource.MERGED_FILES)
                .build();
    }

    /**
     * Opens a catalog merging every listed file into one dataset. All files must share a schema by equality. A Hive
     * partition key matching a physical column prunes that column; a path-only key is synthesized into an appended
     * column.
     *
     * <p>Open reads no file bytes: the dataset name comes from {@code options} or the root URI, and the merged dataset
     * is built (every footer fetched and decoded, schema equality validated) only when {@link #dataset(String)} first
     * resolves it. A schema mismatch or a corrupt footer therefore fails at that first resolution, not at open.
     *
     * @throws IllegalArgumentException if {@code source} lists no files
     * @throws java.io.UncheckedIOException if the source fails to list
     */
    public static FilesetCatalog open(FileSource source, CatalogOptions options) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");

        List<FileEntry> files = listSorted(source);
        if (files.isEmpty()) {
            throw closeAndReportNoFiles(source);
        }
        String name = options.datasetName().orElseGet(() -> deriveName(files, source.root()));
        return new FilesetCatalog(source, List.of(name), Map.of(name, new PendingDataset(files, options)), false);
    }

    /**
     * Opens a catalog publishing one dataset per file: every file the {@link FileSource} lists becomes its own
     * single-file dataset, named by its file name with the extension stripped, in relative-path order. Files never
     * merge and need not share a schema; a directory of heterogeneous files (one Natural Earth theme per file) resolves
     * to one dataset each.
     *
     * <p>Open reads no file bytes: the dataset names come from the listing alone, and each file's dataset is built (its
     * footer fetched and decoded) only when {@link #dataset(String)} first resolves it. A file whose footer is corrupt
     * or unreachable therefore fails at that first resolution, not at open.
     *
     * <p>The source is expected to list a flat set of uniquely named files (a non-recursive {@code *.parquet} match).
     * Should two entries still derive the same dataset name, open fails naming both files rather than silently dropping
     * one.
     *
     * @throws IllegalArgumentException if {@code source} lists no files
     * @throws IllegalStateException if two files derive the same dataset name
     * @throws java.io.UncheckedIOException if the source fails to list
     */
    public static FilesetCatalog openPerFile(FileSource source) {
        Objects.requireNonNull(source, "source");
        List<FileEntry> files = listSorted(source);
        if (files.isEmpty()) {
            throw closeAndReportNoFiles(source);
        }
        try {
            SequencedMap<String, PendingDataset> byName = fileEntriesByDatasetName(files);
            return new FilesetCatalog(source, List.copyOf(byName.keySet()), byName, true);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(List.of(), source);
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static SequencedMap<String, PendingDataset> fileEntriesByDatasetName(List<FileEntry> files) {
        SequencedMap<String, PendingDataset> byName = new LinkedHashMap<>();
        for (FileEntry file : files) {
            String name = stripExtension(file.relativePath());
            PendingDataset colliding =
                    byName.putIfAbsent(name, new PendingDataset(List.of(file), CatalogOptions.defaults()));
            if (colliding != null) {
                throw new IllegalStateException(
                        "files '" + colliding.files().get(0).relativePath() + "' and '" + file.relativePath()
                                + "' both derive the dataset name '" + name + "'");
            }
        }
        return byName;
    }

    /**
     * Builds a lazily resolved dataset by opening its files. The byte sources are registered for {@link #close()} only
     * after the build succeeds; a failed build closes them here and leaves the entry pending, ready for a later retry.
     */
    private ParquetDataset buildLazily(PendingDataset pending) {
        List<ByteRangeSource> opened = new ArrayList<>(pending.files().size());
        try {
            for (FileEntry file : pending.files()) {
                opened.add(file.open());
            }
            ParquetDataset dataset = buildDataset(pending.files(), opened, pending.options(), source.root());
            openSources.addAll(opened);
            return dataset;
        } catch (RuntimeException failure) {
            RuntimeException cleanup = null;
            for (ByteRangeSource bytes : opened) {
                cleanup = closeChaining(bytes, cleanup);
            }
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @SuppressWarnings("java:S2259") // open() guarantees at least one file; the loop always assigns unifiedSchema
    private static ParquetDataset buildDataset(
            List<FileEntry> files, List<ByteRangeSource> opened, CatalogOptions options, URI root) {

        List<GeoParquetMetadata> perFileGeo = new ArrayList<>();
        List<Map<String, String>> perFilePartitions = new ArrayList<>(files.size());
        List<FileStats> perFileFooterStats = new ArrayList<>(files.size());
        ParquetSchema unifiedSchema = null;
        ParquetSource firstFileSource = null;
        for (int index = 0; index < files.size(); index++) {
            ParquetSource fileDataset = ParquetSource.open(opened.get(index));
            if (firstFileSource == null) {
                firstFileSource = fileDataset;
            }
            ParquetSchema schema = fileDataset.schema();
            if (unifiedSchema == null) {
                unifiedSchema = schema;
            } else if (!unifiedSchema.equals(schema)) {
                throw new IllegalStateException("files do not share a schema by equality");
            }
            parseGeoMetadata(fileDataset, files.get(index)).ifPresent(perFileGeo::add);
            Map<String, String> partitions =
                    HivePartitionResolver.partitionValues(files.get(index).relativePath());
            perFilePartitions.add(partitions);
            perFileFooterStats.add(fileDataset.fileStats());
        }

        HivePartitioning partitioning = HivePartitioning.bind(perFilePartitions, unifiedSchema);
        ParquetSchema augmentedSchema = unifiedSchema.withAppendedLeaves(partitioning.syntheticLeaves());
        List<FileStats> stats = new ArrayList<>(files.size());
        for (int index = 0; index < files.size(); index++) {
            FileStats footer = perFileFooterStats.get(index);
            FileStats hive = partitioning.fileStats(perFilePartitions.get(index), footer.recordCount());
            stats.add(footer.withOverrides(hive));
        }

        Optional<GeoParquetMetadata> aggregatedGeo = GeoMetadataAggregator.aggregate(perFileGeo);
        // A one-file dataset reuses the gather pass's already-parsed source; reopening would fetch and decode
        // the footer a second time, which on a large remote file costs a full extra footer round trip.
        ParquetSource allFiles = files.size() == 1 ? firstFileSource : ParquetSource.open(new PreOpenedFileset(opened));
        DatasetCapabilities caps = capabilities(partitioning, aggregatedGeo);
        String name = options.datasetName().orElseGet(() -> deriveName(files, root));
        List<String> locations = files.stream().map(FileEntry::relativePath).toList();
        FilesetDataset.PartitionContext partitions =
                new FilesetDataset.PartitionContext(augmentedSchema, partitioning, perFilePartitions, stats);
        return new FilesetDataset(
                name, allFiles, partitions, opened, locations, caps, aggregatedGeo, OpenOptions.DEFAULTS);
    }

    /**
     * Reads one file's GeoParquet {@code geo} key-value metadata. A file with no {@code geo} key, a blank one, or one
     * whose value cannot be parsed contributes no geo metadata: an unparseable or forward-incompatible {@code geo}
     * block is logged and skipped rather than aborting the whole catalog open.
     */
    private static Optional<GeoParquetMetadata> parseGeoMetadata(ParquetSource fileDataset, FileEntry file) {
        String geoJson = fileDataset.keyValueMetadata().get("geo");
        if (geoJson == null || geoJson.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(GeoParquetMetadata.parse(geoJson));
        } catch (RuntimeException unparseable) {
            LOGGER.log(
                    System.Logger.Level.WARNING,
                    "ignoring unparseable GeoParquet 'geo' metadata in file '" + file.relativePath() + "'",
                    unparseable);
            return Optional.empty();
        }
    }

    private static DatasetCapabilities capabilities(
            HivePartitioning partitioning, Optional<GeoParquetMetadata> aggregatedGeo) {
        return DatasetCapabilities.builder()
                .fileStats(DatasetCapabilities.FileStatsSource.FOOTER_AGGREGATE)
                .partitionModel(
                        partitioning.isPartitioned()
                                ? DatasetCapabilities.PartitionModel.HIVE_PATH
                                : DatasetCapabilities.PartitionModel.NONE)
                .fileSpatialBounds(
                        aggregatedGeo.isPresent()
                                ? DatasetCapabilities.FileSpatialBounds.NATIVE_GEO
                                : DatasetCapabilities.FileSpatialBounds.NONE)
                .cheapCount(true)
                .cheapBounds(aggregatedGeo.isPresent())
                .build();
    }

    @Override
    public CatalogCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public List<String> datasets() {
        return datasetNames;
    }

    @Override
    public ParquetDataset dataset(String name) {
        ParquetDataset dataset = datasetsByName.get(name);
        if (dataset != null) {
            return dataset;
        }
        PendingDataset pending = pendingByName.get(name);
        if (pending == null) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have " + datasets() + ")");
        }
        return datasetsByName.computeIfAbsent(name, unused -> buildLazily(pending));
    }

    @Override
    public void close() {
        RuntimeException failure = closeAll(openSources, source);
        if (failure != null) {
            throw failure;
        }
    }

    private static List<FileEntry> listSorted(FileSource source) {
        try (Stream<FileEntry> files = source.list()) {
            return files.sorted(Comparator.comparing(FileEntry::relativePath)).toList();
        }
    }

    /**
     * Derives a dataset name from the file list or root URI.
     *
     * <p>When the source holds exactly one file, the name is derived from that file's relative path (extension
     * stripped) rather than the root URI. This is necessary because
     * {@link io.tileverse.parquetry.io.LocalFileSource#file} sets {@code root} to the parent directory, whose name is
     * opaque (e.g. a JUnit temp dir), not the file itself.
     *
     * <p>For multi-file sources, the last component of the root URI is used as the dataset name.
     */
    private static String deriveName(List<FileEntry> files, URI root) {
        if (files.size() == 1) {
            return stripExtension(files.get(0).relativePath());
        }
        String path = root.getPath();
        if (path == null || path.isEmpty()) {
            return "dataset";
        }
        String trimmed = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        String last = trimmed.substring(trimmed.lastIndexOf('/') + 1);
        return last.isEmpty() ? "dataset" : last;
    }

    private static String stripExtension(String filename) {
        String base = filename.substring(filename.lastIndexOf('/') + 1);
        int dot = base.lastIndexOf('.');
        return dot > 0 ? base.substring(0, dot) : base;
    }

    /** Closes {@code source} and returns the no-files failure, with any close failure attached as suppressed. */
    private static IllegalArgumentException closeAndReportNoFiles(FileSource source) {
        RuntimeException cleanup = closeAll(List.of(), source);
        IllegalArgumentException failure = new IllegalArgumentException("no files found at " + source.root());
        if (cleanup != null) {
            failure.addSuppressed(cleanup);
        }
        return failure;
    }

    /**
     * Closes every byte source and then the file source, attempting all of them even when one fails. Returns the first
     * failure with any later failures attached as suppressed, or {@code null} when every close succeeded.
     */
    private static RuntimeException closeAll(List<ByteRangeSource> sources, FileSource fileSource) {
        RuntimeException failure = null;
        for (ByteRangeSource src : sources) {
            failure = closeChaining(src, failure);
        }
        return closeChaining(fileSource, failure);
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

    /** A {@link FilesetReader} over byte sources the catalog already opened (and owns). */
    private record PreOpenedFileset(List<ByteRangeSource> sources) implements FilesetReader {
        @Override
        public ByteRangeSource openFile(int index) {
            return sources.get(index);
        }

        @Override
        public int fileCount() {
            return sources.size();
        }
    }
}
