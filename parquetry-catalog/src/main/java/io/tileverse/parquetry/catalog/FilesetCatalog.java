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
package io.tileverse.parquetry.catalog;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.FilesetDataset;
import io.tileverse.parquetry.dataset.FilesetReader;
import io.tileverse.parquetry.dataset.GeoMetadataAggregator;
import io.tileverse.parquetry.dataset.HivePartitionResolver;
import io.tileverse.parquetry.dataset.HivePartitioning;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * The file-based {@link DatasetCatalog}: it resolves exactly one merged {@link FilesetDataset} from a
 * {@link FileSource}, whether that source is a single file, a directory glob, or a Hive-partitioned tree. All matched
 * files must share a schema by equality.
 *
 * <p>Open reads each file's footer in a gather pass (schema, GeoParquet {@code geo} metadata, row count, and Hive
 * partition values) and again when the all-files dataset opens; composing the already-opened single-file readers into
 * the multi-file dataset is a possible follow-up. From the gathered metadata the partition keys are bound
 * ({@link HivePartitioning#bind}: a key matching a physical column prunes that column, a path-only key is synthesized
 * into an appended column), the per-file {@code geo} metadata is unioned ({@link GeoMetadataAggregator#aggregate}), and
 * one {@link FilesetDataset} is built with per-file partition {@link FileStats} for pruning.
 *
 * <p>Hive {@code key=value} segments are a physical-column pruning aid, never a dataset discriminator: the whole tree
 * is one dataset. The footer reads at open are the known scale ceiling, acceptable for moderate file counts.
 *
 * <p>The catalog owns the byte sources it opens and the {@link FileSource}; {@link #close()} releases both.
 */
public final class FilesetCatalog implements DatasetCatalog {

    private static final System.Logger LOGGER = System.getLogger(FilesetCatalog.class.getName());

    private final FileSource source;
    private final List<ByteRangeSource> openSources;
    private final ParquetDataset dataset;
    private final CatalogCapabilities capabilities;

    private FilesetCatalog(FileSource source, List<ByteRangeSource> openSources, ParquetDataset dataset) {
        this.source = source;
        this.openSources = openSources;
        this.dataset = dataset;
        this.capabilities = CatalogCapabilities.builder()
                .enumeratesDatasets(false)
                .schemaSource(CatalogCapabilities.SchemaSource.MERGED_FILES)
                .build();
    }

    /**
     * Opens a catalog over {@code source}. Every file is opened immediately and all files must share a schema by
     * equality. A Hive partition key matching a physical column prunes that column; a path-only key is synthesized into
     * an appended column.
     *
     * @throws IllegalArgumentException if {@code source} lists no files
     * @throws IllegalStateException if the files do not share a schema
     * @throws io.tileverse.parquetry.format.ParquetFormatException if any footer fails to conform to the spec
     * @throws java.io.UncheckedIOException if any source fails to deliver bytes
     */
    public static FilesetCatalog open(FileSource source, CatalogOptions options) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");

        List<FileEntry> files = listSorted(source);
        if (files.isEmpty()) {
            RuntimeException cleanup = closeAll(List.of(), source);
            IllegalArgumentException failure = new IllegalArgumentException("no files found at " + source.root());
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }

        List<ByteRangeSource> opened = new ArrayList<>(files.size());
        try {
            for (FileEntry file : files) {
                opened.add(file.open());
            }
            ParquetDataset built = buildDataset(files, opened, options, source.root());
            return new FilesetCatalog(source, opened, built);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(opened, source);
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
        for (int index = 0; index < files.size(); index++) {
            ParquetSource fileDataset = ParquetSource.open(opened.get(index));
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
        ParquetSource allFiles = ParquetSource.open(new PreOpenedFileset(opened));
        DatasetCapabilities caps = capabilities(partitioning, aggregatedGeo);
        String name = options.datasetName().orElseGet(() -> deriveName(files, root));
        List<String> locations = files.stream().map(FileEntry::relativePath).toList();
        FilesetDataset.PartitionContext partitions =
                new FilesetDataset.PartitionContext(augmentedSchema, partitioning, perFilePartitions, stats);
        return new FilesetDataset(name, allFiles, partitions, opened, locations, caps, aggregatedGeo);
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
        return List.of(dataset.name());
    }

    @Override
    public ParquetDataset dataset(String name) {
        if (!dataset.name().equals(name)) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have '" + dataset.name() + "')");
        }
        return dataset;
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
