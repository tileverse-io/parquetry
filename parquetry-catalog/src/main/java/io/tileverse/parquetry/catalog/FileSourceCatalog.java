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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.dataset.DatasetCapabilities;
import io.tileverse.parquetry.dataset.DatasetCapabilities.FileStatsSource;
import io.tileverse.parquetry.dataset.DatasetCapabilities.PartitionModel;
import io.tileverse.parquetry.dataset.DatasetUnit;
import io.tileverse.parquetry.dataset.FileSourceDataset;
import io.tileverse.parquetry.dataset.FilesetReader;
import io.tileverse.parquetry.dataset.HivePartitionResolver;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;

/**
 * Reads the files of a {@link FileSource} as one or more {@link Dataset}s. A single file or single-unit listing yields
 * one dataset; a directory of heterogeneous files or hive-partitioned trees yields N (controlled by
 * {@link CatalogOptions#maxHiveDepth()}). Every file is opened eagerly and owned by the catalog; {@link #close()}
 * releases all byte sources and the {@link FileSource}.
 */
public final class FileSourceCatalog implements DatasetCatalog {

    private final FileSource source;
    private final List<ByteRangeSource> openSources;
    private final Map<String, Dataset> datasets;
    private final CatalogCapabilities capabilities;

    private FileSourceCatalog(
            FileSource source,
            List<ByteRangeSource> openSources,
            Map<String, Dataset> datasets,
            CatalogCapabilities capabilities) {
        this.source = source;
        this.openSources = openSources;
        this.datasets = datasets;
        this.capabilities = capabilities;
    }

    /**
     * Opens a catalog over {@code source}, resolving datasets per {@code options}.
     *
     * @throws IllegalArgumentException if {@code source} lists no files
     */
    public static FileSourceCatalog open(FileSource source, CatalogOptions options) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(options, "options");

        List<ByteRangeSource> opened = new ArrayList<>();
        Map<String, Dataset> built = new LinkedHashMap<>();
        try {
            List<FileEntry> files = listSorted(source);
            if (files.isEmpty()) {
                throw new IllegalArgumentException("no files found at " + source.root());
            }
            List<DatasetUnit> units =
                    HivePartitionResolver.resolve(files, options.maxHiveDepth().orElse(null));
            requireDatasetNameAppliesToOneDataset(options, units);
            for (DatasetUnit unit : units) {
                List<ByteRangeSource> unitSources = new ArrayList<>(unit.files().size());
                for (FileEntry file : unit.files()) {
                    ByteRangeSource byteSource = file.open();
                    opened.add(byteSource);
                    unitSources.add(byteSource);
                }
                ParquetDataset dataset = ParquetDataset.open(new PreOpenedFileset(unitSources));
                String name = datasetName(options, units, unit);
                if (built.containsKey(name)) {
                    throw new IllegalStateException("two dataset units resolve to the same name '" + name + "'");
                }
                built.put(name, new FileSourceDataset(name, dataset, pureParquetCapabilities(unit)));
            }
            CatalogCapabilities caps = CatalogCapabilities.builder()
                    .enumeratesDatasets(built.size() > 1)
                    .build();
            return new FileSourceCatalog(source, opened, built, caps);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(opened, source);
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    @Override
    public CatalogCapabilities capabilities() {
        return capabilities;
    }

    @Override
    public List<String> datasets() {
        return List.copyOf(datasets.keySet());
    }

    @Override
    public Dataset dataset(String name) {
        Dataset dataset = datasets.get(name);
        if (dataset == null) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have " + datasets.keySet() + ")");
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

    private static void requireDatasetNameAppliesToOneDataset(CatalogOptions options, List<DatasetUnit> units) {
        if (options.datasetName().isPresent() && units.size() > 1) {
            throw new IllegalStateException("datasetName is set but the source resolves to " + units.size()
                    + " datasets; datasetName applies only to a single-dataset source");
        }
    }

    private static String datasetName(CatalogOptions options, List<DatasetUnit> units, DatasetUnit unit) {
        if (units.size() == 1) {
            return options.datasetName().orElse(unit.name());
        }
        return unit.name();
    }

    private static DatasetCapabilities pureParquetCapabilities(DatasetUnit unit) {
        PartitionModel partitionModel =
                unit.partitionValues().isEmpty() ? PartitionModel.NONE : PartitionModel.HIVE_PATH;
        return DatasetCapabilities.builder()
                .fileStats(FileStatsSource.FOOTER_AGGREGATE)
                .partitionModel(partitionModel)
                .cheapCount(true)
                .build();
    }

    private static List<FileEntry> listSorted(FileSource source) {
        try (Stream<FileEntry> files = source.list()) {
            return files.sorted(Comparator.comparing(FileEntry::relativePath)).toList();
        }
    }

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
