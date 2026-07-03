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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Covers the survivor fan-out on {@link FilesetDataset}'s synthetic-column path: a read that projects a path-only Hive
 * column drains its survivor files concurrently through {@link ConcurrentSurvivorReads}. The concurrent read must
 * deliver exactly the rows a per-file read would, each augmented with its file's synthesized partition value, and
 * closing it early must return every borrowed buffer and reservation across the fanned-out survivors.
 */
class FilesetDatasetConcurrentReadTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath REGION = ColumnPath.of("region");

    /** Distinct per-file row counts give the survivor set a non-repeating shape a broken interleave would not match. */
    private static final int[] ROW_COUNTS = {1_500, 900, 2_100, 1_200};

    private static final List<String> REGIONS = List.of("west", "east", "north", "south");

    @Test
    void concurrentSyntheticRowsEqualSingleFileReferenceAsMultiset(@TempDir Path root) throws IOException {
        List<Path> files = writeRegionTree(root);
        List<String> reference = singleFileReferenceKeys(files);

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            List<String> concurrent =
                    readRowKeys(catalog.dataset(catalog.datasets().get(0)));
            assertThat(concurrent).hasSameSizeAs(reference);
            assertThat(sorted(concurrent)).isEqualTo(sorted(reference));
        }
    }

    @Test
    void concurrentSyntheticBatchesDeliverEveryRowExactlyOnce(@TempDir Path root) throws IOException {
        List<Path> files = writeRegionTree(root);
        List<String> reference = singleFileReferenceKeys(files);

        try (FilesetCatalog catalog =
                FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults())) {
            List<String> keys = new ArrayList<>();
            try (Stream<ParquetRecordBatch> batches = catalog.dataset(
                            catalog.datasets().get(0))
                    .readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                batches.forEach(batch -> collectRowKeys(batch, keys));
            }
            assertThat(keys).hasSameSizeAs(reference);
            assertThat(sorted(keys)).isEqualTo(sorted(reference));
        }
    }

    @Test
    void earlyCloseAcrossSyntheticSurvivorsRestoresPoolAndBudget(@TempDir Path tmp) throws IOException {
        int rowsPerFile = 4_000;
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, rowsPerFile);
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        List<ByteRangeSource> sources = openSources(Collections.nCopies(4, file));
        try {
            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .prefetchDepth(4)
                            .maxConcurrentFiles(4)
                            .build())
                    .build();
            FilesetDataset dataset = syntheticDataset(sources, REGIONS, openOptions);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stepInto(stream, 10);
            }
        } finally {
            closeAll(sources);
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers borrowed across the fanned-out survivors are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across the fanned-out survivors on early close")
                .isEqualTo(capacityBefore);
    }

    /** Consumes the first {@code rows} rows and abandons the rest, exercising an early close mid-fan-out. */
    private static void stepInto(Stream<ParquetRecord> stream, int rows) {
        Iterator<ParquetRecord> it = stream.iterator();
        for (int consumed = 0; consumed < rows; consumed++) {
            assertThat(it.hasNext()).isTrue();
            it.next();
        }
    }

    private static List<String> readRowKeys(ParquetDataset dataset) {
        try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return stream.map(FilesetDatasetConcurrentReadTest::rowKey).toList();
        }
    }

    /**
     * The row keys a per-file read would deliver: each region's file read on its own as a single-file source, its rows
     * keyed with the region value the dataset synthesizes from that file's path.
     */
    private static List<String> singleFileReferenceKeys(List<Path> files) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < files.size(); i++) {
            String region = REGIONS.get(i);
            try (ByteRangeSource source = TestParquetFiles.openRangeReader(files.get(i))) {
                ParquetSource single = ParquetSource.open(source);
                try (Stream<ParquetRecord> rows =
                        single.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    rows.forEach(row -> keys.add(physicalKey(row) + "|" + region));
                }
            }
        }
        return keys;
    }

    private static void collectRowKeys(ParquetRecordBatch batch, List<String> into) {
        try (batch) {
            for (int row = 0; row < batch.rowCount(); row++) {
                into.add(rowKey(batch.materialize(row)));
            }
        }
    }

    private static String rowKey(ParquetRecord row) {
        return physicalKey(row) + "|" + row.getString(REGION);
    }

    private static String physicalKey(ParquetRecord row) {
        return row.getInt(YEAR) + "|" + row.getString(COUNTRY) + "|" + row.getDouble(VALUE);
    }

    private static List<String> sorted(List<String> keys) {
        List<String> copy = new ArrayList<>(keys);
        Collections.sort(copy);
        return copy;
    }

    private static List<Path> writeRegionTree(Path root) throws IOException {
        List<Path> files = new ArrayList<>(ROW_COUNTS.length);
        for (int i = 0; i < ROW_COUNTS.length; i++) {
            Path dir = Files.createDirectories(root.resolve("region=" + REGIONS.get(i)));
            files.add(TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, ROW_COUNTS[i]));
        }
        return files;
    }

    /**
     * Builds a {@link FilesetDataset} directly over {@code sources} with one synthetic {@code region} column per file,
     * bound to {@code openOptions}. Mirrors what the catalog assembles, letting a test inject the custom pool and
     * budget the catalog's default-runtime open does not expose.
     */
    private static FilesetDataset syntheticDataset(
            List<ByteRangeSource> sources, List<String> regions, OpenOptions openOptions) {
        List<Map<String, String>> perFilePartitions =
                regions.stream().map(region -> Map.of("region", region)).toList();
        ParquetSchema unifiedSchema = null;
        List<FileStats> footerStats = new ArrayList<>(sources.size());
        for (ByteRangeSource source : sources) {
            ParquetSource one = ParquetSource.open(source, openOptions);
            if (unifiedSchema == null) {
                unifiedSchema = one.schema();
            }
            footerStats.add(one.fileStats());
        }
        HivePartitioning partitioning = HivePartitioning.bind(perFilePartitions, unifiedSchema);
        ParquetSchema augmentedSchema = unifiedSchema.withAppendedLeaves(partitioning.syntheticLeaves());
        List<FileStats> stats = new ArrayList<>(sources.size());
        for (int i = 0; i < sources.size(); i++) {
            FileStats footer = footerStats.get(i);
            FileStats hive = partitioning.fileStats(perFilePartitions.get(i), footer.recordCount());
            stats.add(footer.withOverrides(hive));
        }
        ParquetSource allFiles = ParquetSource.open(TestFilesets.of(sources), openOptions);
        List<String> locations = regions.stream()
                .map(region -> "region=" + region + "/f.parquet")
                .toList();
        FilesetDataset.PartitionContext context =
                new FilesetDataset.PartitionContext(augmentedSchema, partitioning, perFilePartitions, stats);
        DatasetCapabilities capabilities = DatasetCapabilities.builder()
                .fileStats(DatasetCapabilities.FileStatsSource.FOOTER_AGGREGATE)
                .partitionModel(DatasetCapabilities.PartitionModel.HIVE_PATH)
                .build();
        return new FilesetDataset(
                "regions", allFiles, context, sources, locations, capabilities, Optional.empty(), openOptions);
    }

    private static List<ByteRangeSource> openSources(List<Path> files) {
        List<ByteRangeSource> sources = new ArrayList<>(files.size());
        for (Path file : files) {
            sources.add(TestParquetFiles.openRangeReader(file));
        }
        return sources;
    }

    private static void closeAll(List<ByteRangeSource> sources) {
        for (ByteRangeSource source : sources) {
            source.close();
        }
    }
}
