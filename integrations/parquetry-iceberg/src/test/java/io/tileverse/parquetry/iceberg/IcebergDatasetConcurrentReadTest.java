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

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.CatalogSnapshot;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.prune.FileStats;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Covers the survivor fan-out on {@link IcebergDataset}: a probe-less read over the snapshot's data files drains them
 * concurrently. The concurrent read must deliver exactly the rows the sequential per-file composition would, for both a
 * pass-through and a reconciled table schema, and closing it early must return every borrowed buffer and reservation.
 */
class IcebergDatasetConcurrentReadTest {

    private static final String TABLE = "v2_flat_columns";
    private static final long TOTAL_ROWS = 10_000L;

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath IDENTIFIER = ColumnPath.of("identifier");
    private static final ColumnPath XMIN = ColumnPath.of("xmin");

    /** A probe that keeps every unit; its presence alone pins the read to the sequential per-file composition. */
    private static final SpatialReadProbe KEEP_EVERYTHING =
            (minX, minY, maxX, maxY) -> SpatialReadProbe.Decision.keep();

    @TempDir
    Path tempDir;

    private final List<AutoCloseable> openResources = new ArrayList<>();

    @AfterEach
    void closeResources() throws Exception {
        for (AutoCloseable resource : openResources) {
            resource.close();
        }
    }

    @Test
    void concurrentPassThroughRowsEqualSequentialAsMultiset() {
        IcebergDataset dataset = dataset(currentFields(), OpenOptions.DEFAULTS);

        List<String> concurrent = readRowKeys(dataset, ID, ReadOptions.DEFAULTS);
        List<String> sequential = readRowKeys(dataset, ID, sequentialReadOptions());

        assertThat(concurrent).hasSize((int) TOTAL_ROWS);
        assertThat(sorted(concurrent)).isEqualTo(sorted(sequential));
    }

    @Test
    void concurrentReconciledRowsEqualSequentialAsMultiset() {
        IcebergDataset dataset = dataset(renamedIdFields(), OpenOptions.DEFAULTS);

        List<String> concurrent = readRowKeys(dataset, IDENTIFIER, ReadOptions.DEFAULTS);
        List<String> sequential = readRowKeys(dataset, IDENTIFIER, sequentialReadOptions());

        assertThat(concurrent).hasSize((int) TOTAL_ROWS);
        assertThat(sorted(concurrent)).isEqualTo(sorted(sequential));
    }

    @Test
    void earlyCloseAcrossDataFilesRestoresPoolAndBudget() {
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();
        OpenOptions openOptions = OpenOptions.builder()
                .runtime(ParquetRuntime.builder()
                        .segmentPool(pool)
                        .fetchBudget(budget)
                        .prefetchDepth(4)
                        .maxConcurrentFiles(4)
                        .build())
                .build();

        IcebergDataset dataset = dataset(currentFields(), openOptions);
        try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            stepInto(stream, 10);
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers borrowed across the fanned-out data files are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across the fanned-out data files on early close")
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

    private static List<String> readRowKeys(IcebergDataset dataset, ColumnPath idColumn, ReadOptions options) {
        try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            return stream.map(row -> rowKey(row, idColumn)).toList();
        }
    }

    /** A per-row key over the identifier and one bound; two rows with the same key are genuinely the same row. */
    private static String rowKey(ParquetRecord row, ColumnPath idColumn) {
        return row.getString(idColumn) + "|" + row.getDouble(XMIN);
    }

    private static ReadOptions sequentialReadOptions() {
        return ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();
    }

    private static List<String> sorted(List<String> keys) {
        List<String> copy = new ArrayList<>(keys);
        Collections.sort(copy);
        return copy;
    }

    private IcebergDataset dataset(List<IcebergField> tableFields, OpenOptions openOptions) {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        Path tableDir = root.resolve(TABLE);
        IcebergFileIO bootstrap = StorageIcebergFileIO.owning(
                StorageFactory.open(tableDir.toUri()), tableDir.toUri().toString());
        openResources.add(bootstrap);
        IcebergTableMetadata metadata = readMetadata(tableDir, bootstrap);

        IcebergFileIO io = StorageIcebergFileIO.owning(StorageFactory.open(tableDir.toUri()), metadata.tableLocation());
        openResources.add(io);
        List<IcebergManifests.DataFileRef> dataFiles =
                IcebergManifests.readDataFiles(metadata.manifestListLocation(), io);
        List<IcebergField> currentFields = metadata.fields();
        List<FileStats> fileStats = new ArrayList<>(dataFiles.size());
        List<ByteRangeSource> sources = new ArrayList<>(dataFiles.size());
        for (IcebergManifests.DataFileRef ref : dataFiles) {
            fileStats.add(IcebergFileStats.from(ref, currentFields, Map.of()));
            ByteRangeSource source = io.open(ref.location());
            openResources.add(source);
            sources.add(source);
        }

        CatalogSnapshot snapshot =
                new CatalogSnapshot(metadata.currentSnapshotId(), metadata.currentSnapshotTimestampMs());
        IcebergPartitionSpec partitionSpec = IcebergPartitionSpec.of(List.of(), List.of());
        IcebergDeletePlan deletePlan = IcebergDeletePlan.of(List.of(), io, null, IcebergNameMapping.empty());
        return new IcebergDataset(
                TABLE,
                snapshot,
                IcebergSchema.of(tableFields),
                partitionSpec,
                dataFiles,
                fileStats,
                sources,
                deletePlan,
                metadata.formatVersion(),
                IcebergNameMapping.empty(),
                IcebergNestedSchema.of(null),
                openOptions);
    }

    private static IcebergTableMetadata readMetadata(Path tableDir, IcebergFileIO io) {
        String metadataLocation =
                IcebergMetadataResolver.resolve(io, tableDir.toUri().toString(), IcebergOptions.defaults());
        return IcebergTableMetadata.read(readUtf8(io, metadataLocation), IcebergOptions.defaults());
    }

    private static String readUtf8(IcebergFileIO io, String location) {
        try (ByteRangeSource source = io.open(location)) {
            long size = source.size();
            byte[] bytes = new byte[Math.toIntExact(size)];
            source.readFully(0, MemorySegment.ofArray(bytes));
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static List<IcebergField> currentFields() {
        return List.of(
                new IcebergField(1, "id", "string", false),
                new IcebergField(2, "xmin", "double", false),
                new IcebergField(3, "ymin", "double", false),
                new IcebergField(4, "xmax", "double", false),
                new IcebergField(5, "ymax", "double", false));
    }

    private static List<IcebergField> renamedIdFields() {
        List<IcebergField> fields = new ArrayList<>(currentFields());
        fields.set(0, new IcebergField(1, "identifier", "string", false));
        return fields;
    }
}
