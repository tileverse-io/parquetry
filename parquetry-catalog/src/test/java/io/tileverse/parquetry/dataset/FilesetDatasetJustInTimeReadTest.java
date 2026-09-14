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
package io.tileverse.parquetry.dataset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Pred;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.internal.read.TestParquetFiles;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.LocalFileSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.materializer.Materializer;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.CorpusFixtures;

/**
 * A multi-file {@link FilesetDataset} opens each file as its read reaches it. Without a probe the survivors fan out and
 * the read delivers exactly the rows that a per-file read would deliver; with a probe the files are visited one at a
 * time in order; closing early returns every borrowed buffer; a file that fails does so at its turn, after the files
 * before it were delivered; counts sum survivors through the bounded fold.
 */
class FilesetDatasetJustInTimeReadTest {

    private static final ColumnPath YEAR = ColumnPath.of("year");
    private static final ColumnPath COUNTRY = ColumnPath.of("country");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final ColumnPath REGION = ColumnPath.of("region");

    /** Distinct per-file row counts give the file-ordered sequence a shape that a wrong interleave cannot reproduce. */
    private static final int[] ROW_COUNTS = {1_500, 900, 2_100, 1_200};

    /** A probe that keeps every unit; its presence alone pins the read to the sequential visit. */
    private static final SpatialReadProbe KEEP_EVERYTHING =
            (minX, minY, maxX, maxY) -> SpatialReadProbe.Decision.keep();

    /** Materializes the row key asserted on by the {@link ParquetRecord} reads. */
    private static final Materializer<String> ROW_KEY = (schema, row) -> rowKey(row);

    private static final Path ALL_TYPES = CorpusFixtures.parquetTestingData().resolve("alltypes_plain.parquet");

    /** A file with a leaf column {@code b} beside a top-level column {@code a} that is a group of nested lists. */
    private static final Path NESTED_LISTS = CorpusFixtures.parquetTestingData().resolve("nested_lists.snappy.parquet");

    private static final ColumnPath NESTED_GROUP = ColumnPath.of("a");
    private static final ColumnPath NESTED_LEAF = ColumnPath.of("b");

    @Test
    void fanOutRowsEqualThePerFileReferenceAsMultiset(@TempDir Path root) throws IOException {
        List<Path> files = writeDistinctFiles(root, ROW_COUNTS);
        List<String> reference = perFileReferenceKeys(files);

        try (FilesetCatalog catalog = openCatalog(root)) {
            List<String> rows = readRowKeys(catalog.dataset(catalog.datasets().get(0)), ReadOptions.DEFAULTS);

            assertThat(rows).hasSameSizeAs(reference);
            assertThat(sorted(rows)).isEqualTo(sorted(reference));
        }
    }

    @Test
    void fanOutBatchesDeliverEveryRowExactlyOnce(@TempDir Path root) throws IOException {
        List<Path> files = writeDistinctFiles(root, ROW_COUNTS);
        List<String> reference = perFileReferenceKeys(files);

        try (FilesetCatalog catalog = openCatalog(root)) {
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
    void probePresenceVisitsFilesOneAtATimeInOrder(@TempDir Path root) throws IOException {
        List<Path> files = writeDistinctFiles(root, ROW_COUNTS);
        List<String> fileOrdered = perFileReferenceKeys(files);
        ReadOptions probing =
                ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();

        try (FilesetCatalog catalog = openCatalog(root)) {
            List<String> rows = readRowKeys(catalog.dataset(catalog.datasets().get(0)), probing);

            assertThat(rows).containsExactlyElementsOf(fileOrdered);
        }
    }

    @Test
    void aProbedMaterializerReadVisitsFilesOneAtATimeInOrder(@TempDir Path root) throws IOException {
        List<Path> files = writeDistinctFiles(root, ROW_COUNTS);
        List<String> fileOrdered = perFileReferenceKeys(files);
        ReadOptions probing =
                ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();

        try (FilesetCatalog catalog = openCatalog(root)) {
            List<String> keys;
            try (Stream<String> rows = catalog.dataset(catalog.datasets().get(0))
                    .read(Predicate.ALWAYS_TRUE, Projection.ALL, ROW_KEY, probing)) {
                keys = rows.toList();
            }

            assertThat(keys).containsExactlyElementsOf(fileOrdered);
        }
    }

    @Test
    void iteratorPulledRowsMatchThePerFileReference(@TempDir Path root) throws IOException {
        List<Path> files = writeDistinctFiles(root, new int[] {1_500, 1, 900, 1});
        List<String> reference = perFileReferenceKeys(files);

        try (FilesetCatalog catalog = openCatalog(root)) {
            List<String> delivered = new ArrayList<>();
            try (Stream<ParquetRecord> stream = catalog.dataset(
                            catalog.datasets().get(0))
                    .read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                Iterator<ParquetRecord> it = stream.iterator();
                while (it.hasNext()) {
                    delivered.add(rowKey(it.next()));
                }
            }

            assertThat(delivered).hasSameSizeAs(reference);
            assertThat(sorted(delivered)).isEqualTo(sorted(reference));
        }
    }

    @Test
    void earlyCloseOnTheFanOutRestoresPoolAndBudget(@TempDir Path tmp) throws IOException {
        Path file = TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(tmp, 4_000);
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
            FilesetDataset dataset = TestFilesetDatasets.plain(sources, openOptions);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stepInto(stream, 10);
            }
        } finally {
            closeAll(sources);
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers borrowed across the fanned-out files are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across the fanned-out files on early close")
                .isEqualTo(capacityBefore);
    }

    @Test
    void aFileFailingAtItsTurnFailsAfterTheFilesBeforeItWereDelivered(@TempDir Path root) throws IOException {
        List<Path> files = writeDistinctFiles(root, new int[] {700, 400, 300});
        List<ByteRangeSource> healthy = openSources(files);
        SwitchableByteRangeSource second = new SwitchableByteRangeSource(healthy.get(1));
        List<ByteRangeSource> sources = List.of(healthy.get(0), second, healthy.get(2));
        try {
            FilesetDataset dataset = TestFilesetDatasets.plain(sources, OpenOptions.DEFAULTS);
            second.failFromNowOn();
            ReadOptions probing =
                    ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();
            List<String> delivered = new ArrayList<>();

            assertThatThrownBy(() -> drainInto(dataset, probing, delivered))
                    .isInstanceOf(UncheckedIOException.class)
                    .hasMessageContaining("switched off");
            assertThat(delivered)
                    .as("every row of the file before the failing one was delivered first")
                    .hasSize(700);
        } finally {
            closeAll(healthy);
        }
    }

    @Test
    void countSumsSurvivorsWithAndWithoutAResidual(@TempDir Path root) throws IOException {
        Files.copy(ALL_TYPES, root.resolve("a.parquet"));
        Files.copy(ALL_TYPES, root.resolve("b.parquet"));
        long single;
        long singleFiltered;
        Predicate residual = Pred.col("id").gt(4);
        try (ByteRangeSource one = ByteRangeSource.ofFile(ALL_TYPES)) {
            ParquetSource source = ParquetSource.open(one);
            single = source.count();
            singleFiltered = source.count(residual);
        }
        assertThat(singleFiltered).isPositive();

        try (FilesetCatalog catalog = openCatalog(root)) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));

            assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(2 * single);
            assertThat(dataset.count(residual, ReadOptions.DEFAULTS)).isEqualTo(2 * singleFiltered);
        }
    }

    @Test
    void materializerReadSeesTheSynthesizedPartitionColumn(@TempDir Path root) throws IOException {
        Path west = Files.createDirectories(root.resolve("region=west"));
        Path east = Files.createDirectories(root.resolve("region=east"));
        TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(west, 120);
        TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(east, 80);

        try (FilesetCatalog catalog = openCatalog(root)) {
            List<String> regions;
            try (Stream<String> stream = catalog.dataset(catalog.datasets().get(0))
                    .read(
                            Predicate.ALWAYS_TRUE,
                            Projection.ALL,
                            (schema, row) -> row.getString(REGION),
                            ReadOptions.DEFAULTS)) {
                regions = stream.toList();
            }

            assertThat(regions).hasSize(200);
            assertThat(regions.stream().filter("west"::equals).count()).isEqualTo(120);
            assertThat(regions.stream().filter("east"::equals).count()).isEqualTo(80);
        }
    }

    @Test
    void aProjectionNamingANestedGroupKeepsThatGroupsLeaves(@TempDir Path root) throws IOException {
        Files.copy(NESTED_LISTS, root.resolve("a.parquet"));
        Files.copy(NESTED_LISTS, root.resolve("b.parquet"));
        Projection leafAndGroup = Projection.ofPhysical(List.of(NESTED_LEAF, NESTED_GROUP));
        List<String> reference = nestedGroupValues(NESTED_LISTS, leafAndGroup);
        assertThat(reference).isNotEmpty().doesNotContainNull();

        try (FilesetCatalog catalog = openCatalog(root)) {
            List<String> values;
            try (Stream<ParquetRecord> stream = catalog.dataset(
                            catalog.datasets().get(0))
                    .read(Predicate.ALWAYS_TRUE, leafAndGroup, ReadOptions.DEFAULTS)) {
                values = stream.map(FilesetDatasetJustInTimeReadTest::nestedGroupValue)
                        .toList();
            }

            List<String> bothCopies = new ArrayList<>(reference);
            bothCopies.addAll(reference);
            assertThat(sorted(values)).isEqualTo(sorted(bothCopies));
        }
    }

    // --- helpers ---

    private static FilesetCatalog openCatalog(Path root) {
        return FilesetCatalog.open(LocalFileSource.directory(root, "**.parquet"), CatalogOptions.defaults());
    }

    private static void stepInto(Stream<ParquetRecord> stream, int rows) {
        Iterator<ParquetRecord> it = stream.iterator();
        for (int consumed = 0; consumed < rows; consumed++) {
            assertThat(it.hasNext()).isTrue();
            it.next();
        }
    }

    /** Appends the row key of every row as the read delivers it, leaving behind what arrived before a failure. */
    private static void drainInto(ParquetDataset dataset, ReadOptions options, List<String> delivered) {
        try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            stream.map(FilesetDatasetJustInTimeReadTest::rowKey).forEach(delivered::add);
        }
    }

    private static List<String> readRowKeys(ParquetDataset dataset, ReadOptions options) {
        try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            return stream.map(FilesetDatasetJustInTimeReadTest::rowKey).toList();
        }
    }

    /** The file-ordered row keys of each file read on its own, concatenated in listing (path) order. */
    private static List<String> perFileReferenceKeys(List<Path> files) {
        List<String> keys = new ArrayList<>();
        for (Path file : files) {
            try (ByteRangeSource source = TestParquetFiles.openRangeReader(file)) {
                ParquetSource single = ParquetSource.open(source);
                try (Stream<ParquetRecord> rows =
                        single.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    rows.map(FilesetDatasetJustInTimeReadTest::rowKey).forEach(keys::add);
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

    /** The nested group's rendered value per row, read from {@code file} on its own as a single-file source. */
    private static List<String> nestedGroupValues(Path file, Projection projection) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetSource single = ParquetSource.open(source);
            try (Stream<ParquetRecord> rows = single.read(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)) {
                return rows.map(FilesetDatasetJustInTimeReadTest::nestedGroupValue)
                        .toList();
            }
        }
    }

    private static String nestedGroupValue(ParquetRecord row) {
        Object group = row.get(NESTED_GROUP);
        return group == null ? null : group.toString();
    }

    private static String rowKey(ParquetRecord row) {
        return row.getInt(YEAR) + "|" + row.getString(COUNTRY) + "|" + row.getDouble(VALUE);
    }

    private static List<String> sorted(List<String> keys) {
        List<String> copy = new ArrayList<>(keys);
        Collections.sort(copy);
        return copy;
    }

    /** One file per {@code file-<i>} directory (no {@code =}, hence no Hive partition), in listing order. */
    private static List<Path> writeDistinctFiles(Path root, int[] rowCounts) throws IOException {
        List<Path> files = new ArrayList<>(rowCounts.length);
        for (int i = 0; i < rowCounts.length; i++) {
            Path dir = Files.createDirectories(root.resolve("file-" + i));
            files.add(TestParquetFiles.writeFlatThreeColumnFileMultiRowGroup(dir, rowCounts[i]));
        }
        return files;
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

    /**
     * A byte source that serves its delegate until switched off, after which every read fails. It reports no source
     * identifier, keeping its footer out of the shared cache, which makes every reopen read through it.
     */
    private static final class SwitchableByteRangeSource implements ByteRangeSource {

        private final ByteRangeSource delegate;
        private volatile boolean failing;

        SwitchableByteRangeSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        void failFromNowOn() {
            failing = true;
        }

        @Override
        public long size() {
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            if (failing) {
                throw new UncheckedIOException(new IOException("source switched off"));
            }
            return delegate.read(offset, dst);
        }

        @Override
        public void close() {
            // the delegate is owned and closed by the test
        }
    }
}
