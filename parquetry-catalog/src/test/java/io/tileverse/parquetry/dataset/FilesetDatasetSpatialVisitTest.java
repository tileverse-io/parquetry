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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.FileEntry;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.PointParquet;

/**
 * A probe-bearing read over a {@link FilesetDataset} of single-row-group point files, each covering one disjoint
 * integer-X cell, visits the files ordered by their geometry box and drops a file whose cell an earlier file already
 * painted before that file's data is ever read. The boxes come from the footer statistics recorded by the catalog at
 * open; the byte-source recorder around each file proves the skipped file is never touched.
 */
class FilesetDatasetSpatialVisitTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @TempDir
    Path tempDir;

    @Test
    void skipsAlreadyPaintedFilesBeforeReadingThem() throws Exception {
        int[] cellsInPathOrder = {0, 1, 0, 2};
        RecordingFileSource files = writeCellFiles(cellsInPathOrder);
        try (FilesetCatalog catalog = FilesetCatalog.open(files, CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            assertThat(readIntegerXCells(dataset, ReadOptions.DEFAULTS))
                    .as("a read without a probe returns every file's row")
                    .hasSize(cellsInPathOrder.length);

            files.resetTouches();
            ReadOptions decimating = ReadOptions.builder()
                    .spatialReadProbe(skipAlreadyPaintedCell())
                    .build();
            List<Integer> survivingCells = readIntegerXCells(dataset, decimating);

            assertThat(survivingCells)
                    .as("each cell is read once; the repeat of cell 0 is skipped")
                    .containsExactlyInAnyOrder(0, 1, 2);
            Set<Integer> seen = new HashSet<>();
            for (int file = 0; file < cellsInPathOrder.length; file++) {
                boolean repeat = !seen.add(cellsInPathOrder[file]);
                assertThat(files.touched(file))
                        .as("file %d (cell %d) touched during the decimating read", file, cellsInPathOrder[file])
                        .isEqualTo(!repeat);
            }
        }
    }

    @Test
    void probePresentVisitsFilesInAscendingBoundsOrder() throws Exception {
        RecordingFileSource files = writeCellFiles(new int[] {2, 0, 1});
        try (FilesetCatalog catalog = FilesetCatalog.open(files, CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            List<Integer> visitedCells = new ArrayList<>();
            ReadOptions options = ReadOptions.builder()
                    .spatialReadProbe(recordVisitOrder(visitedCells))
                    .build();

            try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
                rows.forEach(row -> {});
            }

            assertThat(visitedCells)
                    .as("files are visited ordered by their box's minimum corner, not in path order")
                    .containsExactly(0, 1, 2);
        }
    }

    @Test
    void noProbeReturnsEveryRowInAnyOrder() throws Exception {
        RecordingFileSource files = writeCellFiles(new int[] {2, 0, 1});
        try (FilesetCatalog catalog = FilesetCatalog.open(files, CatalogOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));

            assertThat(readIntegerXCells(dataset, ReadOptions.DEFAULTS)).containsExactlyInAnyOrder(0, 1, 2);
        }
    }

    /**
     * A screen-map-style probe: the per-row consultation paints the point's cell and keeps the row; the read-only
     * coarse consultation skips a whole file whose cell an earlier file already painted and otherwise descends.
     */
    private static SpatialReadProbe skipAlreadyPaintedCell() {
        Set<Integer> painted = new HashSet<>();
        return new SpatialReadProbe() {
            @Override
            public Decision probe(double minX, double minY, double maxX, double maxY) {
                painted.add(cellOf(minX));
                return Decision.keep();
            }

            @Override
            public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                return painted.contains(cellOf(minX)) ? Decision.skip() : Decision.descend();
            }
        };
    }

    /** Records each cell as soon as the probe first sees it, capturing the file visit order. */
    private static SpatialReadProbe recordVisitOrder(List<Integer> visitedCells) {
        return new SpatialReadProbe() {
            @Override
            public Decision probe(double minX, double minY, double maxX, double maxY) {
                recordOnce(cellOf(minX));
                return Decision.keep();
            }

            @Override
            public Decision probeRegion(double minX, double minY, double maxX, double maxY) {
                recordOnce(cellOf(minX));
                return Decision.descend();
            }

            private void recordOnce(int cell) {
                if (!visitedCells.contains(cell)) {
                    visitedCells.add(cell);
                }
            }
        };
    }

    private RecordingFileSource writeCellFiles(int[] cellsInPathOrder) throws Exception {
        List<Path> paths = new ArrayList<>(cellsInPathOrder.length);
        for (int file = 0; file < cellsInPathOrder.length; file++) {
            Path path = tempDir.resolve("file" + file + ".parquet");
            double[][] points = {{cellsInPathOrder[file] + 0.5, 10.0}};
            paths.add(PointParquet.writePoints(path, "geometry", GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0, points));
        }
        return new RecordingFileSource(tempDir, paths);
    }

    private static List<Integer> readIntegerXCells(ParquetDataset dataset, ReadOptions options) {
        List<Integer> cells = new ArrayList<>();
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            rows.forEach(row -> {
                MemorySegment wkb = (MemorySegment) row.get(GEOMETRY);
                cells.add(cellOf(wkb.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, 5)));
            });
        }
        return cells;
    }

    private static int cellOf(double minX) {
        return (int) Math.floor(minX);
    }

    /**
     * A file source over pre-written files whose byte sources record whether anything read through them. The catalog
     * opens the entries on virtual threads, hence the concurrent map keyed by relative path.
     */
    private static final class RecordingFileSource implements FileSource {

        private final Path root;
        private final List<Path> files;
        private final Map<String, RecordingByteRangeSource> opened = new ConcurrentHashMap<>();

        RecordingFileSource(Path root, List<Path> files) {
            this.root = root;
            this.files = files;
        }

        boolean touched(int file) {
            return opened.get(relativePath(files.get(file))).touched();
        }

        void resetTouches() {
            opened.values().forEach(RecordingByteRangeSource::reset);
        }

        @Override
        public URI root() {
            return root.toUri();
        }

        @Override
        public Stream<FileEntry> list() {
            return files.stream().map(this::entry);
        }

        private FileEntry entry(Path path) {
            String relative = relativePath(path);
            return new FileEntry() {
                @Override
                public String relativePath() {
                    return relative;
                }

                @Override
                public long sizeBytes() {
                    return -1L;
                }

                @Override
                public ByteRangeSource open() {
                    RecordingByteRangeSource source = new RecordingByteRangeSource(ByteRangeSource.ofFile(path));
                    opened.put(relative, source);
                    return source;
                }
            };
        }

        private String relativePath(Path path) {
            return root.relativize(path).toString().replace('\\', '/');
        }

        @Override
        public void close() {
            // the catalog closes the byte sources it opened
        }
    }

    /** Delegates every call and flags whether a {@code size()} or a {@code read()} went through it. */
    private static final class RecordingByteRangeSource implements ByteRangeSource {

        private final ByteRangeSource delegate;
        private volatile boolean touched;

        RecordingByteRangeSource(ByteRangeSource delegate) {
            this.delegate = delegate;
        }

        @Override
        public long size() {
            touched = true;
            return delegate.size();
        }

        @Override
        public int read(long offset, MemorySegment dst) {
            touched = true;
            return delegate.read(offset, dst);
        }

        @Override
        public void close() {
            delegate.close();
        }

        void reset() {
            touched = false;
        }

        boolean touched() {
            return touched;
        }
    }
}
