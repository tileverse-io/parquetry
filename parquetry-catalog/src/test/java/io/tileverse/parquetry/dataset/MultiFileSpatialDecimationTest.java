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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.filter.SpatialReadProbe.Decision;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testsupport.PointParquet;

/**
 * Reads a multi-file GeoParquet dataset of single-row-group point files, each covering a disjoint integer-X cell,
 * through a {@link SpatialReadProbe} and asserts the dataset's two probe-driven behaviors. The coarse consultation
 * skips a whole file whose cell an earlier file already painted, before that file's read pipeline is ever opened; and a
 * probe makes the dataset visit files ordered by their geometry bounds rather than in construction (path) order.
 */
class MultiFileSpatialDecimationTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @TempDir
    Path tempDir;

    /**
     * The file gate consults the probe before opening a file: a file the probe reports as already covered is dropped
     * before its read pipeline opens, and its data is never read. A byte recorder around each file proves the skip; the
     * dropped file's zero I/O is the saving the file gate exists for.
     */
    @Nested
    class FileSkipping {

        @Test
        void skipsAlreadyPaintedFilesBeforeReadingThem() throws Exception {
            int[] cellsInPathOrder = {0, 1, 0, 2};
            List<RecordingByteRangeSource> sources = openSources(cellsInPathOrder);
            try {
                // A serial runtime (no decode-ahead, no prefetch) keeps a kept file's data read the only thing that
                // touches its source during the decimating read.
                ParquetRuntime serialRuntime =
                        ParquetRuntime.defaultRuntime().withMaxDecodeAhead(0).withPrefetchDepth(0);
                DefaultParquetSource dataset = datasetOver(sources, serialRuntime);

                assertThat(rowCount(dataset, ReadOptions.DEFAULTS))
                        .as("a read without a probe returns every file's row")
                        .isEqualTo(cellsInPathOrder.length);

                sources.forEach(RecordingByteRangeSource::reset);
                ReadOptions decimating = ReadOptions.builder()
                        .spatialReadProbe(skipAlreadyPaintedCell())
                        .build();
                List<Integer> survivingCells = readIntegerXCells(dataset, decimating);

                assertThat(survivingCells)
                        .as("each cell is read once; the repeat of cell 0 is skipped")
                        .containsExactlyInAnyOrder(0, 1, 2);

                assertOnlyTheRepeatedCellFileWasRead(sources, cellsInPathOrder);
            } finally {
                closeAll(sources);
            }
        }

        /**
         * A screen-map-style probe. The per-row leaf consultation ({@code probe}) paints the point's cell and keeps the
         * row. The read-only coarse consultation ({@code probeRegion}) skips a whole file whose cell an earlier file
         * already painted, and otherwise descends without painting. The file gate calls {@code probeRegion} before
         * opening a file.
         */
        private SpatialReadProbe skipAlreadyPaintedCell() {
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

        /**
         * The file whose cell repeats an earlier file's cell is dropped before its read opens, hence its source is
         * never touched; every other file is read once and touches its source.
         */
        private void assertOnlyTheRepeatedCellFileWasRead(
                List<RecordingByteRangeSource> sources, int[] cellsInPathOrder) {
            Set<Integer> seen = new HashSet<>();
            for (int file = 0; file < cellsInPathOrder.length; file++) {
                int cell = cellsInPathOrder[file];
                boolean repeat = !seen.add(cell);
                assertThat(sources.get(file).touched())
                        .as("file %d (cell %d) source touched during the decimating read", file, cell)
                        .isEqualTo(!repeat);
            }
        }

        private List<RecordingByteRangeSource> openSources(int[] cellsInPathOrder) throws Exception {
            List<RecordingByteRangeSource> sources = new ArrayList<>(cellsInPathOrder.length);
            for (int file = 0; file < cellsInPathOrder.length; file++) {
                Path path = writeSingleCellFile(file, cellsInPathOrder[file]);
                sources.add(new RecordingByteRangeSource(ByteRangeSource.ofFile(path)));
            }
            return sources;
        }

        /**
         * A {@link ByteRangeSource} that delegates to a backing source and flags whether the per-file read interacted
         * with it at all (a {@code size()} or a {@code read()}), confirming that a skipped file's source is never
         * touched.
         */
        private static final class RecordingByteRangeSource implements ByteRangeSource {

            private final ByteRangeSource delegate;
            private boolean touched;

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

    /**
     * The dataset's files in construction (path) order are spatially interleaved: the path order of their integer-X
     * cells is not ascending. With a probe present, the multi-file read visits files ordered by their geometry bounds'
     * minimum corner; without a probe, the default construction order is preserved.
     */
    @Nested
    class VisitationOrder {

        private static final int[] CELLS_IN_PATH_ORDER = {2, 0, 1};

        @Test
        void probePresentVisitsFilesInAscendingBoundsOrder() throws Exception {
            List<ByteRangeSource> sources = openSources();
            try {
                DefaultParquetSource dataset = datasetOver(sources, ParquetRuntime.defaultRuntime());
                List<Integer> visitedCells = new ArrayList<>();
                ReadOptions options = ReadOptions.builder()
                        .spatialReadProbe(recordVisitOrder(visitedCells))
                        .build();

                drain(dataset, options);

                assertThat(visitedCells)
                        .as("files are visited ordered by their bounds' minimum corner, not in path order")
                        .containsExactly(0, 1, 2);
            } finally {
                closeAll(sources);
            }
        }

        @Test
        void noProbeReturnsEveryRowInAnyOrder() throws Exception {
            List<ByteRangeSource> sources = openSources();
            try {
                DefaultParquetSource dataset = datasetOver(sources, ParquetRuntime.defaultRuntime());

                List<Integer> rowCells = readIntegerXCells(dataset, ReadOptions.DEFAULTS);

                assertThat(rowCells)
                        .as("without a probe the files fan out; every row returns, in no guaranteed order")
                        .containsExactlyInAnyOrder(0, 1, 2);
            } finally {
                closeAll(sources);
            }
        }

        /** Records each cell the moment the probe first sees it, capturing the file visitation order. */
        private SpatialReadProbe recordVisitOrder(List<Integer> visitedCells) {
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

        private List<ByteRangeSource> openSources() throws Exception {
            List<ByteRangeSource> sources = new ArrayList<>(CELLS_IN_PATH_ORDER.length);
            for (int file = 0; file < CELLS_IN_PATH_ORDER.length; file++) {
                Path path = writeSingleCellFile(file, CELLS_IN_PATH_ORDER[file]);
                sources.add(ByteRangeSource.ofFile(path));
            }
            return sources;
        }
    }

    private Path writeSingleCellFile(int file, int cell) throws Exception {
        Path path = tempDir.resolve("file" + file + ".parquet");
        double[][] points = {{cell + 0.5, 10.0}};
        return PointParquet.writePoints(path, "geometry", GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0, points);
    }

    private static DefaultParquetSource datasetOver(List<? extends ByteRangeSource> sources, ParquetRuntime runtime) {
        List<ParquetFileReader> readers = new ArrayList<>(sources.size());
        for (ByteRangeSource source : sources) {
            readers.add(ParquetFileReader.open(source, runtime, Optional.empty()));
        }
        return new DefaultParquetSource(readers, runtime.maxConcurrentFiles());
    }

    private static long rowCount(DefaultParquetSource dataset, ReadOptions options) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            return rows.count();
        }
    }

    private static void drain(DefaultParquetSource dataset, ReadOptions options) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            rows.forEach(row -> {});
        }
    }

    private static List<Integer> readIntegerXCells(DefaultParquetSource dataset, ReadOptions options) {
        List<Integer> cells = new ArrayList<>();
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            rows.forEach(row -> cells.add(integerXCellOf(row)));
        }
        return cells;
    }

    private static int integerXCellOf(ParquetRecord row) {
        MemorySegment wkb = (MemorySegment) row.get(GEOMETRY);
        double x = wkb.get(ValueLayout.JAVA_DOUBLE_UNALIGNED, 5);
        return cellOf(x);
    }

    private static int cellOf(double minX) {
        return (int) Math.floor(minX);
    }

    private static void closeAll(List<? extends ByteRangeSource> sources) {
        for (ByteRangeSource source : sources) {
            source.close();
        }
    }
}
