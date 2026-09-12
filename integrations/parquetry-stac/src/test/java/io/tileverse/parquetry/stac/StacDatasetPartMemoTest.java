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
package io.tileverse.parquetry.stac;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.SpatialReadProbe;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;

/**
 * Covers the bound on the per-part source memo: a part past the bound gives its reader back, a part evicted while a
 * read holds it keeps reading until that read ends, the dataset's drain closes every part left held by an abandoned
 * read whether or not the memo still holds it, and no part's reader closes twice.
 */
class StacDatasetPartMemoTest {

    private static final String GEOMETRY = "geometry";

    private static final int POINTS_PER_PART = 3;

    /** Pins the per-part visit order onto the consuming thread, which keeps a part held for as long as it is read. */
    private static final SpatialReadProbe KEEP_EVERYTHING =
            (minX, minY, maxX, maxY) -> SpatialReadProbe.Decision.keep();

    @TempDir
    Path dir;

    @Test
    void aPartPastTheBoundGivesItsReaderBack() throws Exception {
        int parts = 3;
        int bound = 2;

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = boundedDataset(storages, parts, bound);
            try {
                assertThat(countRows(dataset, ReadOptions.DEFAULTS)).isEqualTo((long) parts * POINTS_PER_PART);
                dataset.runPendingPartEvictions();

                assertThat(dataset.openReaderCount())
                        .as("visiting one part past the bound closes one part's reader")
                        .isEqualTo(bound);
                assertThat(countRows(dataset, ReadOptions.DEFAULTS))
                        .as("the evicted part reopens and reads the same rows")
                        .isEqualTo((long) parts * POINTS_PER_PART);
            } finally {
                dataset.closeResources();
            }
            assertThat(dataset.openReaderCount()).isZero();
        }
    }

    /**
     * Two parts are read at once with the memo bounded to one, which forces a real size eviction while both parts are
     * held. Whichever part Caffeine drops - its admission policy does not promise the oldest - the evicted part is
     * always one held by a read, and the invariant is the same: no reader closes under a live read, both reads finish
     * their rows, and the dropped part's reader closes the moment when its read lets go.
     */
    @Test
    void aRealEvictionLeavesAHeldPartReadable() throws Exception {
        ReadOptions sequential =
                ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = boundedDataset(storages, 2, 1);
            try (Stream<ParquetRecord> firstRead = dataset.read(onlyPart(0), Projection.ALL, sequential);
                    Stream<ParquetRecord> secondRead = dataset.read(onlyPart(1), Projection.ALL, sequential)) {
                Iterator<ParquetRecord> firstRows = startReading(firstRead);
                Iterator<ParquetRecord> secondRows = startReading(secondRead);

                dataset.runPendingPartEvictions();

                assertThat(dataset.memoizedPartCount())
                        .as("the second part took the memo past its bound, dropping one entry")
                        .isOne();
                assertThat(dataset.openReaderCount())
                        .as("the dropped part is held by a read, and its reader stays open")
                        .isEqualTo(2);
                assertThat(remaining(firstRows))
                        .as("the rows left in the first part still arrive")
                        .isEqualTo(POINTS_PER_PART - 1);
                assertThat(remaining(secondRows))
                        .as("the rows left in the second part still arrive")
                        .isEqualTo(POINTS_PER_PART - 1);
            }
            assertThat(dataset.openReaderCount())
                    .as("the evicted part's reader closes with the read that held it; the memoized one stays")
                    .isOne();

            dataset.closeResources();
            assertThat(dataset.openReaderCount()).isZero();
        }
    }

    /**
     * A read whose stream is never closed keeps its hold on the part forever. The dataset's drain is the last word: it
     * closes that part's reader anyway, and the hold given back later finds the part already closed.
     */
    @Test
    void theDrainClosesAPartHeldByAnAbandonedRead() throws Exception {
        ReadOptions sequential =
                ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = boundedDataset(storages, 2, 2);
            Stream<ParquetRecord> abandoned = dataset.read(onlyPart(0), Projection.ALL, sequential);
            startReading(abandoned);
            assertThat(dataset.openReaderCount())
                    .as("the one part visited by the read is held open")
                    .isOne();

            dataset.closeResources();
            assertThat(dataset.openReaderCount())
                    .as("the drain closes the abandoned read's part too")
                    .isZero();

            assertThatCode(abandoned::close)
                    .as("giving the hold back after the drain closes nothing a second time")
                    .doesNotThrowAnyException();
            assertThat(dataset.openReaderCount()).isZero();
        }
    }

    /**
     * Two abandoned reads with the memo bounded to one. Whichever part is dropped by the bound leaves the memo while a
     * read still holds it, and no later lookup of the memo will ever find it again. The dataset's drain closes it
     * anyway, along with the part kept by the memo.
     */
    @Test
    void theDrainClosesAPartDroppedFromTheMemoWhileAnAbandonedReadHeldIt() throws Exception {
        ReadOptions sequential =
                ReadOptions.builder().spatialReadProbe(KEEP_EVERYTHING).build();

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = boundedDataset(storages, 2, 1);
            Stream<ParquetRecord> firstRead = dataset.read(onlyPart(0), Projection.ALL, sequential);
            Stream<ParquetRecord> secondRead = dataset.read(onlyPart(1), Projection.ALL, sequential);
            startReading(firstRead);
            startReading(secondRead);
            dataset.runPendingPartEvictions();

            assertThat(dataset.memoizedPartCount())
                    .as("the bound dropped one of the two held parts from the memo")
                    .isOne();
            assertThat(dataset.openReaderCount())
                    .as("both parts stay open while their reads hold them")
                    .isEqualTo(2);

            dataset.closeResources();
            assertThat(dataset.openReaderCount())
                    .as("the drain closes the part dropped from the memo as well as the one still in it")
                    .isZero();

            assertThatCode(() -> closeBoth(firstRead, secondRead))
                    .as("giving the holds back after the drain closes nothing a second time")
                    .doesNotThrowAnyException();
            assertThat(dataset.openReaderCount()).isZero();
        }
    }

    private static void closeBoth(Stream<ParquetRecord> first, Stream<ParquetRecord> second) {
        first.close();
        second.close();
    }

    @Test
    void eachPartClosesOnceAcrossEvictionAndTheDrain() throws Exception {
        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = boundedDataset(storages, 3, 2);
            countRows(dataset, ReadOptions.DEFAULTS);
            dataset.runPendingPartEvictions();

            dataset.closeResources();
            assertThat(dataset.openReaderCount())
                    .as("every part closed exactly once, whether the bound evicted it or the drain dropped it")
                    .isZero();

            assertThatCode(dataset::closeResources).doesNotThrowAnyException();
            assertThat(dataset.openReaderCount())
                    .as("a second drain finds nothing left to close")
                    .isZero();
        }
    }

    /** Pulls the first row, which is what opens the part and takes its hold on the sequential read path. */
    private static Iterator<ParquetRecord> startReading(Stream<ParquetRecord> rows) {
        Iterator<ParquetRecord> reading = rows.iterator();
        assertThat(reading.hasNext()).isTrue();
        reading.next();
        return reading;
    }

    /** A box around one part's points alone: the other parts' item boxes are disjoint from it and prune away. */
    private static Predicate onlyPart(int part) {
        Bbox box = Bbox.of2d(part - 0.4, -1, part + 0.4, POINTS_PER_PART);
        return new Predicate.Spatial.BboxIntersects(ColumnPath.of(GEOMETRY), box);
    }

    private StacDataset boundedDataset(ContainerStorages storages, int parts, int maxMemoizedParts) throws Exception {
        List<StacItemRef> refs = new ArrayList<>(parts);
        List<double[]> bboxes = new ArrayList<>(parts);
        for (int part = 0; part < parts; part++) {
            Path file = dir.resolve("part" + part + ".parquet");
            StacPointParquet.writePoints(file, GEOMETRY, pointsAt(part));
            refs.add(new StacItemRef("i" + part, file.toUri().toString()));
            bboxes.add(new double[] {part, 0, part, POINTS_PER_PART - 1});
        }
        return new StacDataset(
                "buildings",
                GEOMETRY,
                () -> new StacDataset.Parts(refs, bboxes),
                /* firstRefSupplier */ null,
                Optional.empty(),
                storages,
                OpenOptions.DEFAULTS,
                maxMemoizedParts);
    }

    private static double[][] pointsAt(int part) {
        double[][] points = new double[POINTS_PER_PART][];
        for (int point = 0; point < POINTS_PER_PART; point++) {
            points[point] = new double[] {part, point};
        }
        return points;
    }

    private static long countRows(StacDataset dataset, ReadOptions options) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, options)) {
            return rows.count();
        }
    }

    private static long remaining(Iterator<ParquetRecord> reading) {
        long rows = 0;
        while (reading.hasNext()) {
            reading.next();
            rows++;
        }
        return rows;
    }
}
