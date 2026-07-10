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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.io.SegmentPool;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.runtime.FetchBudget;
import io.tileverse.parquetry.runtime.ParquetRuntime;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

/**
 * Covers the survivor fan-out on {@link StacDataset}: a probe-less read over many parts drains them concurrently
 * through the dataset's fan-out. The concurrent read must deliver exactly the rows a per-part read would, and closing
 * it early must return every borrowed buffer and reservation across the fanned-out parts.
 */
class StacDatasetConcurrentReadTest {

    private static final String GEOMETRY = "geometry";
    private static final ColumnPath GEOMETRY_PATH = ColumnPath.of(GEOMETRY);

    /**
     * Distinct per-part point counts give the survivor set a non-repeating shape a broken interleave would not match.
     */
    private static final int[] POINT_COUNTS = {40, 25, 60, 15};

    @Test
    void concurrentRowsEqualSingleFileReferenceAsMultiset(@TempDir Path dir) throws Exception {
        writeParts(dir);
        List<String> reference = singleFileReferenceKeys(dir);

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = dataset(dir, storages, fanOutOptions(4));
            try {
                List<String> concurrent = readRowKeys(dataset);
                assertThat(concurrent).hasSameSizeAs(reference);
                assertThat(sorted(concurrent)).isEqualTo(sorted(reference));
            } finally {
                dataset.closeResources();
            }
        }
    }

    @Test
    void concurrentBatchesDeliverEveryRowExactlyOnce(@TempDir Path dir) throws Exception {
        writeParts(dir);
        List<String> reference = singleFileReferenceKeys(dir);

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            StacDataset dataset = dataset(dir, storages, fanOutOptions(4));
            try {
                List<String> keys = new ArrayList<>();
                try (Stream<ParquetRecordBatch> batches =
                        dataset.readBatches(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                    batches.forEach(batch -> collectRowKeys(batch, keys));
                }
                assertThat(keys).hasSameSizeAs(reference);
                assertThat(sorted(keys)).isEqualTo(sorted(reference));
            } finally {
                dataset.closeResources();
            }
        }
    }

    @Test
    void earlyCloseAcrossPartsRestoresPoolAndBudget(@TempDir Path dir) throws Exception {
        writeParts(dir);
        SegmentPool pool = SegmentPool.create();
        FetchBudget budget = FetchBudget.ofMaxMemoryFraction(0.1);
        long capacityBefore = budget.available();

        try (ContainerStorages storages = new ContainerStorages(new Properties())) {
            OpenOptions openOptions = OpenOptions.builder()
                    .runtime(ParquetRuntime.builder()
                            .segmentPool(pool)
                            .fetchBudget(budget)
                            .maxConcurrentFiles(4)
                            .build())
                    .build();
            StacDataset dataset = dataset(dir, storages, openOptions);
            try (Stream<ParquetRecord> stream =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                stepInto(stream, 1);
            } finally {
                dataset.closeResources();
            }
        }

        assertThat(pool.stats().outstandingBorrows())
                .as("buffers borrowed across the fanned-out parts are returned on early close")
                .isZero();
        assertThat(budget.available())
                .as("reserved fetch budget is restored across the fanned-out parts on early close")
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

    private static List<String> readRowKeys(StacDataset dataset) {
        try (Stream<ParquetRecord> stream = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return stream.map(StacDatasetConcurrentReadTest::rowKey).toList();
        }
    }

    /** The row keys a per-part read would deliver: each part read on its own as a single-file source. */
    private static List<String> singleFileReferenceKeys(Path dir) throws Exception {
        List<String> keys = new ArrayList<>();
        try (Storage storage = StorageFactory.open(dir.toUri())) {
            for (int i = 0; i < POINT_COUNTS.length; i++) {
                try (ByteRangeSource source = ByteRangeSources.from(storage.openRangeReader(partName(i)))) {
                    ParquetSource single = ParquetSource.open(source);
                    try (Stream<ParquetRecord> rows =
                            single.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                        rows.forEach(row -> keys.add(rowKey(row)));
                    }
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

    /** A per-row key over the point geometry, whose points are all distinct: two rows with the same key are one row. */
    private static String rowKey(ParquetRecord row) {
        return HexFormat.of().formatHex(row.getBinary(GEOMETRY_PATH));
    }

    private static List<String> sorted(List<String> keys) {
        List<String> copy = new ArrayList<>(keys);
        Collections.sort(copy);
        return copy;
    }

    private static StacDataset dataset(Path dir, ContainerStorages storages, OpenOptions openOptions) {
        List<StacItemRef> refs = new ArrayList<>(POINT_COUNTS.length);
        List<double[]> bboxes = new ArrayList<>(POINT_COUNTS.length);
        for (int i = 0; i < POINT_COUNTS.length; i++) {
            refs.add(new StacItemRef(
                    "part-" + i, dir.resolve(partName(i)).toUri().toString()));
            bboxes.add(partBbox(i));
        }
        return new StacDataset("points", GEOMETRY, refs, bboxes, storages, openOptions);
    }

    private static void writeParts(Path dir) throws Exception {
        for (int i = 0; i < POINT_COUNTS.length; i++) {
            StacPointParquet.writePoints(dir.resolve(partName(i)), GEOMETRY, distinctPoints(i, POINT_COUNTS[i]));
        }
    }

    /** Points whose coordinates never repeat within or across parts, keyed off the part index. */
    private static double[][] distinctPoints(int part, int count) {
        double[][] points = new double[count][2];
        for (int i = 0; i < count; i++) {
            points[i][0] = part * 1_000.0 + i;
            points[i][1] = part * 1_000.0 + i + 0.5;
        }
        return points;
    }

    private static double[] partBbox(int part) {
        double min = part * 1_000.0;
        double max = part * 1_000.0 + POINT_COUNTS[part];
        return new double[] {min, min, max, max};
    }

    private static String partName(int part) {
        return "part-" + part + ".parquet";
    }

    private static OpenOptions fanOutOptions(int maxConcurrentFiles) {
        return OpenOptions.builder()
                .runtime(ParquetRuntime.builder()
                        .maxConcurrentFiles(maxConcurrentFiles)
                        .build())
                .build();
    }
}
