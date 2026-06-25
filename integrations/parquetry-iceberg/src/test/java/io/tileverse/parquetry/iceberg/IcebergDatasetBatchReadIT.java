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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.batch.ParquetRecordBatch;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.Dataset;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves the {@code v3_geometry} Iceberg table reads columnar batches through the {@link Dataset} SPI (not the concrete
 * {@code IcebergDataset}), establishing that {@code readBatches} is reachable on the SPI. Batch reads are
 * pushdown-only: a California bbox prunes data files to a positive subset, but surviving pages still hold non-matching
 * rows, hence the pruned total is asserted to be strictly less than the unfiltered total, never an exact match count.
 */
class IcebergDatasetBatchReadIT {

    private static final String TABLE = "v3_geometry";
    private static final Bbox CALIFORNIA = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);

    @TempDir
    Path tempDir;

    @Test
    void readsColumnarBatchesThroughTheSpiWithDataFilePruning() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve("t"));
        Path tableDir = root.resolve(TABLE);

        try (IcebergCatalog catalog = IcebergCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            Dataset dataset = catalog.dataset(TABLE);

            long unfilteredTotal = sumBatchRows(dataset, Predicate.ALWAYS_TRUE);
            assertThat(unfilteredTotal).isEqualTo(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS));

            Predicate california = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), CALIFORNIA);
            long prunedTotal = sumBatchRows(dataset, california);
            assertThat(prunedTotal).isPositive().isLessThan(unfilteredTotal);
        }
    }

    private static long sumBatchRows(Dataset dataset, Predicate predicate) {
        try (Stream<ParquetRecordBatch> batches =
                dataset.readBatches(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return batches.mapToLong(batch -> {
                        try (batch) {
                            return batch.rowCount();
                        }
                    })
                    .sum();
        }
    }
}
