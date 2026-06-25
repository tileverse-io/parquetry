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

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.tileverse.storage.Storage;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.Totals;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Backend-agnostic read assertions over the {@code v3_geometry} testbed table reached through a tileverse
 * {@link Storage}. Subclasses supply a {@link Backend} that makes the table readable through some Storage (a local
 * directory or an uploaded object-store prefix, for example); the assertions here prove the catalog reads, counts, and
 * prunes identically regardless of the backend.
 *
 * <p>The class has no {@code Test}/{@code IT} suffix, hence neither Surefire nor Failsafe runs it directly.
 */
abstract class AbstractIcebergStorageRead {

    protected static final String TABLE = "v3_geometry";
    protected static final String TABLE_LOCATION = "file:///iceberg-geo-testbed/v3_geometry";
    protected static final long EXPECTED_TOTAL_ROWS = 10_000L;
    protected static final Predicate CALIFORNIA =
            new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), Bbox.of2d(-125.0, 32.0, -115.0, 42.0));
    protected static final Predicate FAR_OFFSHORE =
            new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), Bbox.of2d(160.0, -80.0, 170.0, -70.0));

    /**
     * A {@link Storage} rooted at the table's physical bytes, plus the table's logical location URI. Closing the
     * Backend closes the Storage.
     */
    protected record Backend(Storage storage, String tableLocation) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            storage.close();
        }
    }

    /** Subclasses build a Backend by making the {@code v3_geometry} table readable through a Storage. */
    protected abstract Backend openBackend() throws Exception;

    /**
     * The options used to open the catalog. Backends that cannot list (HTTP) override this to pin an explicit metadata
     * location.
     */
    protected IcebergOptions catalogOptions() {
        return IcebergOptions.defaults();
    }

    /** Extract the {@code v3_geometry} table into {@code targetDir} and return its table directory. */
    protected static Path extractTable(Path targetDir) {
        return TestCorpus.extractDirectory("iceberg-geo-testbed", targetDir).resolve(TABLE);
    }

    @Test
    void fullScanCountsEveryRow() throws Exception {
        withDataset(dataset -> {
            long total = dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);
            assertThat(total).isEqualTo(EXPECTED_TOTAL_ROWS);
        });
    }

    @Test
    void spatialQueryReturnsTheSurvivingRows() throws Exception {
        withDataset(dataset -> {
            long matched = countRows(dataset, CALIFORNIA);
            assertThat(matched).isPositive().isEqualTo(dataset.count(CALIFORNIA, ReadOptions.DEFAULTS));
        });
    }

    @Test
    void explainReportsManifestPruning() throws Exception {
        withDataset(dataset -> {
            Totals totals = explainTotals(dataset, CALIFORNIA);
            assertThat(totals.filesTotal()).isEqualTo(10);
            assertThat(totals.filesKept()).isEqualTo(1);
            assertThat(totals.filesSkipped()).isEqualTo(9);
        });
    }

    @Test
    void explainSkipsEveryFileForADisjointQuery() throws Exception {
        withDataset(dataset -> {
            Totals totals = explainTotals(dataset, FAR_OFFSHORE);
            assertThat(totals.filesSkipped()).isEqualTo(totals.filesTotal()).isEqualTo(10);
        });
    }

    private long countRows(ParquetDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private Totals explainTotals(ParquetDataset dataset, Predicate predicate) {
        DatasetExplainPlan plan = dataset.explain(predicate, Projection.ALL, ReadOptions.DEFAULTS);
        return plan.totals();
    }

    private void withDataset(Consumer<ParquetDataset> assertions) throws Exception {
        try (Backend backend = openBackend();
                IcebergCatalog catalog = IcebergCatalog.open(
                        backend.tableLocation(),
                        StorageIcebergFileIO.over(backend.storage(), backend.tableLocation()),
                        catalogOptions())) {
            ParquetDataset dataset = catalog.dataset(catalog.datasets().get(0));
            assertions.accept(dataset);
        }
    }
}
