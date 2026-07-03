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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves {@code ParquetDataset.explain}/{@code explainAnalyze} over an Iceberg table report the file dimension: a
 * regional bbox query keeps only the files whose manifest bounds meet it and marks the rest skipped with a reason, a
 * query that eliminates every file reports all files skipped, and analyze fills in the survivor's execution stats.
 */
class IcebergExplainTest {

    private static final String TABLE = "v3_geometry";
    private static final String GEOM_COLUMN = "geom";

    private static final Predicate CALIFORNIA =
            new Predicate.Spatial.BboxIntersects(ColumnPath.of(GEOM_COLUMN), Bbox.of2d(-125.0, 32.0, -115.0, 42.0));

    @TempDir
    Path tempDir;

    @Test
    void explainReportsKeptAndSkippedFiles() {
        Path tableDir = extractTable();
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(TABLE);

            DatasetExplainPlan plan = dataset.explain(CALIFORNIA, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.totals().filesTotal()).isEqualTo(10);
            assertThat(plan.totals().filesKept()).isEqualTo(1);
            assertThat(plan.totals().filesSkipped()).isEqualTo(9);
            assertThat(plan.totals().rowsSkipped()).hasValue(9000);
            assertThat(plan.files())
                    .filteredOn(file -> file.outcome() == Outcome.KEEP)
                    .singleElement()
                    .satisfies(file -> assertThat(file.rowGroups()).isPresent());
            assertThat(plan.files())
                    .filteredOn(file -> file.outcome() == Outcome.SKIP)
                    .allSatisfy(file -> assertThat(file.reason()).isNotBlank());
        }
    }

    @Test
    void explainOverZeroSurvivorsMarksEveryFileSkipped() {
        Path tableDir = extractTable();
        Predicate farOffshore =
                new Predicate.Spatial.BboxIntersects(ColumnPath.of(GEOM_COLUMN), Bbox.of2d(160.0, -80.0, 170.0, -70.0));
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(TABLE);

            DatasetExplainPlan plan = dataset.explain(farOffshore, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.totals().filesKept()).isZero();
            assertThat(plan.totals().filesSkipped()).isEqualTo(10);
            assertThat(plan.files())
                    .allSatisfy(file -> assertThat(file.rowGroups()).isEmpty());
        }
    }

    @Test
    void explainAnalyzeFillsExecutionForTheSurvivor() {
        Path tableDir = extractTable();
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            ParquetDataset dataset = catalog.dataset(TABLE);

            DatasetExplainPlan plan = dataset.explainAnalyze(CALIFORNIA, Projection.ALL, ReadOptions.DEFAULTS);

            assertThat(plan.files())
                    .filteredOn(file -> file.outcome() == Outcome.KEEP)
                    .singleElement()
                    .satisfies(file -> assertThat(file.rowGroups().orElseThrow().execution())
                            .isPresent());
        }
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }
}
