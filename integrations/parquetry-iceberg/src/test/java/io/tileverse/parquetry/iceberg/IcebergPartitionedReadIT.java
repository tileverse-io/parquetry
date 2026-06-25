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
import java.util.List;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the {@code by_category} fixture, an identity-partitioned Iceberg table (one identity partition on
 * {@code category}, values {@code a}/{@code b}/{@code c}, 100 rows each across three data files). PyIceberg retains the
 * {@code category} column physically in every data file; the read presents it from the data. The partition value also
 * feeds a tight manifest bound per file, which lets an equality predicate on {@code category} skip the two files whose
 * partition is not asked for before opening them.
 */
class IcebergPartitionedReadIT {

    private static final String TABLE = "by_category";

    @TempDir
    Path tempDir;

    @Test
    void readsAllPartitions() {
        withDataset(dataset -> assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .isEqualTo(300L));
    }

    @Test
    void presentsThePartitionColumnFromTheData() {
        withDataset(dataset -> assertThat(leafNames(dataset.schema())).contains("id", "category", "amount"));
    }

    @Test
    void equalityOnThePartitionColumnPrunesToOneFile() {
        withDataset(dataset -> {
            Predicate inB = new Predicate.Eq(ColumnPath.of("category"), new Value.StringVal("b"));

            assertThat(dataset.count(inB, ReadOptions.DEFAULTS)).isEqualTo(100L);

            DatasetExplainPlan plan = dataset.explain(inB, Projection.ALL, ReadOptions.DEFAULTS);
            assertThat(skippedFileCount(plan)).isEqualTo(2);
        });
    }

    @Test
    void rangeOnANonPartitionColumnStillScans() {
        withDataset(dataset -> {
            Predicate big = new Predicate.Gt(ColumnPath.of("amount"), new Value.LongVal(3000L));

            long matched = dataset.count(big, ReadOptions.DEFAULTS);
            assertThat(matched).isPositive().isLessThan(300L);
        });
    }

    private void withDataset(Consumer<IcebergDataset> assertions) {
        Path tableDir = extractTable();
        try (IcebergCatalog catalog = IcebergCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
    }

    private Path extractTable() {
        return TestCorpus.extractDirectory("iceberg-partitioned/by_category", tempDir.resolve(TABLE));
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
    }

    private static long skippedFileCount(DatasetExplainPlan plan) {
        return plan.files().stream()
                .map(FileExplain::outcome)
                .filter(outcome -> outcome == Outcome.SKIP)
                .count();
    }
}
