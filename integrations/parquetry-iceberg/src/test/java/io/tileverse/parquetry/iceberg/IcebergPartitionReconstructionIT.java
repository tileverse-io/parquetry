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
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the {@code by_category_omitted} fixture, an identity-partitioned Iceberg table whose data files physically drop
 * the {@code category} column (the manifest still records the partition tuple). The kept columns {@code id} and
 * {@code amount} keep their field ids 1 and 3. Because the partition column is absent from the data files, presenting
 * it and filtering on it can only come from reconstructing the value out of the manifest partition tuple. This proves
 * that reconstruction path end to end, distinguishing it from {@link IcebergPartitionedReadIT}, where PyIceberg
 * retained the column physically.
 */
class IcebergPartitionReconstructionIT {

    private static final String TABLE = "by_category_omitted";

    @TempDir
    Path tempDir;

    @Test
    void readsEveryRowAcrossAllPartitions() {
        withDataset(dataset -> assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .isEqualTo(300L));
    }

    @Test
    void presentsTheOmittedPartitionColumnInTheSchema() {
        withDataset(dataset -> assertThat(leafNames(dataset.schema())).contains("id", "category", "amount"));
    }

    @Test
    void reconstructsThePartitionValueForEveryMatchedRow() {
        withDataset(dataset -> {
            Predicate inB = new Predicate.Eq(ColumnPath.of("category"), new Value.StringVal("b"));

            List<ParquetRecord> bRows = collect(dataset, inB);

            assertThat(bRows).hasSize(100);
            assertThat(bRows)
                    .allSatisfy(row ->
                            assertThat(row.getString(ColumnPath.of("category"))).isEqualTo("b"));
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
        return TestCorpus.extractDirectory("iceberg-partitioned/by_category_omitted", tempDir.resolve(TABLE));
    }

    private static List<ParquetRecord> collect(IcebergDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).toList();
        }
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
    }
}
