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
package io.tileverse.parquetry.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
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
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves that equality deletes keyed on a uuid and on a decimal are applied: the {@code scalars} fixture removes id 2
 * through a delete tuple on {@code u} and id 7 through a delete tuple on {@code dec}, each scoped to its partition.
 */
class IcebergScalarTypeEqualityDeletesIT {

    private static final String TABLE = "scalars";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath U = ColumnPath.of("u");
    private static final ColumnPath DEC = ColumnPath.of("dec");
    private static final List<Long> LIVE_IDS = List.of(1L, 3L, 4L, 5L, 6L, 8L, 9L, 10L, 11L, 12L);

    @TempDir
    Path tempDir;

    @Test
    void neverReturnsARowDeletedByAUuidOrDecimalKey() {
        withDataset(dataset -> assertThat(idsOf(dataset, Predicate.ALWAYS_TRUE, Projection.ALL))
                .isEqualTo(LIVE_IDS));
    }

    @Test
    void countMatchesTheLiveRowCount() {
        withDataset(dataset -> assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .isEqualTo(LIVE_IDS.size()));
    }

    @Test
    void keyLookupsHonorTheDeletes() {
        withDataset(dataset -> {
            Predicate deletedUuid = new Predicate.Eq(U, new Value.UuidVal(IcebergScalarTypesReadIT.uuidOf(2)));
            assertThat(idsOf(dataset, deletedUuid, Projection.ALL)).isEmpty();

            Predicate deletedDecimal = new Predicate.Eq(DEC, new Value.DecimalVal(new BigDecimal("1.25")));
            assertThat(idsOf(dataset, deletedDecimal, Projection.ALL)).isEmpty();

            Predicate liveDecimal = new Predicate.Eq(DEC, new Value.DecimalVal(new BigDecimal("2.50")));
            assertThat(idsOf(dataset, liveDecimal, Projection.ALL)).containsExactly(8L);
        });
    }

    @Test
    void appliesDeletesWhenTheCallerProjectsOnlyTheIdColumn() {
        withDataset(dataset -> assertThat(idsOf(dataset, Predicate.ALWAYS_TRUE, Projection.ofPhysical(List.of(ID))))
                .isEqualTo(LIVE_IDS));
    }

    private void withDataset(Consumer<IcebergDataset> assertions) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-scalar-types/scalars", tempDir.resolve(TABLE));
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
    }

    private static List<Long> idsOf(IcebergDataset dataset, Predicate predicate, Projection projection) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, projection, ReadOptions.DEFAULTS)) {
            return rows.map(row -> row.getLong(ID)).sorted().toList();
        }
    }
}
