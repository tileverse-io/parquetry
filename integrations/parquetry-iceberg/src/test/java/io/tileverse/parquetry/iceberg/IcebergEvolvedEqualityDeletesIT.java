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
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the vendored Iceberg v2 merge-on-read equality-delete table whose data files physically omit the partition
 * column the delete is keyed on, and asserts that the reader returns exactly the live rows.
 *
 * <p>The table is {@code events(id, category, value)} partitioned by {@code identity(category)}, with three 5-row data
 * files (one per partition a/b/c, ids 0..14, {@code value = id * 1.5}). Every data file drops the stored
 * {@code category} column; the manifest still records the partition tuple. One equality-delete file in partition
 * {@code category=a} is keyed on {@code category} alone with the tuple {@code (category="a")}, removing the five
 * category-a rows (ids 0..4) and leaving ten live rows (ids 5..14).
 *
 * <p>This exercises the reconciled path: parquetry reconstructs {@code category} as a constant out of the manifest
 * partition tuple, and the equality anti-predicate folds against that reconstruction constant. The category-a files
 * fold to always-false and are skipped; the b/c files fold to always-true and are kept. The fixture and its live-row
 * oracle come from Apache Iceberg's own writer, cross-read with IcebergGenerics.
 */
class IcebergEvolvedEqualityDeletesIT {

    private static final String TABLE = "equality-evolved";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath CATEGORY = ColumnPath.of("category");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final long LIVE_COUNT = 10L;
    private static final Set<Long> DELETED_IDS = Set.of(0L, 1L, 2L, 3L, 4L);
    private static final Set<Long> LIVE_IDS = new TreeSet<>(List.of(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L));

    @TempDir
    Path tempDir;

    @Test
    void readsOnlyTheLiveRows() {
        withDataset(dataset -> {
            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE);

            assertThat(rows).hasSize((int) LIVE_COUNT);
            assertThat(idsOf(rows)).isEqualTo(LIVE_IDS);
        });
    }

    @Test
    void neverReturnsADeletedRow() {
        withDataset(dataset -> {
            Set<Long> ids = idsOf(collect(dataset, Predicate.ALWAYS_TRUE));

            assertThat(ids).hasSize((int) LIVE_COUNT).doesNotContainAnyElementsOf(DELETED_IDS);
        });
    }

    @Test
    void countMatchesTheLiveRowCount() {
        withDataset(dataset -> assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .isEqualTo(LIVE_COUNT));
    }

    @Test
    void countMatchesTheReadRowCount() {
        withDataset(dataset -> {
            long readRows = collect(dataset, Predicate.ALWAYS_TRUE).size();

            assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(readRows);
        });
    }

    @Test
    void reconstructsTheOmittedPartitionColumnForEveryLiveRow() {
        withDataset(dataset -> {
            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE);

            assertThat(rows)
                    .allSatisfy(row -> assertThat(row.getString(CATEGORY)).isIn("b", "c"));
        });
    }

    @Test
    void readsTheSameLiveSetThroughANarrowProjectionExcludingTheEqualityColumn() {
        withDataset(dataset -> {
            Projection idAndValue = Projection.ofPhysical(List.of(ID, VALUE));

            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE, idAndValue);

            assertThat(rows).hasSize((int) LIVE_COUNT);
            assertThat(idsOf(rows)).isEqualTo(LIVE_IDS).doesNotContainAnyElementsOf(DELETED_IDS);
        });
    }

    @Test
    void readsTheCorrectValueColumnThroughANarrowProjectionExcludingTheEqualityColumn() {
        withDataset(dataset -> {
            Projection idAndValue = Projection.ofPhysical(List.of(ID, VALUE));

            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE, idAndValue);

            assertThat(rows).allSatisfy(row -> assertThat(row.getDouble(VALUE)).isEqualTo(row.getLong(ID) * 1.5));
        });
    }

    @Test
    void presentsTheCorrectValueColumnForASurvivingRow() {
        withDataset(dataset -> {
            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE);

            assertThat(rows).allSatisfy(row -> assertThat(row.getDouble(VALUE)).isEqualTo(row.getLong(ID) * 1.5));
        });
    }

    @Test
    void presentsTheReconstructedPartitionColumnInTheSchema() {
        withDataset(dataset -> assertThat(leafNames(dataset.schema())).contains("id", "category", "value"));
    }

    @Test
    void appliesDeletesUnderACallerPredicateOnTheReconciledPath() {
        withDataset(dataset -> {
            Predicate keepsALiveRow = new Predicate.Eq(ID, new Value.LongVal(7L));
            assertThat(idsOf(collect(dataset, keepsALiveRow))).containsExactly(7L);

            Predicate asksForADeletedRow = new Predicate.Eq(ID, new Value.LongVal(2L));
            assertThat(collect(dataset, asksForADeletedRow)).isEmpty();
        });
    }

    private void withDataset(Consumer<ParquetDataset> assertions) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/equality-evolved", tempDir.resolve(TABLE));
        try (IcebergCatalog catalog = IcebergCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            assertions.accept(catalog.dataset(TABLE));
        }
    }

    private static List<ParquetRecord> collect(ParquetDataset dataset, Predicate predicate) {
        return collect(dataset, predicate, Projection.ALL);
    }

    private static List<ParquetRecord> collect(ParquetDataset dataset, Predicate predicate, Projection projection) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, projection, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).toList();
        }
    }

    private static Set<Long> idsOf(List<ParquetRecord> rows) {
        return rows.stream().map(row -> row.getLong(ID)).collect(Collectors.toCollection(TreeSet::new));
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
    }
}
