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

import java.nio.file.Path;
import java.util.HashSet;
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
 * Reads the vendored Iceberg v2 merge-on-read equality-delete table and asserts that the reader returns exactly the
 * live rows. The table is a single 10-row data file plus one equality-delete file on {@code (category, flag)} with
 * tuples {@code (category="a", flag="x")} and {@code (category=NULL, flag="y")}, leaving 8 live rows. The fixture and
 * its live-row oracle come from Apache Iceberg's own writer, cross-read with IcebergGenerics. The deleted ids are 1 and
 * 2; the live ids include the null-edge survivors 3, 4, and 5.
 */
class IcebergEqualityDeletesIT {

    private static final String TABLE = "equality";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath CATEGORY = ColumnPath.of("category");
    private static final ColumnPath FLAG = ColumnPath.of("flag");
    private static final long LIVE_COUNT = 8L;
    private static final Set<Long> DELETED_IDS = Set.of(1L, 2L);
    private static final Set<Long> LIVE_IDS = new TreeSet<>(List.of(3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L));

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
    void keepsTheNullEdgeSurvivors() {
        withDataset(dataset -> {
            Set<Long> ids = idsOf(collect(dataset, Predicate.ALWAYS_TRUE));

            assertThat(ids).contains(3L, 4L, 5L);
        });
    }

    @Test
    void appliesDeletesUnderACallerPredicate() {
        withDataset(dataset -> {
            Predicate categoryA = new Predicate.Eq(CATEGORY, new Value.StringVal("a"));

            Set<Long> ids = idsOf(collect(dataset, categoryA));

            assertThat(ids).doesNotContainAnyElementsOf(DELETED_IDS);
            assertThat(ids).contains(5L, 9L);
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
    void appliesDeletesWhenTheCallerProjectsOnlyANonEqualityColumn() {
        withDataset(dataset -> {
            Projection onlyId = Projection.ofPhysical(List.of(ID));

            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE, onlyId);

            assertThat(rows).hasSize((int) LIVE_COUNT);
            assertThat(idsOf(rows)).isEqualTo(LIVE_IDS);
            assertThat(idsOf(rows)).doesNotContainAnyElementsOf(DELETED_IDS);
        });
    }

    @Test
    void doesNotLeakTheWidenedEqualityColumnsIntoANarrowProjection() {
        withDataset(dataset -> {
            Projection onlyId = Projection.ofPhysical(List.of(ID));

            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE, onlyId);

            assertThat(rows).hasSize((int) LIVE_COUNT).allSatisfy(row -> {
                assertThat(row.schema().find(ID)).isPresent();
                assertThat(row.schema().find(CATEGORY)).isEmpty();
                assertThat(row.schema().find(FLAG)).isEmpty();
            });
        });
    }

    @Test
    void appliesDeletesWhenTheProjectionPartiallyOverlapsTheEqualityColumns() {
        withDataset(dataset -> {
            Projection idAndCategory = Projection.ofPhysical(List.of(ID, CATEGORY));

            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE, idAndCategory);

            assertThat(idsOf(rows)).isEqualTo(LIVE_IDS);
            assertThat(rows).allSatisfy(row -> {
                assertThat(row.schema().find(ID)).isPresent();
                assertThat(row.schema().find(CATEGORY)).isPresent();
                assertThat(row.schema().find(FLAG)).isEmpty();
            });
        });
    }

    @Test
    void theMaterializerReadAppliesDeletesAndDoesNotLeakTheEqualityColumns() {
        withDataset(dataset -> {
            Projection onlyId = Projection.ofPhysical(List.of(ID));

            List<MaterializedRow> rows = collectMaterialized(dataset, onlyId);

            Set<Long> ids = rows.stream().map(MaterializedRow::id).collect(Collectors.toCollection(TreeSet::new));
            assertThat(ids).isEqualTo(LIVE_IDS).doesNotContainAnyElementsOf(DELETED_IDS);
            assertThat(rows).allSatisfy(row -> {
                assertThat(row.hasColumn(ID)).isTrue();
                assertThat(row.hasColumn(CATEGORY)).isFalse();
                assertThat(row.hasColumn(FLAG)).isFalse();
            });
        });
    }

    private void withDataset(Consumer<ParquetDataset> assertions) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/equality", tempDir.resolve(TABLE));
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
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

    private static List<MaterializedRow> collectMaterialized(ParquetDataset dataset, Projection projection) {
        try (Stream<MaterializedRow> rows =
                dataset.read(Predicate.ALWAYS_TRUE, projection, MaterializedRow::of, ReadOptions.DEFAULTS)) {
            return rows.toList();
        }
    }

    private static Set<Long> idsOf(List<ParquetRecord> rows) {
        return rows.stream().map(row -> row.getLong(ID)).collect(Collectors.toCollection(TreeSet::new));
    }

    /** A row materialized through the {@link io.tileverse.parquetry.materializer.Materializer} read overload. */
    private record MaterializedRow(long id, Set<ColumnPath> columns) {

        static MaterializedRow of(ParquetSchema projectedSchema, ParquetRecord row) {
            Set<ColumnPath> present = new HashSet<>();
            for (ColumnPath candidate : List.of(ID, CATEGORY, FLAG)) {
                if (projectedSchema.find(candidate).isPresent()) {
                    present.add(candidate);
                }
            }
            return new MaterializedRow(row.getLong(ID), present);
        }

        boolean hasColumn(ColumnPath column) {
            return columns.contains(column);
        }
    }
}
