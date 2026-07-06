/*
 * (c) Copyright 2025 Multiversio LLC. All rights reserved.
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
import java.util.stream.LongStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.dataset.explain.DatasetExplainPlan;
import io.tileverse.parquetry.dataset.explain.FileExplain;
import io.tileverse.parquetry.dataset.explain.Outcome;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.filter.explain.RowGroupOutcome;
import io.tileverse.parquetry.filter.explain.RowGroupPlan;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the vendored Iceberg v2 merge-on-read positional-delete table and asserts that the reader returns exactly the
 * live rows. The table is a single 1000-row data file (ten 100-row row groups, row position equals {@code id}) plus one
 * position-delete file removing row group 3 entirely (positions 300..399) and the rows that span the row-group-5/6
 * boundary (positions 595..604), leaving 890 live rows. The fixture and its 890-row oracle come from Apache Iceberg's
 * own writer.
 */
class IcebergPositionalDeletesIT {

    private static final String TABLE = "positional";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath VALUE = ColumnPath.of("value");
    private static final long ROW_COUNT = 1000L;
    private static final long LIVE_COUNT = 890L;

    @TempDir
    Path tempDir;

    @Test
    void readsOnlyTheLiveRows() {
        withDataset(dataset -> {
            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE);

            assertThat(rows).hasSize((int) LIVE_COUNT);
            assertThat(idsOf(rows)).isEqualTo(liveIds());
            assertThat(rows).allSatisfy(row -> assertThat(row.getDouble(VALUE)).isEqualTo(row.getLong(ID) * 1.5));
        });
    }

    @Test
    void neverReturnsADeletedRow() {
        withDataset(dataset -> {
            Set<Long> ids = idsOf(collect(dataset, Predicate.ALWAYS_TRUE));

            assertThat(ids).doesNotContainAnyElementsOf(deletedIds());
        });
    }

    @Test
    void appliesDeletesUnderACallerPredicate() {
        withDataset(dataset -> {
            Predicate categoryB = new Predicate.Eq(ColumnPath.of("category"), new Value.StringVal("b"));

            Set<Long> ids = idsOf(collect(dataset, categoryB));

            assertThat(ids).isEqualTo(liveCategoryBIds());
        });
    }

    @Test
    void countMatchesTheLiveRowCount() {
        withDataset(dataset -> assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                .isEqualTo(LIVE_COUNT));
    }

    @Test
    void eliminatesAFullyDeletedRowGroupAndKeepsUntouchedGroupsWhole() {
        withDataset(dataset -> {
            List<RowGroupPlan> groups = rowGroupPlans(dataset);

            assertThat(groups).hasSize(10);
            assertThat(groups)
                    .filteredOn(group -> group.outcome() == RowGroupOutcome.ELIMINATED)
                    .singleElement()
                    .satisfies(group -> assertThat(group.index()).isEqualTo(3));
            assertThat(byIndex(groups, 0).outcome()).isIn(RowGroupOutcome.FULL, RowGroupOutcome.MATCHED);
            assertThat(byIndex(groups, 5).outcome()).isNotEqualTo(RowGroupOutcome.ELIMINATED);
        });
    }

    private List<RowGroupPlan> rowGroupPlans(ParquetDataset dataset) {
        DatasetExplainPlan plan = dataset.explain(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS);
        FileExplain file = plan.files().stream()
                .filter(candidate -> candidate.outcome() == Outcome.KEEP)
                .findFirst()
                .orElseThrow();
        return file.rowGroups().orElseThrow().rowGroups();
    }

    private static RowGroupPlan byIndex(List<RowGroupPlan> groups, int index) {
        return groups.stream()
                .filter(group -> group.index() == index)
                .findFirst()
                .orElseThrow();
    }

    private void withDataset(Consumer<ParquetDataset> assertions) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/positional", tempDir.resolve(TABLE));
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            assertions.accept(catalog.dataset(TABLE));
        }
    }

    private static List<ParquetRecord> collect(ParquetDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).toList();
        }
    }

    private static Set<Long> idsOf(List<ParquetRecord> rows) {
        return rows.stream().map(row -> row.getLong(ID)).collect(Collectors.toCollection(TreeSet::new));
    }

    private static Set<Long> liveIds() {
        Set<Long> ids = new TreeSet<>();
        LongStream.range(0, ROW_COUNT).forEach(ids::add);
        ids.removeAll(deletedIds());
        return ids;
    }

    private static Set<Long> liveCategoryBIds() {
        Set<Long> ids = new TreeSet<>();
        LongStream.range(0, ROW_COUNT).filter(id -> id % 3 == 1).forEach(ids::add);
        ids.removeAll(deletedIds());
        return ids;
    }

    private static Set<Long> deletedIds() {
        Set<Long> deleted = new TreeSet<>();
        LongStream.range(300, 400).forEach(deleted::add);
        LongStream.range(595, 605).forEach(deleted::add);
        return deleted;
    }
}
