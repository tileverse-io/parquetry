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
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.filter.Value;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the vendored Iceberg v3 deletion-vector table and asserts the reader returns exactly the live rows. The table
 * is a single 100-row data file (row position equals {@code id}) plus one Puffin deletion vector removing positions
 * {@code 10..14, 50, 73, 97, 98, 99}, leaving 90 live rows. The fixture and its 90-row oracle come from Apache
 * Iceberg's own deletion-vector writer.
 */
class IcebergDeletionVectorsIT {

    private static final String TABLE = "deletion-vectors";
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath CATEGORY = ColumnPath.of("category");
    private static final long ROW_COUNT = 100L;
    private static final long LIVE_COUNT = 90L;

    @TempDir
    Path tempDir;

    @Test
    void readsOnlyTheLiveRows() {
        withDataset(dataset -> {
            List<ParquetRecord> rows = collect(dataset, Predicate.ALWAYS_TRUE);

            assertThat(rows).hasSize((int) LIVE_COUNT);
            assertThat(idsOf(rows)).isEqualTo(liveIds());
        });
    }

    @Test
    void neverReturnsADeletedRow() {
        withDataset(dataset -> {
            Set<Long> ids = idsOf(collect(dataset, Predicate.ALWAYS_TRUE));

            assertThat(ids).hasSize((int) LIVE_COUNT).doesNotContainAnyElementsOf(deletedIds());
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
            long read = collect(dataset, Predicate.ALWAYS_TRUE).size();
            long counted = dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS);

            assertThat(counted).isEqualTo(read);
        });
    }

    @Test
    void appliesDeletesUnderACallerPredicate() {
        withDataset(dataset -> {
            Predicate categoryA = new Predicate.Eq(CATEGORY, new Value.StringVal("a"));

            Set<Long> ids = idsOf(collect(dataset, categoryA));

            assertThat(ids).isEqualTo(liveCategoryAIds());
        });
    }

    private void withDataset(Consumer<ParquetDataset> assertions) {
        Path tableDir = TestCorpus.extractDirectory("iceberg-deletes/deletion-vectors", tempDir.resolve(TABLE));
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

    /**
     * The live ids whose row has {@code category == "a"}. The fixture cycles category over a, b, c by {@code id % 3};
     * category a is every id divisible by 3, and the deletion vector then removes 12 and 99 from that set.
     */
    private static Set<Long> liveCategoryAIds() {
        Set<Long> ids = new TreeSet<>();
        LongStream.range(0, ROW_COUNT).filter(id -> id % 3 == 0).forEach(ids::add);
        ids.removeAll(deletedIds());
        return ids;
    }

    private static Set<Long> deletedIds() {
        return new TreeSet<>(Set.of(10L, 11L, 12L, 13L, 14L, 50L, 73L, 97L, 98L, 99L));
    }
}
