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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.ParquetSchemaException;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Reads the vendored Iceberg v3 row-lineage tables and asserts the two reserved metadata columns a caller may project
 * by name: {@code _row_id} and {@code _last_updated_sequence_number}. Neither is part of the table's user schema; a
 * read that does not name them sees the schema unchanged.
 *
 * <p>The {@code fresh} table has no materialized lineage: three data files ({@code id} 100..114, {@code _row_id} 0..14,
 * {@code value = id * 1.5}) span a multi-file manifest (files a and b share one manifest, where file b's
 * {@code _row_id} base 5 is recovered only by cumulative within-manifest inheritance) and a per-file
 * data-sequence-number boundary (files a and b at sequence 1, file c at sequence 2). The {@code materialized} table
 * stores {@code _row_id} physically for only some of one file's rows (a mix of stored and null cells); the per-row
 * coalesce keeps a stored cell and falls back to the computed value for a null cell. Every expected value comes from
 * Apache Iceberg's own reader.
 */
class IcebergRowLineageIT {

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath ROW_ID = ColumnPath.of("_row_id");
    private static final ColumnPath LAST_UPDATED_SEQUENCE_NUMBER = ColumnPath.of("_last_updated_sequence_number");
    private static final long FRESH_ROW_COUNT = 15L;
    private static final long ID_TO_ROW_ID_OFFSET = 100L;
    private static final long LAST_FILE_A_B_ID = 109L;

    @TempDir
    Path tempDir;

    @Test
    void synthesizesRowIdFromFirstRowIdPlusPositionAcrossFiles() {
        withFresh(dataset -> {
            List<ParquetRecord> rows = read(dataset, Projection.ofPhysical(List.of(ID, ROW_ID)));

            assertThat(rows).hasSize((int) FRESH_ROW_COUNT);
            assertThat(rows)
                    .allSatisfy(
                            row -> assertThat(row.getLong(ROW_ID)).isEqualTo(row.getLong(ID) - ID_TO_ROW_ID_OFFSET));
        });
    }

    @Test
    void synthesizesLastUpdatedSequenceNumberFromThePerFileDataSequenceNumber() {
        withFresh(dataset -> {
            List<ParquetRecord> rows = read(dataset, Projection.ofPhysical(List.of(ID, LAST_UPDATED_SEQUENCE_NUMBER)));

            assertThat(rows).hasSize((int) FRESH_ROW_COUNT);
            assertThat(rows).allSatisfy(row -> {
                long expected = row.getLong(ID) <= LAST_FILE_A_B_ID ? 1L : 2L;
                assertThat(row.getLong(LAST_UPDATED_SEQUENCE_NUMBER)).isEqualTo(expected);
            });
        });
    }

    @Test
    void keepsTheLineageColumnsOutOfTheSchemaAndTheDefaultRead() {
        withFresh(dataset -> {
            assertThat(leafNames(dataset.schema())).doesNotContain("_row_id", "_last_updated_sequence_number");

            List<ParquetRecord> rows = read(dataset, Projection.ALL);

            assertThat(rows).hasSize((int) FRESH_ROW_COUNT);
            assertThat(rows)
                    .allSatisfy(row -> assertThat(leafNames(row.schema()))
                            .doesNotContain("_row_id", "_last_updated_sequence_number"));
        });
    }

    @Test
    void composesTheRowIdOutputWithACallerPredicate() {
        withFresh(dataset -> {
            Predicate keepsOneRow = new Predicate.Eq(ID, new Value.LongVal(107L));

            List<ParquetRecord> rows = read(dataset, keepsOneRow, Projection.ofPhysical(List.of(ID, ROW_ID)));

            assertThat(rows).hasSize(1);
            assertThat(rows.getFirst().getLong(ROW_ID)).isEqualTo(7L);
        });
    }

    @Test
    void coalescesTheMaterializedRowIdPerRow() {
        withMaterialized(dataset -> {
            List<ParquetRecord> rows = read(dataset, Projection.ofPhysical(List.of(ID, ROW_ID)));

            assertThat(rows).hasSize(10);
            assertThat(rows).allSatisfy(row -> {
                long id = row.getLong(ID);
                // File 1 (ids 5..9) stores _row_id only for ids 6 and 8; its other cells are null and fall back to the
                // computed base 5 + position, which equals the id here. File 0 (ids 0..4) is fully synthesized.
                long expected = (id == 6L || id == 8L) ? 1000L + (id - 5L) : id;
                assertThat(row.getLong(ROW_ID)).isEqualTo(expected);
            });
        });
    }

    @Test
    void keepsTrueRowPositionsForNullCellsWhenACallerPredicateDropsRows() {
        withMaterialized(dataset -> {
            // Dropping id 6 (a stored cell) leaves file 1 survivors 5,7,8,9 at true within-file positions 0,2,3,4. A
            // null cell must still resolve to base 5 + its TRUE position (id 7 -> 7, id 9 -> 9), not a compacted index.
            Predicate dropsAStoredCell = new Predicate.NotEq(ID, new Value.LongVal(6L));

            List<ParquetRecord> rows = read(dataset, dropsAStoredCell, Projection.ofPhysical(List.of(ID, ROW_ID)));

            assertThat(rows).hasSize(9);
            assertThat(rows).allSatisfy(row -> {
                long id = row.getLong(ID);
                long expected = (id == 8L) ? 1003L : id;
                assertThat(row.getLong(ROW_ID)).isEqualTo(expected);
            });
        });
    }

    @Test
    void keepsAMaterializedRowIdLeafOutOfADefaultRead() {
        withMaterialized(dataset -> {
            List<ParquetRecord> rows = read(dataset, Projection.ALL);

            assertThat(rows).hasSize(10);
            assertThat(rows)
                    .allSatisfy(row -> assertThat(leafNames(row.schema()))
                            .doesNotContain("_row_id", "_last_updated_sequence_number"));
        });
    }

    @Test
    void synthesizesRowIdThroughALineageOnlyProjection() {
        withFresh(dataset -> {
            List<ParquetRecord> rows = read(dataset, Projection.ofPhysical(List.of(ROW_ID)));

            assertThat(rows).hasSize((int) FRESH_ROW_COUNT);
            assertThat(rowIdsOf(rows)).isEqualTo(rowIdRange(0L, FRESH_ROW_COUNT));
        });
    }

    @Test
    void doesNotSynthesizeLineageOnAPreV3ReconciledRead() {
        // The equality-evolved table is format version 2: row lineage does not exist there, and a reserved name in
        // the projection is an ordinary (absent) column, not a lineage request. The reconciled read still presents
        // every table field with the deletes applied; no lineage column is fabricated.
        withTable("iceberg-deletes/equality-evolved", "equality-evolved", dataset -> {
            List<ParquetRecord> rows = read(dataset, Projection.ofPhysical(List.of(ID, LAST_UPDATED_SEQUENCE_NUMBER)));

            assertThat(idsOf(rows)).containsExactly(5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 13L, 14L);
            assertThat(rows)
                    .allSatisfy(row -> assertThat(leafNames(row.schema()))
                            .doesNotContain("_row_id", "_last_updated_sequence_number"));
        });
    }

    @Test
    void treatsAReservedNameAsAnOrdinaryColumnOnAPreV3PassThroughRead() {
        // On a pre-v3 pass-through file a reserved lineage name is not rewritten; projecting one behaves exactly
        // like projecting any column the file does not have.
        withTable("iceberg-deletes/positional", "positional", dataset -> {
            Projection lineageNamed = Projection.ofPhysical(List.of(ID, ROW_ID));

            assertThatThrownBy(() -> read(dataset, lineageNamed)).isInstanceOf(ParquetSchemaException.class);
        });
    }

    private void withFresh(Consumer<ParquetDataset> assertions) {
        withTable("iceberg-row-lineage/fresh", "fresh", assertions);
    }

    private void withMaterialized(Consumer<ParquetDataset> assertions) {
        withTable("iceberg-row-lineage/materialized", "materialized", assertions);
    }

    private void withTable(String resource, String name, Consumer<ParquetDataset> assertions) {
        Path tableDir = TestCorpus.extractDirectory(resource, tempDir.resolve(name));
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            assertions.accept(catalog.dataset(name));
        }
    }

    private static List<ParquetRecord> read(ParquetDataset dataset, Projection projection) {
        return read(dataset, Predicate.ALWAYS_TRUE, projection);
    }

    private static List<ParquetRecord> read(ParquetDataset dataset, Predicate predicate, Projection projection) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, projection, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).toList();
        }
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
    }

    private static Set<Long> idsOf(List<ParquetRecord> rows) {
        return rows.stream().map(row -> row.getLong(ID)).collect(Collectors.toCollection(TreeSet::new));
    }

    private static Set<Long> rowIdsOf(List<ParquetRecord> rows) {
        return rows.stream().map(row -> row.getLong(ROW_ID)).collect(Collectors.toCollection(TreeSet::new));
    }

    private static Set<Long> rowIdRange(long startInclusive, long count) {
        return LongStream.range(startInclusive, startInclusive + count)
                .boxed()
                .collect(Collectors.toCollection(TreeSet::new));
    }
}
