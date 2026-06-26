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
import java.util.Optional;
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
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves an evolved Iceberg table reads its data files correctly by field id. The {@code v3_minimal} corpus table has a
 * flat schema (id#1:string, n#2:int) over ten data files of ten thousand rows. Each test evolves that schema in place
 * over the same files (a fresh metadata version with a renamed, added, dropped, reordered, or promoted column reusing
 * the original field ids), then opens the evolved table and asserts the read reconciles each data file back to the
 * evolved schema by field id.
 *
 * <p>The data files keep their original footer field ids; the evolved schema reuses those ids for renamed and promoted
 * fields and introduces a new id for the added field. That is exactly what {@link IcebergReconciliation} maps.
 */
class IcebergFieldIdEvolutionIT {

    private static final String TABLE = "v3_minimal";
    private static final long EXPECTED_TOTAL_ROWS = 10_000L;

    private static final IcebergField ID = new IcebergField(1, "id", "string", false);
    private static final IcebergField N = new IcebergField(2, "n", "int", false);

    @TempDir
    Path tempDir;

    @Test
    void unevolvedTableReadsThroughThePassThroughPath() {
        Path tableDir = extractTable();

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "n");
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);
        });
    }

    @Test
    void renamePresentsTheNewNameAndFiltersOnIt() {
        IcebergField renamed = new IcebergField(N.fieldId(), "count", "int", false);
        Path tableDir = evolve(List.of(ID, renamed));

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "count");
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);
            assertThat(firstRow(dataset).getInt(ColumnPath.of("count"))).isGreaterThanOrEqualTo(0);

            Predicate countAboveHalf = new Predicate.Gt(ColumnPath.of("count"), new Value.IntVal(5_000));
            long matched = countMatching(dataset, countAboveHalf);
            assertThat(matched).isPositive().isLessThan(EXPECTED_TOTAL_ROWS);
            assertThat(matched).isEqualTo(dataset.count(countAboveHalf, ReadOptions.DEFAULTS));
        });
    }

    @Test
    void addedColumnReadsAsNullAndFoldsEqualityAway() {
        IcebergField added = new IcebergField(99, "label", "string", false);
        Path tableDir = evolve(List.of(ID, N, added));

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "n", "label");
            assertThat(firstRow(dataset).isNull(ColumnPath.of("label"))).isTrue();

            Predicate labelIsNull = new Predicate.IsNull(ColumnPath.of("label"));
            assertThat(dataset.count(labelIsNull, ReadOptions.DEFAULTS)).isEqualTo(EXPECTED_TOTAL_ROWS);

            Predicate labelEqualsX = new Predicate.Eq(ColumnPath.of("label"), new Value.StringVal("x"));
            assertThat(dataset.count(labelEqualsX, ReadOptions.DEFAULTS)).isZero();
            assertThat(countMatching(dataset, labelEqualsX)).isZero();
        });
    }

    @Test
    void addedColumnWithInitialDefaultReadsTheDefault() {
        IcebergField added = new IcebergField(99, "label", "string", false, Optional.of(new Value.StringVal("n/a")));
        Path tableDir = evolve(List.of(ID, N, added));

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "n", "label");
            assertThat(firstRow(dataset).getString(ColumnPath.of("label"))).isEqualTo("n/a");

            Predicate labelIsDefault = new Predicate.Eq(ColumnPath.of("label"), new Value.StringVal("n/a"));
            assertThat(dataset.count(labelIsDefault, ReadOptions.DEFAULTS)).isEqualTo(EXPECTED_TOTAL_ROWS);
            assertThat(countMatching(dataset, labelIsDefault)).isEqualTo(EXPECTED_TOTAL_ROWS);

            Predicate labelIsNull = new Predicate.IsNull(ColumnPath.of("label"));
            assertThat(dataset.count(labelIsNull, ReadOptions.DEFAULTS)).isZero();
        });
    }

    @Test
    void droppedColumnIsNotPresentedAndEveryRowStillReads() {
        IcebergField renamed = new IcebergField(N.fieldId(), "count", "int", false);
        Path tableDir = evolve(List.of(renamed));

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("count");
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);
            assertThat(firstRow(dataset).schema().leafColumns())
                    .noneMatch(path -> path.name().equals("id"));
        });
    }

    @Test
    void reorderFollowsTheTableLeafOrder() {
        IcebergField renamed = new IcebergField(N.fieldId(), "count", "int", false);
        Path tableDir = evolve(List.of(renamed, ID));

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("count", "id");
            ParquetRecord row = firstRow(dataset);
            assertThat(row.getInt(ColumnPath.of("count"))).isGreaterThanOrEqualTo(0);
            assertThat(row.getString(ColumnPath.of("id"))).isNotEmpty();
        });
    }

    @Test
    void intToLongPromotionPresentsInt64AndFiltersTheInt32FileAsLong() {
        IcebergField promoted = new IcebergField(N.fieldId(), "count", "long", false);
        Path tableDir = evolve(List.of(ID, promoted));

        withDataset(tableDir, dataset -> {
            assertThat(leafKind(dataset.schema(), "count")).isEqualTo(PrimitiveKind.INT64);
            assertThat(fullScanRowCount(dataset)).isEqualTo(EXPECTED_TOTAL_ROWS);

            long readLong = firstRow(dataset).getLong(ColumnPath.of("count"));
            assertThat(readLong).isGreaterThanOrEqualTo(0L);

            assertPromotedColumnFiltersAsLong(dataset);
        });
    }

    /**
     * The promoted {@code count} column holds the same underlying INT32 values as the unevolved {@code n} field; a
     * threshold predicate therefore matches the same positive, less-than-total count the rename test asserts. A
     * {@code long} literal and an {@code int} literal at the same threshold must agree through both the record-level
     * read and the pushdown count, proving the int-to-long widening reaches every comparison path.
     */
    private static void assertPromotedColumnFiltersAsLong(IcebergDataset dataset) {
        Predicate countAboveHalfLong = new Predicate.Gt(ColumnPath.of("count"), new Value.LongVal(5_000L));
        long matchedLong = countMatching(dataset, countAboveHalfLong);
        assertThat(matchedLong).isPositive().isLessThan(EXPECTED_TOTAL_ROWS);
        assertThat(matchedLong).isEqualTo(dataset.count(countAboveHalfLong, ReadOptions.DEFAULTS));

        Predicate countAboveHalfInt = new Predicate.Gt(ColumnPath.of("count"), new Value.IntVal(5_000));
        long matchedInt = countMatching(dataset, countAboveHalfInt);
        assertThat(matchedInt).isEqualTo(matchedLong);
        assertThat(matchedInt).isEqualTo(dataset.count(countAboveHalfInt, ReadOptions.DEFAULTS));
    }

    private Path evolve(List<IcebergField> evolvedFields) {
        Path tableDir = extractTable();
        return IcebergSchemaEvolution.evolveCurrentSchema(tableDir, evolvedFields);
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private void withDataset(Path tableDir, Consumer<IcebergDataset> assertions) {
        try (IcebergCatalog catalog = IcebergCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
    }

    private static long fullScanRowCount(IcebergDataset dataset) {
        return countMatching(dataset, Predicate.ALWAYS_TRUE);
    }

    private static long countMatching(IcebergDataset dataset, Predicate predicate) {
        try (Stream<ParquetRecord> rows = dataset.read(predicate, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.count();
        }
    }

    private static ParquetRecord firstRow(IcebergDataset dataset) {
        try (Stream<ParquetRecord> rows = dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.map(ParquetRecord::detach).findFirst().orElseThrow();
        }
    }

    private static List<String> leafNames(ParquetSchema schema) {
        return schema.leafColumns().stream().map(ColumnPath::name).toList();
    }

    private static PrimitiveKind leafKind(ParquetSchema schema, String leafName) {
        for (SchemaNode child : schema.root().children()) {
            if (child instanceof SchemaNode.Primitive leaf && leaf.name().equals(leafName)) {
                return leaf.kind();
            }
        }
        throw new IllegalArgumentException("no leaf named " + leafName);
    }
}
