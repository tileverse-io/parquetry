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
 * Proves an Iceberg table whose data files embed no Parquet field ids reads through the table's
 * {@code schema.name-mapping.default} document (the {@code add_files} migration shape), and through the implicit
 * schema-derived mapping when the property is absent. The {@code migrated} corpus table has a flat schema (id#1:string,
 * n#2:int) over three id-less data files of one thousand rows each; its metadata holds the mapping property with names
 * matching the physical columns.
 *
 * <p>The rename test is the regression anchor for the silent misread this feature removed: an id-less file used to pass
 * through under its old physical name, and a consumer reading the renamed column saw null.
 */
class IcebergNameMappingIT {

    private static final String TABLE = "migrated";
    private static final long EXPECTED_TOTAL_ROWS = 3_000L;

    private static final IcebergField ID = new IcebergField(1, "id", "string", false);
    private static final IcebergField N = new IcebergField(2, "n", "int", false);

    /** The mapping after a rename of {@code n} to {@code count}: Iceberg keeps the old name as an alias. */
    private static final String ALIAS_MAPPING =
            "[{\"field-id\": 1, \"names\": [\"id\"]}, {\"field-id\": 2, \"names\": [\"n\", \"count\"]}]";

    @TempDir
    Path tempDir;

    @Test
    void migrationReadResolvesThroughTheExplicitMapping() {
        Path tableDir = extractTable();

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "n");
            assertThat(countMatching(dataset, Predicate.ALWAYS_TRUE)).isEqualTo(EXPECTED_TOTAL_ROWS);
            assertThat(firstRow(dataset).getString(ColumnPath.of("id"))).isNotEmpty();

            Predicate nAboveHalf = new Predicate.Gt(ColumnPath.of("n"), new Value.IntVal(500_000));
            long matched = countMatching(dataset, nAboveHalf);
            assertThat(matched).isPositive().isLessThan(EXPECTED_TOTAL_ROWS);
            assertThat(matched).isEqualTo(dataset.count(nAboveHalf, ReadOptions.DEFAULTS));
        });
    }

    @Test
    void selectStarShapeMatchesTheDatasetSchema() {
        Path tableDir = extractTable();

        withDataset(tableDir, dataset -> {
            List<String> datasetLeaves = leafNames(dataset.schema());
            List<String> recordLeaves = leafNames(firstRow(dataset).schema());
            assertThat(recordLeaves).isEqualTo(datasetLeaves);
        });
    }

    @Test
    void renameResolvesTheOldPhysicalNameThroughTheAlias() {
        IcebergField renamed = new IcebergField(2, "count", "int", false);
        Path tableDir = IcebergSchemaEvolution.evolveCurrentSchema(extractTable(), List.of(ID, renamed), ALIAS_MAPPING);

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "count");
            assertThat(firstRow(dataset).getInt(ColumnPath.of("count"))).isGreaterThanOrEqualTo(0);

            Predicate isNull = new Predicate.IsNull(ColumnPath.of("count"));
            assertThat(dataset.count(isNull, ReadOptions.DEFAULTS)).isZero();

            Predicate countAboveHalf = new Predicate.Gt(ColumnPath.of("count"), new Value.IntVal(500_000));
            long matched = countMatching(dataset, countAboveHalf);
            assertThat(matched).isPositive().isLessThan(EXPECTED_TOTAL_ROWS);
            assertThat(matched).isEqualTo(dataset.count(countAboveHalf, ReadOptions.DEFAULTS));
        });
    }

    /**
     * Removing the mapping from an already-renamed table is a degenerate state: the manifests still describe the
     * physical column under field id 2, but the read can no longer locate it and presents {@code count} as null.
     * Record-level reads pin that boundary here. Predicate pushdown over the renamed column is deliberately not
     * asserted: file pruning trusts the manifest metrics for field id 2 (zero nulls recorded), matching Iceberg's own
     * metrics-based scan planning over a table whose mapping was deleted.
     */
    @Test
    void renameWithoutAMappingReadsTheRenamedColumnAsNull() {
        IcebergField renamed = new IcebergField(2, "count", "int", false);
        Path tableDir = IcebergSchemaEvolution.evolveCurrentSchema(extractTable(), List.of(ID, renamed), null);

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "count");
            assertThat(countMatching(dataset, Predicate.ALWAYS_TRUE)).isEqualTo(EXPECTED_TOTAL_ROWS);

            ParquetRecord row = firstRow(dataset);
            assertThat(row.getString(ColumnPath.of("id"))).isNotEmpty();
            assertThat(row.isNull(ColumnPath.of("count"))).isTrue();
        });
    }

    @Test
    void unevolvedTableWithoutAMappingReadsThroughTheImplicitMapping() {
        Path tableDir = IcebergSchemaEvolution.evolveCurrentSchema(extractTable(), List.of(ID, N), null);

        withDataset(tableDir, dataset -> {
            assertThat(leafNames(dataset.schema())).containsExactly("id", "n");
            assertThat(countMatching(dataset, Predicate.ALWAYS_TRUE)).isEqualTo(EXPECTED_TOTAL_ROWS);
            assertThat(firstRow(dataset).getString(ColumnPath.of("id"))).isNotEmpty();
        });
    }

    private Path extractTable() {
        Path root = TestCorpus.extractDirectory("iceberg-name-mapping", tempDir.resolve(TABLE));
        return root.resolve(TABLE);
    }

    private void withDataset(Path tableDir, Consumer<IcebergDataset> assertions) {
        try (IcebergTableCatalog catalog = IcebergTableCatalog.openLocal(tableDir, IcebergOptions.defaults())) {
            IcebergDataset dataset = (IcebergDataset) catalog.dataset(TABLE);
            assertions.accept(dataset);
        }
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
}
