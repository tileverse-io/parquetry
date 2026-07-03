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
import io.tileverse.parquetry.filter.Bbox;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Proves a renamed Iceberg geometry column reads and prunes identically under its new name. The {@code v3_geometry}
 * corpus table has a flat schema (id#1:string, geom#2:geometry) over ten data files of ten thousand rows; the existing
 * {@code IcebergPruningTest} establishes that a California bbox query over {@code geom} keeps one file and matches a
 * positive subset of the rows.
 *
 * <p>This evolves field id 2 to the name {@code geometry} (its geometry type unchanged) over the same files, then
 * asserts the California bbox query on the new name returns exactly the same matching count the unevolved {@code geom}
 * query does. The match is computed against the unevolved table as the baseline; the assertion never hard-codes a row
 * count that could drift with the corpus.
 */
class IcebergGeometryEvolutionIT {

    private static final String TABLE = "v3_geometry";
    private static final long EXPECTED_TOTAL_ROWS = 10_000L;
    private static final Bbox CALIFORNIA = Bbox.of2d(-125.0, 32.0, -115.0, 42.0);

    @TempDir
    Path tempDir;

    @Test
    void renamedGeometryColumnMatchesTheSameRowsUnderTheNewName() {
        long baselineMatches = baselineCaliforniaMatches();

        IcebergField renamed = new IcebergField(2, "geometry", "geometry", false);
        Path evolvedTable = evolve(List.of(new IcebergField(1, "id", "string", false), renamed));

        withDataset(evolvedTable, dataset -> {
            assertThat(leafNames(dataset)).containsExactly("id", "geometry");
            assertThat(dataset.count(Predicate.ALWAYS_TRUE, ReadOptions.DEFAULTS))
                    .isEqualTo(EXPECTED_TOTAL_ROWS);

            Predicate california = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geometry"), CALIFORNIA);
            long matched = countMatching(dataset, california);
            assertThat(matched).isPositive().isEqualTo(baselineMatches);
            assertThat(matched).isEqualTo(dataset.count(california, ReadOptions.DEFAULTS));
        });
    }

    /** The California match count over the unevolved {@code geom} column, the known-correct baseline. */
    private long baselineCaliforniaMatches() {
        Path tableDir = extractTable();
        long[] matched = new long[1];
        withDataset(tableDir, dataset -> {
            Predicate california = new Predicate.Spatial.BboxIntersects(ColumnPath.of("geom"), CALIFORNIA);
            matched[0] = countMatching(dataset, california);
        });
        assertThat(matched[0]).isPositive();
        return matched[0];
    }

    private Path evolve(List<IcebergField> evolvedFields) {
        Path tableDir = extractTableInto("evolved");
        return IcebergSchemaEvolution.evolveCurrentSchema(tableDir, evolvedFields);
    }

    private Path extractTable() {
        return extractTableInto("baseline");
    }

    private Path extractTableInto(String subdir) {
        Path root = TestCorpus.extractDirectory("iceberg-geo-testbed", tempDir.resolve(subdir));
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

    private static List<String> leafNames(IcebergDataset dataset) {
        return dataset.schema().leafColumns().stream().map(ColumnPath::name).toList();
    }
}
