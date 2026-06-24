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
package io.tileverse.parquetry.geo;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Exercises {@link JtsMaterializer} over the {@code geoparquet/geoparquet-testing} corpus ({@code data/} +
 * {@code samples/} tiers, GeoParquet 2.0): every fixture must open, expose parseable {@code geo} metadata with a
 * version, have at least one geometry column recognized, and decode every geometry row through the materializer without
 * error. The reader-level conformance lives in {@code parquetry-core}; this is the JTS adapter's view of the same
 * files.
 */
class JtsMaterializerCorpusIT {

    private static final Path CORPUS = extractCorpus();

    private static Path extractCorpus() {
        try {
            Path root = Files.createTempDirectory("geoparquet-testing");
            TestCorpus.extractDirectory("geoparquet-testing/data", Files.createDirectories(root.resolve("data")));
            TestCorpus.extractDirectory("geoparquet-testing/samples", Files.createDirectories(root.resolve("samples")));
            return root;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not extract the geoparquet-testing corpus", e);
        }
    }

    static Stream<Arguments> conformanceFiles() throws IOException {
        try (Stream<Path> walk = Files.walk(CORPUS)) {
            return walk
                    .filter(path -> path.toString().endsWith(".parquet"))
                    .sorted()
                    .map(path -> Arguments.of(CORPUS.relativize(path).toString(), path))
                    .toList()
                    .stream();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFiles")
    void readsAndDecodesEveryGeometry(String label, Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);

            String geoJson = reader.keyValueMetadata().get("geo");
            assertThat(geoJson).as("%s: 'geo' metadata present", label).isNotNull();
            GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);
            assertThat(geo.version()).as("%s: geo metadata version", label).isNotBlank();

            JtsMaterializer materializer = new JtsMaterializer(reader.schema());
            Set<ColumnPath> geometryColumns = materializer.geometryColumns();
            assertThat(geometryColumns)
                    .as("%s: at least one geometry column recognized", label)
                    .isNotEmpty();

            long rows = decodeAllRows(reader, materializer, geometryColumns, label);
            assertThat(rows).as("%s: row count", label).isPositive();
        }
    }

    private static long decodeAllRows(
            ParquetReader reader, JtsMaterializer materializer, Set<ColumnPath> geometryColumns, String label) {
        try (Stream<Map<ColumnPath, Object>> rows =
                reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, materializer, ReadOptions.DEFAULTS)) {
            return rows.peek(row -> assertGeometriesDecode(row, geometryColumns, label))
                    .count();
        }
    }

    private static void assertGeometriesDecode(
            Map<ColumnPath, Object> row, Set<ColumnPath> geometryColumns, String label) {
        for (ColumnPath geometryColumn : geometryColumns) {
            Object value = row.get(geometryColumn);
            if (value != null) {
                assertThat(value)
                        .as("%s: column %s decodes to a JTS geometry", label, geometryColumn)
                        .isInstanceOf(Geometry.class);
            }
        }
    }
}
