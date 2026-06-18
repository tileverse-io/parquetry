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
package io.tileverse.parquetry.conformance.geoparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;

/**
 * Reader-level conformance against the {@code geoparquet/geoparquet-testing} {@code data/} and {@code samples/} tiers
 * (GeoParquet 2.0). This is the source-of-truth check that {@code parquetry-core} reads the corpus, independent of any
 * integration adapter: every fixture must open, expose parseable {@code geo} metadata, recognize its geometry leaf via
 * the native logical type, and hand back a length-bounded WKB value that a conformant decoder (JTS) accepts for every
 * row. The geo-jts and geotools suites cover the same files through their own materializer and CRS resolver.
 */
class GeoParquetReadConformanceIT {

    static Stream<Arguments> conformanceFiles() throws IOException {
        return Stream.concat(parquetFiles(GeoParquetCorpus.data()), parquetFiles(GeoParquetCorpus.samples()));
    }

    private static Stream<Arguments> parquetFiles(Path root) throws IOException {
        try (Stream<Path> walk = Files.walk(root)) {
            List<Arguments> files = walk.filter(path -> path.toString().endsWith(".parquet"))
                    .sorted()
                    .map(path -> Arguments.of(root.getFileName() + "/" + root.relativize(path), path))
                    .toList();
            return files.stream();
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("conformanceFiles")
    void readsMetadataAndDecodesEveryGeometry(String label, Path file) throws Exception {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);

            assertThat(reader.keyValueMetadata().get("geo"))
                    .as("%s: 'geo' metadata present", label)
                    .isNotNull();

            ParquetSchema schema = reader.schema();
            Set<ColumnPath> geometryColumns = GeoParquetCorpus.geometryColumns(schema);
            assertThat(geometryColumns)
                    .as("%s: at least one geometry column recognized via its native logical type", label)
                    .isNotEmpty();

            long rows = decodeEveryGeometry(reader, geometryColumns, label);
            assertThat(rows).as("%s: row count", label).isPositive();
        }
    }

    private static long decodeEveryGeometry(ParquetReader reader, Set<ColumnPath> geometryColumns, String label) {
        try (Stream<ParquetRecord> rows = reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
            return rows.peek(row -> decodeGeometries(row, geometryColumns, label))
                    .count();
        }
    }

    private static void decodeGeometries(ParquetRecord row, Set<ColumnPath> geometryColumns, String label) {
        for (ColumnPath column : geometryColumns) {
            byte[] wkb = GeoParquetCorpus.wkbBytes(row, column);
            if (wkb != null) {
                Geometry geometry = decode(wkb, label, column);
                assertThat(geometry)
                        .as("%s: %s decodes to a JTS geometry", label, column)
                        .isNotNull();
            }
        }
    }

    private static Geometry decode(byte[] wkb, String label, ColumnPath column) {
        try {
            return new WKBReader().read(wkb);
        } catch (Exception e) {
            throw new AssertionError(label + ": " + column + " has WKB a conformant decoder rejects", e);
        }
    }
}
