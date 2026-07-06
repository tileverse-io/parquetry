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
package io.tileverse.parquetry.geo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.testkit.TestCorpus;

import tools.jackson.databind.json.JsonMapper;

/**
 * Exercises {@link JtsMaterializer} over the {@code geoparquet/geoparquet-testing} {@code bad_data/} tier, which pairs
 * each deliberately-broken file with a machine-readable {@code manifest.json} entry naming the GeoParquet rule it
 * violates. The reader-level negative conformance lives in {@code parquetry-core}; this pins the JTS adapter's leniency
 * over the same files.
 *
 * <p>The materializer validates almost nothing of the file against its own {@code geo} metadata, and does not
 * cross-check the geometry against the declared {@code geometry_types}, {@code edges}, or {@code crs} - those
 * metadata-vs-data mismatches are tolerated (a validation layer's job). It does, however, bound the WKB walk to each
 * value's length and reject a payload that runs past its own bytes:
 *
 * <ul>
 *   <li><b>Must-detect:</b> {@code wkb-truncated} - the stored value is shorter than the geometry its header declares;
 *       decoding throws rather than reading the adjacent bytes of the column window.
 *   <li><b>Tolerated:</b> every metadata mismatch (bbox / crs / edges / orientation / geometry-type / zm / epoch /
 *       version), missing or malformed {@code geo} metadata, {@code wkb-with-srid-prefix} (EWKB is accepted even though
 *       GeoParquet disallows it), and {@code wkb-wrong-type-byte} (a LineString header over a Point body still forms a
 *       structurally-valid empty LineString that only {@code geometry_types} validation catches).
 * </ul>
 *
 * <p>The suite also asserts the {@code bad_data/} directory and {@code manifest.json} stay in lockstep: a corpus
 * refresh that adds a fixture forces a conscious entry here rather than silently going untested.
 */
class JtsMaterializerBadDataIT {

    private static final Path BAD_DATA = extractBadData();

    private static Path extractBadData() {
        try {
            Path root = Files.createTempDirectory("geoparquet-testing-bad");
            TestCorpus.extractDirectory("geoparquet-testing/bad_data", Files.createDirectories(root));
            return root;
        } catch (IOException e) {
            throw new UncheckedIOException("Could not extract the geoparquet-testing bad_data corpus", e);
        }
    }

    /** Structurally corrupt WKB the materializer rejects; every other fixture is a tolerated metadata disagreement. */
    private static final Set<String> MUST_DETECT = Set.of("wkb-truncated.parquet");

    static Stream<Arguments> badDataFiles() {
        return manifest().keySet().stream().sorted().map(Arguments::of);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badDataFiles")
    void materializerPosture(String fileName) {
        Path file = BAD_DATA.resolve(fileName);
        assertThat(file).as("manifest entry %s has a fixture file", fileName).exists();

        if (MUST_DETECT.contains(fileName)) {
            assertThatThrownBy(() -> readAndMaterialize(file))
                    .as("%s is a structurally corrupt WKB value the materializer must reject", fileName)
                    .isInstanceOf(RuntimeException.class);
        } else {
            assertThatCode(() -> readAndMaterialize(file))
                    .as("%s is a metadata-vs-data disagreement the materializer tolerates", fileName)
                    .doesNotThrowAnyException();
        }
    }

    @Test
    void wrongTypeWkbMaterializesToAStructurallyValidEmptyGeometry() {
        Map<ColumnPath, Object> wrongType = firstRow(BAD_DATA.resolve("wkb-wrong-type-byte.parquet"));
        assertThat(wrongType.get(ColumnPath.of("geometry")))
                .as("a LineString header over a Point body materializes to an empty geometry, not an error")
                .isInstanceOfSatisfying(
                        Geometry.class,
                        geometry -> assertThat(geometry.isEmpty())
                                .as("the declared LineString header with a Point body yields an empty geometry")
                                .isTrue());
    }

    @Test
    void manifestAndDirectoryStayInLockstep() throws IOException {
        try (Stream<Path> walk = Files.walk(BAD_DATA)) {
            Set<String> onDisk = walk.filter(path -> path.toString().endsWith(".parquet"))
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toSet());
            assertThat(onDisk)
                    .as("every bad_data parquet fixture is classified by this suite via the manifest")
                    .isEqualTo(manifest().keySet());
        }
    }

    private static Map<ColumnPath, Object> firstRow(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            JtsMaterializer materializer = new JtsMaterializer(reader.schema());
            try (Stream<Map<ColumnPath, Object>> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, materializer, ReadOptions.DEFAULTS)) {
                return rows.findFirst().orElseThrow();
            }
        }
    }

    private static void readAndMaterialize(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            JtsMaterializer materializer = new JtsMaterializer(reader.schema());
            try (Stream<Map<ColumnPath, Object>> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, materializer, ReadOptions.DEFAULTS)) {
                rows.forEach(JtsMaterializerBadDataIT::touchGeometries);
            }
        }
    }

    private static void touchGeometries(Map<ColumnPath, Object> row) {
        for (Object value : row.values()) {
            if (value instanceof Geometry geometry) {
                geometry.getEnvelopeInternal();
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> manifest() {
        try {
            Path manifest = BAD_DATA.resolve("manifest.json");
            return JsonMapper.builder().build().readValue(Files.readAllBytes(manifest), Map.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Could not read the bad_data manifest", e);
        }
    }
}
