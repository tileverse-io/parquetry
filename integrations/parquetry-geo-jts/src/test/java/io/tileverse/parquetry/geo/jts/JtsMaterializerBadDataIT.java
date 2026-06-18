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
package io.tileverse.parquetry.geo.jts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

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

import io.tileverse.parquetry.data.ParquetReader;
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
 * <p>The materializer is lenient: it validates almost nothing of the file against its own {@code geo} metadata, and its
 * WKB decoder does not cross-check the geometry against the declared {@code geometry_types}, {@code edges}, or
 * {@code crs}. This pins that posture as a tested contract: <b>every</b> {@code bad_data/} fixture materializes without
 * throwing. The violations the manifest catalogs - bbox / crs / edges / orientation / geometry-type / zm / epoch /
 * version mismatches, missing or malformed {@code geo} metadata, and even WKB-structural corruption - are all
 * tolerated. Enforcing them is the job of a geometry-engine or validation layer on top of the reader, not the reader.
 *
 * <p>Two cases are worth calling out because their leniency is wider than a metadata disagreement:
 *
 * <ul>
 *   <li>{@code wkb-with-srid-prefix} - parquetry accepts EWKB (an SRID-prefixed WKB) even though GeoParquet disallows
 *       it.
 *   <li>{@code wkb-truncated} and {@code wkb-wrong-type-byte} - the WKB decoder does not bound its walk to the binary
 *       value's length; a truncated or mis-typed payload decodes to a wrong-but-non-crashing geometry (a truncated
 *       point reads adjacent bytes as coordinates; a body that disagrees with the header type yields an empty geometry
 *       of the declared type) rather than an error. {@link #wkbStructuralCorruptionReadsLenientlyWithoutThrowing()}
 *       pins this; a future stricter decoder flips the contract consciously.
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

    static Stream<Arguments> badDataFiles() {
        return manifest().keySet().stream().sorted().map(Arguments::of);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badDataFiles")
    void everyBadFixtureIsToleratedAndReadsWithoutThrowing(String fileName) {
        Path file = BAD_DATA.resolve(fileName);
        assertThat(file).as("manifest entry %s has a fixture file", fileName).exists();
        assertThatCode(() -> readAndMaterialize(file))
                .as("%s is tolerated by the lenient reader and must read without throwing", fileName)
                .doesNotThrowAnyException();
    }

    @Test
    void wkbStructuralCorruptionReadsLenientlyWithoutThrowing() {
        Map<ColumnPath, Object> truncated = firstRow(BAD_DATA.resolve("wkb-truncated.parquet"));
        assertThat(truncated.get(ColumnPath.of("geometry")))
                .as("a truncated WKB point decodes to a (wrong) geometry rather than throwing")
                .isInstanceOf(Geometry.class);

        Map<ColumnPath, Object> wrongType = firstRow(BAD_DATA.resolve("wkb-wrong-type-byte.parquet"));
        assertThat(wrongType.get(ColumnPath.of("geometry")))
                .as("a WKB whose header type disagrees with its body decodes to a geometry rather than throwing")
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
            ParquetReader reader = ParquetReader.open(source);
            JtsMaterializer materializer = new JtsMaterializer(reader.schema());
            try (Stream<Map<ColumnPath, Object>> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, materializer, ReadOptions.DEFAULTS)) {
                return rows.findFirst().orElseThrow();
            }
        }
    }

    private static void readAndMaterialize(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
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
