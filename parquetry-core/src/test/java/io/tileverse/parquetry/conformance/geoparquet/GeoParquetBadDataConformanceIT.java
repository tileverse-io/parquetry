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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.io.WKBReader;

import io.tileverse.parquetry.data.ParquetReader;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Reader-level negative conformance against the {@code geoparquet/geoparquet-testing} {@code bad_data/} tier.
 * {@code parquetry-core} is a lenient reader: it cross-checks nothing against the {@code geo} metadata and hands back
 * raw bytes for the geometry column. This pins that <b>every</b> deliberately-broken fixture reads at the record level
 * without throwing - bbox / crs / edges / orientation / geometry-type / zm / version mismatches, missing or malformed
 * {@code geo} metadata, and WKB-structural corruption alike. Enforcing the manifest's rules is a validation layer's
 * job.
 *
 * <p>The reader handing back bytes is separate from whether those bytes decode, and the two WKB fixtures split on that:
 *
 * <ul>
 *   <li>{@link #truncatedWkbIsRejectedByAConformantDecoder()} - {@code wkb-truncated}'s value is 13 bytes; decoded as a
 *       length-bounded value, it is rejected ("read past end of input"). Both JTS and parquetry's own
 *       {@code MemorySegmentWkbReader} reject it, the latter because it bounds its walk to the value length; the
 *       geo-jts {@code JtsMaterializerBadDataIT} pins that the materializer throws on this fixture.
 *   <li>{@link #wrongTypeWkbDecodesToAStructurallyValidButWrongGeometry()} - {@code wkb-wrong-type-byte} declares a
 *       LineString header over a Point body. The bytes still form a structurally-valid geometry (an empty LineString),
 *       so no decoder rejects it; only cross-checking the geometry against the declared {@code geometry_types} catches
 *       the disagreement, which the lenient reader does not do.
 * </ul>
 */
class GeoParquetBadDataConformanceIT {

    static Stream<Arguments> badDataFiles() {
        return GeoParquetCorpus.manifest().keySet().stream().sorted().map(Arguments::of);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("badDataFiles")
    void everyBadFixtureReadsWithoutThrowingAtRecordLevel(String fileName) {
        Path file = GeoParquetCorpus.badData().resolve(fileName);
        assertThat(file).as("manifest entry %s has a fixture file", fileName).exists();
        assertThatCode(() -> drainRecords(file))
                .as("%s is tolerated by the lenient reader and must read without throwing", fileName)
                .doesNotThrowAnyException();
    }

    @Test
    void truncatedWkbIsRejectedByAConformantDecoder() {
        byte[] wkb = firstGeometryWkb(GeoParquetCorpus.badData().resolve("wkb-truncated.parquet"));
        assertThat(wkb)
                .as("the truncated geometry value is present (the corruption is its short length)")
                .isNotNull();
        assertThatThrownBy(() -> new WKBReader().read(wkb))
                .as("read length-bounded, the truncated WKB is rejected by a conformant decoder")
                .isInstanceOf(Exception.class);
    }

    @Test
    void wrongTypeWkbDecodesToAStructurallyValidButWrongGeometry() throws Exception {
        byte[] wkb = firstGeometryWkb(GeoParquetCorpus.badData().resolve("wkb-wrong-type-byte.parquet"));
        assertThat(wkb).as("the geometry value is present").isNotNull();
        assertThat(new WKBReader().read(wkb))
                .as("a LineString header over a Point body still parses; only geometry_types validation catches it")
                .isNotNull();
    }

    @Test
    void malformedGeoMetadataIsRejectedByTheGeoParquetParser() {
        // The generic reader degrades a corrupt 'geo' block to a non-geo read (everyBadFixture... pins that). The
        // GeoParquet metadata parser, in contrast, rejects it: invalid JSON, and a 'crs' that is JSON but not valid
        // PROJJSON (no 'type'). The metadata-vs-data and incomplete-schema violations are not parser-level and stay
        // tolerated.
        for (String fileName : List.of("geo-invalid-json.parquet", "crs-invalid-projjson.parquet")) {
            String geo = geoMetadata(GeoParquetCorpus.badData().resolve(fileName));
            assertThat(geo).as("%s exposes a 'geo' metadata key", fileName).isNotNull();
            assertThatThrownBy(() -> GeoParquetMetadata.parse(geo))
                    .as("%s: the GeoParquet metadata parser rejects the malformed 'geo' block", fileName)
                    .isInstanceOf(RuntimeException.class);
        }
    }

    private static String geoMetadata(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            return ParquetReader.open(source).keyValueMetadata().get("geo");
        }
    }

    @Test
    void manifestAndDirectoryStayInLockstep() throws IOException {
        try (Stream<Path> walk = Files.walk(GeoParquetCorpus.badData())) {
            Set<String> onDisk = walk.filter(path -> path.toString().endsWith(".parquet"))
                    .map(path -> path.getFileName().toString())
                    .collect(Collectors.toSet());
            assertThat(onDisk)
                    .as("every bad_data parquet fixture is classified by this suite via the manifest")
                    .isEqualTo(GeoParquetCorpus.manifest().keySet());
        }
    }

    private static void drainRecords(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(GeoParquetBadDataConformanceIT::touchEveryColumn);
            }
        }
    }

    private static void touchEveryColumn(ParquetRecord row) {
        for (int col = 0; col < row.columnCount(); col++) {
            row.get(col);
        }
    }

    private static byte[] firstGeometryWkb(Path file) {
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetReader reader = ParquetReader.open(source);
            Set<ColumnPath> geometryColumns = GeoParquetCorpus.geometryColumns(reader.schema());
            ColumnPath geometry = geometryColumns.iterator().next();
            try (Stream<ParquetRecord> rows =
                    reader.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                ParquetRecord first = rows.findFirst().orElseThrow();
                return GeoParquetCorpus.wkbBytes(first, geometry);
            }
        }
    }
}
