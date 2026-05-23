/*
 * Copyright (c) 2026 Tileverse.io
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.data.ParquetDataset;
import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * Conformance suite against the {@code opengeospatial/geoparquet} {@code test_data/} fixtures. For each
 * {@code data-<type>-encoding_wkb.parquet} file we read every row through parquetry's {@link JtsMaterializer} and
 * compare the decoded {@link Geometry} to the WKT in the matching {@code data-<type>-wkt.csv} sidecar.
 *
 * <p>Gated on the OGC repo being cloned alongside parquetry; skips silently otherwise. {@code generate_test_data.py} in
 * that repo describes how the fixtures were built (Shapely + pyarrow).
 *
 * <p>Only the WKB-encoded fixtures are exercised. The {@code _encoding_native} fixtures use a GeoParquet 2.0
 * structured-coordinate encoding (groups with {@code x}/{@code y} fields) that's out of scope for the WKB-focused
 * materializer; native-coord decoding lands in a follow-up.
 */
class OgcGeoParquetConformanceIT {

    private static final Path TEST_DATA =
            Paths.get("../../geoparquet/test_data").toAbsolutePath().normalize();

    static boolean fixturesAvailable() {
        return Files.isDirectory(TEST_DATA);
    }

    @Test
    void testDataDirectoryIsAvailable() {
        assertThat(TEST_DATA)
                .as("opengeospatial/geoparquet test_data must be cloned alongside parquetry")
                .isDirectory();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon"})
    void wkbEncodedFixtureRoundTripsThroughJtsMaterializer(String geomType) throws Exception {
        if (!fixturesAvailable()) {
            return;
        }
        Path file = TEST_DATA.resolve("data-" + geomType + "-encoding_wkb.parquet");
        Path csv = TEST_DATA.resolve("data-" + geomType + "-wkt.csv");
        assertThat(file).isRegularFile();
        assertThat(csv).isRegularFile();

        List<String> expectedWkt = readExpectedWkt(csv);
        List<Geometry> actual = readGeometries(file);

        assertThat(actual)
                .as("%s row count must match the WKT sidecar", geomType)
                .hasSameSizeAs(expectedWkt);

        WKTReader wktReader = new WKTReader();
        for (int i = 0; i < expectedWkt.size(); i++) {
            String wantWkt = expectedWkt.get(i);
            Geometry got = actual.get(i);
            if (wantWkt.isEmpty()) {
                assertThat(got).as("%s row %d should be null", geomType, i).isNull();
                continue;
            }
            Geometry want = wktReader.read(wantWkt);
            boolean matches = (want.isEmpty() && got != null && got.isEmpty()) || want.equalsExact(got, 1e-9);
            assertThat(matches)
                    .as("%s row %d: want %s got %s", geomType, i, want, got)
                    .isTrue();
        }
    }

    private static List<Geometry> readGeometries(Path file) throws IOException {
        ColumnPath geometry = ColumnPath.of("geometry");
        try (Storage storage = StorageFactory.open(file.getParent().toUri());
                RangeReader reader = storage.openRangeReader(file.getFileName().toString())) {
            ParquetDataset dataset = ParquetDataset.open(reader);
            assertGeoMetadataParses(dataset, geometry);
            JtsMaterializer materializer = new JtsMaterializer(dataset.schema());
            assertThat(materializer.geometryColumns())
                    .as("SchemaBuilder must synthesize the Geometry annotation on the WKB geometry column")
                    .contains(geometry);
            try (Stream<Map<ColumnPath, Object>> rows =
                    dataset.read(Predicate.ALWAYS_TRUE, Projection.ALL, materializer, ReadOptions.DEFAULTS)) {
                return rows.map(r -> (Geometry) r.get(geometry)).toList();
            }
        }
    }

    /**
     * Verifies the OGC fixtures land in the typed GeoParquet metadata model and that the schema crossing the format ->
     * core boundary already carries the synthesized native logical-type annotation.
     */
    private static void assertGeoMetadataParses(ParquetDataset dataset, ColumnPath geometry) {
        String geoJson = dataset.keyValueMetadata().get("geo");
        assertThat(geoJson)
                .as("OGC fixtures must carry a 'geo' key-value metadata entry")
                .isNotNull();
        GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);
        assertThat(geo)
                .as("OGC fixtures advertise GeoParquet 1.1; parse must dispatch to the V1_1 record permit")
                .isInstanceOf(GeoParquetMetadata.V1_1.class);
        assertThat(geo.primaryColumn()).isEqualTo("geometry");
        assertThat(geo.columns()).containsKey("geometry");

        SchemaNode.Primitive leaf =
                (SchemaNode.Primitive) dataset.schema().find(geometry).orElseThrow();
        assertThat(leaf.logicalType())
                .as("SchemaBuilder must fold the 'geo' JSON into a LogicalType.Geometry annotation on the WKB column")
                .hasValueSatisfying(lt -> assertThat(lt).isInstanceOf(LogicalType.Geometry.class));
    }

    private static List<String> readExpectedWkt(Path csv) throws IOException {
        List<String> wkts = new ArrayList<>();
        boolean firstLine = true;
        for (String raw : Files.readAllLines(csv)) {
            if (firstLine) {
                firstLine = false;
                continue; // header
            }
            // CSV shape: <col>,"WKT". Strip the leading `<col>,` and the surrounding double quotes around the WKT.
            int firstComma = raw.indexOf(',');
            String wkt = raw.substring(firstComma + 1).trim();
            if (wkt.startsWith("\"") && wkt.endsWith("\"")) {
                wkt = wkt.substring(1, wkt.length() - 1);
            }
            wkts.add(wkt);
        }
        return wkts;
    }
}
