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
package io.tileverse.parquetry.geotools.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Optional;

import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.referencing.CRS;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testkit.TestCorpus;

/**
 * Resolves the {@code geoparquet/geoparquet-testing} {@code data/crs/} fixtures (GeoParquet 2.0) end-to-end through
 * {@link FeatureTypeMapper} and asserts the GeoTools CRS that lands on the geometry attribute. The reader-level CRS
 * identity parsing lives in {@code parquetry-core}; this is the GeoTools adapter's registry resolution of the same
 * fixtures.
 *
 * <p>The fixtures place the CRS in the {@code geo} metadata (the native logical-type {@code crs} is left empty); the
 * mapper resolves an EPSG identifier when the PROJJSON has one, and otherwise defaults to lon/lat WGS84 (the GeoParquet
 * default), which is what OGC:CRS84 and an id-less PROJJSON document both resolve to.
 */
class GeoParquetCrsResolutionIT {

    /**
     * @param fixture the {@code data/crs/} file name
     * @param expectedEpsg the EPSG code the geometry CRS must resolve to, or {@code -1} for the lon/lat WGS84 default
     *     (the GeoParquet default, used when the PROJJSON has no EPSG identifier)
     */
    @ParameterizedTest(name = "{0} -> EPSG:{1}")
    @CsvSource({
        "crs-epsg-3857.parquet, 3857",
        "crs-epsg-4326.parquet, 4326",
        "crs-default.parquet, -1",
        "crs-ogc-crs84.parquet, -1",
        "crs-projjson-full.parquet, -1",
    })
    void resolvesGeometryCrs(String fixture, int expectedEpsg, @TempDir Path dir) throws Exception {
        Path file = TestCorpus.extractFile("geoparquet-testing/data/crs/" + fixture, dir);
        try (ByteRangeSource src = ByteRangeSource.ofFile(file)) {
            ParquetSource ds = ParquetSource.open(src);
            Optional<GeoParquetMetadata> geo =
                    Optional.of(GeoParquetMetadata.parse(ds.keyValueMetadata().get("geo")));
            FeatureTypeMapper.Mapping mapping = FeatureTypeMapper.map(fixture, null, ds.schema(), geo, null);
            SimpleFeatureType ft = mapping.featureType();
            org.geotools.api.referencing.crs.CoordinateReferenceSystem crs = ft.getCoordinateReferenceSystem();

            assertThat(crs).as("%s resolves a geometry CRS", fixture).isNotNull();
            if (expectedEpsg > 0) {
                assertThat(CRS.lookupEpsgCode(crs, false))
                        .as("%s should resolve to EPSG:%s", fixture, expectedEpsg)
                        .isEqualTo(expectedEpsg);
            }
            assertThat(CRS.getAxisOrder(crs))
                    .as("%s should resolve to a lon/lat (easting-first) CRS", fixture)
                    .isEqualTo(CRS.AxisOrder.EAST_NORTH);
        }
    }
}
