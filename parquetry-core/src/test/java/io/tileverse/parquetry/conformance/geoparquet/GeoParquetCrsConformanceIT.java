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
package io.tileverse.parquetry.conformance.geoparquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoColumn;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.schema.geo.projjson.CoordinateReferenceSystem;
import io.tileverse.parquetry.schema.geo.projjson.Identifier;

/**
 * Reader-level CRS conformance against the {@code geoparquet/geoparquet-testing} {@code data/crs/} fixtures, which each
 * declare the geometry column under one of the five PROJJSON CRS forms. This pins how {@code parquetry-core} parses the
 * CRS identity out of the {@code geo} metadata, distinct from the registry resolution to a GeoTools CRS the geotools
 * suite covers: an EPSG identifier yields its code, while OGC:CRS84, an id-less PROJJSON document, and an absent CRS
 * have no EPSG code (the consumer falls back to the GeoParquet default).
 */
class GeoParquetCrsConformanceIT {

    /**
     * @param fixture the {@code data/crs/} file name
     * @param expectedEpsg the EPSG code the geometry column's CRS must yield, or {@code -1} when no EPSG code is
     *     derivable (OGC:CRS84, an id-less PROJJSON document, or no CRS at all)
     */
    @ParameterizedTest(name = "{0} -> EPSG:{1}")
    @CsvSource({
        "crs-epsg-3857.parquet, 3857",
        "crs-epsg-4326.parquet, 4326",
        "crs-ogc-crs84.parquet, -1",
        "crs-projjson-full.parquet, -1",
        "crs-default.parquet, -1",
    })
    void parsesGeometryColumnCrsIdentity(String fixture, int expectedEpsg) throws Exception {
        Path file = GeoParquetCorpus.crs().resolve(fixture);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            String geoJson = reader.keyValueMetadata().get("geo");
            assertThat(geoJson).as("%s: 'geo' metadata present", fixture).isNotNull();

            GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);
            GeoColumn column = geo.columns().get(geo.primaryColumn());
            assertThat(column)
                    .as("%s: primary geometry column present in 'geo'", fixture)
                    .isNotNull();

            OptionalInt epsg = column.crs()
                    .flatMap(CoordinateReferenceSystem::id)
                    .map(Identifier::epsgCode)
                    .orElse(OptionalInt.empty());

            if (expectedEpsg > 0) {
                assertThat(epsg)
                        .as("%s: parsed CRS exposes its EPSG code", fixture)
                        .hasValue(expectedEpsg);
            } else {
                assertThat(epsg)
                        .as("%s: no EPSG code is derivable (OGC:CRS84 / id-less PROJJSON / absent CRS)", fixture)
                        .isEmpty();
            }
        }
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource({"crs-ogc-crs84.parquet, OGC, CRS84"})
    void parsesNonEpsgAuthorityIdentity(String fixture, String authority, String code) throws Exception {
        Path file = GeoParquetCorpus.crs().resolve(fixture);
        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);
            GeoParquetMetadata geo =
                    GeoParquetMetadata.parse(reader.keyValueMetadata().get("geo"));
            Optional<Identifier> id =
                    geo.columns().get(geo.primaryColumn()).crs().flatMap(CoordinateReferenceSystem::id);

            assertThat(id).as("%s: CRS carries a typed identifier", fixture).isPresent();
            assertThat(id.orElseThrow().authority()).isEqualTo(authority);
            assertThat(id.orElseThrow().code()).isEqualTo(code);
        }
    }
}
