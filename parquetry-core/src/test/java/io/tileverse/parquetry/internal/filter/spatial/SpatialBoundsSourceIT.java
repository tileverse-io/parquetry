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
package io.tileverse.parquetry.internal.filter.spatial;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.tileverse.parquetry.format.BoundingBox;
import io.tileverse.parquetry.format.FileMetaData;
import io.tileverse.parquetry.format.ParquetFormat;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.SchemaBuilder;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;

/**
 * End-to-end integration test for {@link SpatialBoundsSource}: opens a real GeoParquet fixture, reads the footer
 * through {@link ParquetFormat#readFooter}, parses the {@code "geo"} key-value metadata into the typed
 * {@link GeoParquetMetadata}, then exercises {@link SpatialBoundsSource#of} against the produced state.
 *
 * <p>Only {@link GeoJsonFileBoundsSource} is exercised end-to-end today: the opengeospatial/geoparquet fixtures we
 * vendor at {@code ../../geoparquet/} all advertise GeoParquet 1.1.0 without native
 * {@link io.tileverse.parquetry.format.GeospatialStatistics} (parquet-cpp does not emit field 17 yet) and without
 * {@code covering} sidecar columns. {@link NativeStatsSource} and {@link CoveringColumnSource} stay covered by
 * {@code SpatialBoundsSourceTest}'s synthetic dispatch fixtures until we land producer-side fixtures with those shapes.
 */
class SpatialBoundsSourceIT {

    private static final Path GEOPARQUET_ROOT =
            Paths.get("../../geoparquet").toAbsolutePath().normalize();
    private static final Path EXAMPLE_PARQUET = GEOPARQUET_ROOT.resolve("examples/example.parquet");

    @Test
    void exampleParquetFallsThroughToGeoJsonFileBounds() {
        if (!Files.isRegularFile(EXAMPLE_PARQUET)) {
            return;
        }

        try (ByteRangeSource byteSource = ByteRangeSource.ofFile(EXAMPLE_PARQUET)) {
            FileMetaData footer = ParquetFormat.readFooter(byteSource);
            String geoJson = lookupGeoJson(footer);
            assertThat(geoJson)
                    .as("example.parquet must carry the 'geo' KV entry")
                    .isNotNull();
            GeoParquetMetadata geo = GeoParquetMetadata.parse(geoJson);

            ParquetSchema schema = SchemaBuilder.build(footer.schema());
            SpatialBoundsSource source = SpatialBoundsSource.of(footer, schema, Optional.of(geo));
            assertThat(source)
                    .as(
                            "example.parquet has 'geo' bbox but no native stats / covering; dispatch lands on the JSON tier")
                    .isInstanceOf(GeoJsonFileBoundsSource.class);

            ColumnPath geometry = ColumnPath.of("geometry");
            BoundingBox bounds = source.fileBounds(geometry).orElseThrow();
            assertThat(bounds.xmin())
                    .as("example.parquet 'geo' bbox starts at -180 longitude")
                    .isCloseTo(-180.0, within(1e-6));
            assertThat(bounds.xmax()).isCloseTo(180.0, within(1e-6));
            assertThat(bounds.ymin()).isCloseTo(-18.288, within(1e-3));
            assertThat(bounds.ymax()).isCloseTo(83.2332, within(1e-3));
            assertThat(source.rowGroupBounds(geometry, 0))
                    .as("the 'geo' JSON bbox is file-level only; per-row-group queries return empty")
                    .isEmpty();
        }
    }

    private static String lookupGeoJson(FileMetaData footer) {
        return footer.keyValueMetadata().stream()
                .filter(kv -> "geo".equals(kv.key()))
                .map(kv -> kv.value().orElse(""))
                .findFirst()
                .orElse(null);
    }
}
