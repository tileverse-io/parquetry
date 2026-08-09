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
package io.tileverse.parquetry.internal.write;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.tileverse.parquetry.data.ParquetFileReader;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.schema.geo.geoparquet.BboxCovering;
import io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata;
import io.tileverse.parquetry.testsupport.Wkb;

/**
 * Pins the default consequence of a plain geo write: with only a WGS84 CRS declared on the geometry column and no
 * explicit {@link WriteOptions.CoveringMode}, the writer emits a GeoParquet 1.1 {@code bbox} covering with FLOAT
 * leaves.
 *
 * <p>The default GeoParquet metadata mode is {@code DUAL_V1_1_AND_V2_0}, under which an unset covering choice resolves
 * to AUTO, and AUTO over a CRS84/EPSG:4326 geometry picks FLOAT. This test guards that chain end to end: a caller who
 * writes geometry the ordinary way gets a page-prunable float covering for free, without ever handing the writer a bbox
 * column.
 */
class DefaultGeoWriteCoveringTest {

    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");

    @TempDir
    Path tempDir;

    @Test
    void defaultGeoWriteEmitsAFloatCovering() throws Exception {
        Path file = writeGeometryOnlyFileWithDefaults();

        try (ByteRangeSource source = ByteRangeSource.ofFile(file)) {
            ParquetFileReader reader = ParquetFileReader.open(source);

            String geoJson = reader.keyValueMetadata().get("geo");
            assertThat(geoJson)
                    .as("the default DUAL metadata mode emits the GeoParquet 1.1 \"geo\" block")
                    .isNotNull();

            GeoParquetMetadata parsed = GeoParquetMetadata.parse(geoJson);
            BboxCovering covering =
                    parsed.columns().get("geometry").covering().orElseThrow().bbox();
            assertThat(covering.xmin()).isEqualTo(ColumnPath.of("bbox", "xmin"));
            assertThat(covering.ymin()).isEqualTo(ColumnPath.of("bbox", "ymin"));
            assertThat(covering.xmax()).isEqualTo(ColumnPath.of("bbox", "xmax"));
            assertThat(covering.ymax()).isEqualTo(ColumnPath.of("bbox", "ymax"));

            SchemaNode.Primitive xminLeaf = (SchemaNode.Primitive)
                    reader.schema().find(ColumnPath.of("bbox", "xmin")).orElseThrow();
            assertThat(xminLeaf.kind())
                    .as("a default geo write over a WGS84 geometry emits a FLOAT covering")
                    .isEqualTo(PrimitiveKind.FLOAT);
        }
    }

    private Path writeGeometryOnlyFileWithDefaults() throws Exception {
        ParquetSchema schema = geometryOnlySchema();
        WriteOptions options = WriteOptions.builder()
                .tempDir(tempDir)
                .crsEpsg("geometry", 4326)
                .build();
        Path file = tempDir.resolve("default-geo-covering.parquet");
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(WriteFixtures.batch(schema, marchingPointRows()));
        }
        return file;
    }

    private static List<Map<ColumnPath, Object>> marchingPointRows() {
        List<Map<ColumnPath, Object>> rows = new ArrayList<>();
        for (int id = 0; id < 16; id++) {
            String wkt = "POINT (%d 5)".formatted(id);
            rows.add(Map.of(GEOMETRY, Wkb.fromWkt(wkt)));
        }
        return rows;
    }

    private static ParquetSchema geometryOnlySchema() {
        SchemaNode.Group root = new SchemaNode.Group(
                "schema", Repetition.REQUIRED, List.of(requiredBinary("geometry")), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive requiredBinary(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }
}
