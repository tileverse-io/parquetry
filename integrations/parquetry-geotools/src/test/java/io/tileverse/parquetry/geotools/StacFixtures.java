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
package io.tileverse.parquetry.geotools;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.ParquetRecordBatch;
import io.tileverse.parquetry.columnar.Validity;
import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.data.WriteOptions.GeoParquetMetadataMode;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Materializes a trimmed Overture-shaped STAC catalog tree on disk for the factory test: a {@code catalog.json}, one
 * {@code building} collection with two items, and the two GeoParquet parts the items reference. The parts hold disjoint
 * 2D points (west and east) and are written in GeoParquet 2.0 mode so native per-row-group bounds exist.
 */
final class StacFixtures {

    private static final String GEOMETRY_COLUMN = "geometry";

    private StacFixtures() {}

    /** Writes the whole tree under {@code root} and returns the path to {@code catalog.json}. */
    static Path writeOvertureMini(Path root) throws Exception {
        writeJsonDocuments(root);
        writeParquetParts(root);
        return root.resolve("catalog.json");
    }

    private static void writeJsonDocuments(Path root) throws Exception {
        Path building = Files.createDirectories(root.resolve("building"));
        Path items = Files.createDirectories(building.resolve("items"));

        writeText(root.resolve("catalog.json"), CATALOG_JSON);
        writeText(building.resolve("collection.json"), COLLECTION_JSON);
        writeText(items.resolve("item-west.json"), ITEM_WEST_JSON);
        writeText(items.resolve("item-east.json"), ITEM_EAST_JSON);
    }

    private static void writeParquetParts(Path root) throws Exception {
        Path parts = Files.createDirectories(root.resolve("building/parts"));
        writePoints(parts.resolve("west.parquet"), new double[][] {{0, 0}, {5, 5}});
        writePoints(parts.resolve("east.parquet"), new double[][] {{100, 0}, {105, 5}});
    }

    private static void writeText(Path file, String content) throws Exception {
        Files.writeString(file, content, StandardCharsets.UTF_8);
    }

    private static void writePoints(Path file, double[][] points) throws Exception {
        ParquetSchema schema = flatGeometrySchema(GEOMETRY_COLUMN);
        WriteOptions options = WriteOptions.builder()
                .tempDir(file.getParent())
                .geoParquetMetadata(GeoParquetMetadataMode.DUAL_V1_1_AND_V2_0)
                .crsEpsg(GEOMETRY_COLUMN, 4326)
                .build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options)) {
            writer.writeBatch(pointBatch(schema, GEOMETRY_COLUMN, points));
        }
    }

    private static ParquetRecordBatch pointBatch(ParquetSchema schema, String column, double[][] points) {
        MemorySegment[] wkb = new MemorySegment[points.length];
        for (int i = 0; i < points.length; i++) {
            wkb[i] = MemorySegment.ofArray(wkbPoint(points[i][0], points[i][1]));
        }
        BitSet allValid = new BitSet(points.length);
        allValid.set(0, points.length);
        Validity validity = Validity.of(allValid, points.length);
        Map<ColumnPath, ColumnVector> columns = Map.of(ColumnPath.of(column), BinaryVector.materialized(wkb, validity));
        return new DefaultParquetRecordBatch(schema, columns, points.length, Arena.ofShared());
    }

    private static ParquetSchema flatGeometrySchema(String column) {
        SchemaNode.Primitive leaf = new SchemaNode.Primitive(
                column, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.of(leaf), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1);
        buffer.putInt(1);
        buffer.putDouble(x);
        buffer.putDouble(y);
        return buffer.array();
    }

    private static final String CATALOG_JSON = """
            {
              "type": "Catalog",
              "stac_version": "1.0.0",
              "id": "overture-mini",
              "description": "A trimmed Overture-shaped STAC catalog for tests",
              "links": [
                { "rel": "self", "href": "catalog.json" },
                { "rel": "child", "href": "building/collection.json" }
              ]
            }
            """;

    private static final String COLLECTION_JSON = """
            {
              "type": "Collection",
              "stac_version": "1.0.0",
              "id": "building",
              "title": "Buildings",
              "description": "Building footprints",
              "license": "ODbL-1.0",
              "extent": {
                "spatial": { "bbox": [[0, 0, 120, 20]] },
                "temporal": { "interval": [["2024-01-01T00:00:00Z", null]] }
              },
              "links": [
                { "rel": "self", "href": "collection.json" },
                { "rel": "item", "href": "items/item-west.json" },
                { "rel": "item", "href": "items/item-east.json" },
                { "rel": "pmtiles", "href": "buildings.pmtiles", "type": "application/vnd.pmtiles", "title": "Buildings tiles" }
              ]
            }
            """;

    private static final String ITEM_WEST_JSON = """
            {
              "type": "Feature",
              "stac_version": "1.0.0",
              "id": "item-west",
              "bbox": [0, 0, 10, 10],
              "properties": { "datetime": "2024-01-01T00:00:00Z" },
              "geometry": null,
              "links": [],
              "assets": {
                "data": { "href": "../parts/west.parquet", "type": "application/vnd.apache.parquet", "title": "West part", "roles": ["data"] }
              }
            }
            """;

    private static final String ITEM_EAST_JSON = """
            {
              "type": "Feature",
              "stac_version": "1.0.0",
              "id": "item-east",
              "bbox": [100, 0, 110, 10],
              "properties": { "datetime": "2024-01-01T00:00:00Z" },
              "geometry": null,
              "links": [],
              "assets": {
                "data": { "href": "../parts/east.parquet", "type": "application/vnd.apache.parquet", "title": "East part", "roles": ["data"] }
              }
            }
            """;
}
