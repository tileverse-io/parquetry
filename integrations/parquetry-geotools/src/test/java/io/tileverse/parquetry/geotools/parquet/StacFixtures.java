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
package io.tileverse.parquetry.geotools.parquet;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;

import io.tileverse.parquetry.columnar.BinaryVector;
import io.tileverse.parquetry.columnar.ColumnVector;
import io.tileverse.parquetry.columnar.DefaultParquetRecordBatch;
import io.tileverse.parquetry.columnar.DoubleVector;
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
 * Materializes trimmed Overture-shaped STAC catalogs on disk for the factory tests, in both flavors the factory can
 * open. {@link #writeOvertureMini} produces the JSON flavor: a {@code catalog.json}, one {@code building} collection
 * with two items, and the two GeoParquet parts the items reference. {@link #writeItemTable} produces the
 * stac-geoparquet flavor: an {@code items.parquet} index whose rows point at the same two parts. Both flavors reference
 * the same GeoParquet parts, which hold disjoint 2D points (west and east) and are written in GeoParquet 2.0 mode so
 * native per-row-group bounds exist.
 */
final class StacFixtures {

    private static final String GEOMETRY_COLUMN = "geometry";

    private StacFixtures() {}

    /** Writes the JSON-catalog tree under {@code root} and returns the path to {@code catalog.json}. */
    static Path writeOvertureMini(Path root) throws Exception {
        writeJsonDocuments(root);
        writeParquetParts(root);
        return root.resolve("catalog.json");
    }

    /**
     * Writes the stac-geoparquet flavor under {@code root} and returns the path to the {@code items.parquet} index. The
     * index holds one row per item, each pointing at a GeoParquet part written under {@code parts/}; the factory opens
     * the index through {@code GeoParquetStacReader} and resolves each part relative to the index's container.
     */
    static Path writeItemTable(Path root) throws Exception {
        writeParquetParts(root);
        Path index = root.resolve("items.parquet");
        writeItemTableIndex(
                index,
                List.of(
                        new ItemRow("item-west", "building", 0, 0, 10, 10, "building/parts/west.parquet"),
                        new ItemRow("item-east", "building", 100, 0, 110, 10, "building/parts/east.parquet")));
        return index;
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

    /** One item-table row: the item id, its collection, a flat bbox, and the href of its GeoParquet data part. */
    private record ItemRow(
            String id, String collection, double xmin, double ymin, double xmax, double ymax, String assetHref) {}

    private static void writeItemTableIndex(Path file, List<ItemRow> rows) throws Exception {
        ParquetSchema schema = itemTableSchema();
        WriteOptions options = WriteOptions.builder().tempDir(file.getParent()).build();
        try (ParquetFileWriter writer = ParquetFileWriter.create(Files.newOutputStream(file), schema, options);
                ParquetRecordBatch batch = itemTableBatch(schema, rows)) {
            writer.writeBatch(batch);
        }
    }

    private static ParquetRecordBatch itemTableBatch(ParquetSchema schema, List<ItemRow> rows) {
        int count = rows.size();
        Validity allValid = Validity.allValid(count);
        Map<ColumnPath, ColumnVector> columns = new LinkedHashMap<>();
        columns.put(ColumnPath.of("item_id"), utf8Column(rows, ItemRow::id, allValid));
        columns.put(ColumnPath.of("asset_href"), utf8Column(rows, ItemRow::assetHref, allValid));
        columns.put(ColumnPath.of("bbox_xmin"), doubleColumn(rows, ItemRow::xmin, allValid));
        columns.put(ColumnPath.of("bbox_ymin"), doubleColumn(rows, ItemRow::ymin, allValid));
        columns.put(ColumnPath.of("bbox_xmax"), doubleColumn(rows, ItemRow::xmax, allValid));
        columns.put(ColumnPath.of("bbox_ymax"), doubleColumn(rows, ItemRow::ymax, allValid));
        columns.put(ColumnPath.of("collection"), utf8Column(rows, ItemRow::collection, allValid));
        return new DefaultParquetRecordBatch(schema, columns, count, Arena.ofShared());
    }

    private static ColumnVector utf8Column(List<ItemRow> rows, Function<ItemRow, String> field, Validity validity) {
        MemorySegment[] values = new MemorySegment[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            byte[] utf8 = field.apply(rows.get(i)).getBytes(StandardCharsets.UTF_8);
            values[i] = MemorySegment.ofArray(utf8);
        }
        return BinaryVector.materialized(values, validity);
    }

    private static ColumnVector doubleColumn(List<ItemRow> rows, ToDoubleFunction<ItemRow> field, Validity validity) {
        double[] values = new double[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            values[i] = field.applyAsDouble(rows.get(i));
        }
        return DoubleVector.materialized(values, validity);
    }

    private static ParquetSchema itemTableSchema() {
        List<SchemaNode> leaves = List.of(
                utf8Leaf("item_id"),
                utf8Leaf("asset_href"),
                doubleLeaf("bbox_xmin"),
                doubleLeaf("bbox_ymin"),
                doubleLeaf("bbox_xmax"),
                doubleLeaf("bbox_ymax"),
                utf8Leaf("collection"));
        SchemaNode.Group root = new SchemaNode.Group("schema", Repetition.REQUIRED, leaves, Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Primitive utf8Leaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive doubleLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.REQUIRED, PrimitiveKind.DOUBLE, OptionalInt.empty(), Optional.empty(), -1);
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
