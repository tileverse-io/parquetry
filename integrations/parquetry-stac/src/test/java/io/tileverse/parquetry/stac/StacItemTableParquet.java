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
package io.tileverse.parquetry.stac;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import io.tileverse.parquetry.data.ParquetFileWriter;
import io.tileverse.parquetry.data.ParquetRecordBatchBuilder;
import io.tileverse.parquetry.data.WriteOptions;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.Repetition;
import io.tileverse.parquetry.schema.SchemaNode;

/**
 * Writes a stac-geoparquet item-table file for the index-reader tests: one row per STAC item, holding the columns the
 * specification defines - {@code type}, {@code stac_version}, {@code id}, a WKB {@code geometry}, a nested {@code bbox}
 * struct, an {@code assets} struct with one sub-struct per asset key, {@code collection}, and a microsecond
 * {@code datetime} - plus the item-table metadata document under the {@code stac-geoparquet} footer key.
 *
 * <p>Every column is optional and every scenario knob lives in {@link Options}, which lets a test drop the columns a
 * reader must tolerate missing.
 */
final class StacItemTableParquet {

    private StacItemTableParquet() {}

    /**
     * One entry of an item's asset dictionary. {@code key} is the dictionary key, which becomes the name of the asset's
     * sub-struct; {@code alternateS3Href} fills the {@code alternate.s3.href} sub-struct when non-null.
     */
    record Asset(String key, String href, String type, String title, List<String> roles, String alternateS3Href) {

        static Asset data(String href) {
            return new Asset("data", href, "application/vnd.apache.parquet", null, null, null);
        }
    }

    /**
     * One item row. A null {@code id} or {@code datetime} writes a null in its column. The bbox is all four corners or
     * none: an item with no bbox writes neither the {@code bbox} struct nor the {@code geometry} derived from it, and a
     * partially specified one is rejected rather than silently dropped. {@code datetime} is an ISO instant string.
     */
    record Item(
            String id,
            String collection,
            Double xmin,
            Double ymin,
            Double xmax,
            Double ymax,
            String datetime,
            List<Asset> assets) {

        Item {
            boolean complete = xmin != null && ymin != null && xmax != null && ymax != null;
            boolean absent = xmin == null && ymin == null && xmax == null && ymax == null;
            if (!complete && !absent) {
                throw new IllegalArgumentException("Item " + id + " has a partial bbox: pass all four corners or none");
            }
        }

        /** Whether the item has a bbox; the constructor guarantees the four corners are either all set or all null. */
        boolean hasBbox() {
            return xmin != null;
        }
    }

    /**
     * Which optional columns the file declares, the physical type of the bbox corners, and the item-table metadata
     * document to write in the footer.
     */
    record Options(
            boolean idColumn,
            boolean bboxColumn,
            PrimitiveKind bboxCornerKind,
            boolean collectionColumn,
            boolean datetimeColumn,
            String stacGeoparquetKv) {

        static Options defaults() {
            return new Options(true, true, PrimitiveKind.DOUBLE, true, true, null);
        }

        Options withKv(String kv) {
            return new Options(idColumn, bboxColumn, bboxCornerKind, collectionColumn, datetimeColumn, kv);
        }

        Options withoutCollectionColumn() {
            return new Options(idColumn, bboxColumn, bboxCornerKind, false, datetimeColumn, stacGeoparquetKv);
        }

        Options withoutIdColumn() {
            return new Options(false, bboxColumn, bboxCornerKind, collectionColumn, datetimeColumn, stacGeoparquetKv);
        }

        /**
         * Writes the bbox corners with the given physical type. {@link PrimitiveKind#DOUBLE} and
         * {@link PrimitiveKind#FLOAT} are the coordinate types producers write; {@link PrimitiveKind#BYTE_ARRAY} models
         * a corrupt item-table whose corners are text.
         */
        Options withBboxCornerKind(PrimitiveKind kind) {
            return new Options(idColumn, bboxColumn, kind, collectionColumn, datetimeColumn, stacGeoparquetKv);
        }
    }

    private static final String STAC_GEOPARQUET_KEY = "stac-geoparquet";
    private static final String FEATURE_TYPE = "Feature";
    private static final String STAC_VERSION_VALUE = "1.1.0";

    private static final String ASSETS = "assets";
    private static final String ASSET_HREF = "href";
    private static final String ASSET_TYPE = "type";
    private static final String ASSET_TITLE = "title";
    private static final String ASSET_ROLES = "roles";
    private static final String ASSET_ALTERNATE = "alternate";
    private static final String ALTERNATE_S3 = "s3";

    private static final ColumnPath TYPE = ColumnPath.of("type");
    private static final ColumnPath STAC_VERSION = ColumnPath.of("stac_version");
    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath GEOMETRY = ColumnPath.of("geometry");
    private static final ColumnPath BBOX = ColumnPath.of("bbox");
    private static final ColumnPath BBOX_XMIN = ColumnPath.of("bbox", "xmin");
    private static final ColumnPath BBOX_YMIN = ColumnPath.of("bbox", "ymin");
    private static final ColumnPath BBOX_XMAX = ColumnPath.of("bbox", "xmax");
    private static final ColumnPath BBOX_YMAX = ColumnPath.of("bbox", "ymax");
    private static final ColumnPath ASSETS_PATH = ColumnPath.of(ASSETS);
    private static final ColumnPath COLLECTION = ColumnPath.of("collection");
    private static final ColumnPath DATETIME = ColumnPath.of("datetime");

    static Path write(Path file, List<Item> items) throws Exception {
        return write(file, items, Options.defaults());
    }

    static Path write(Path file, List<Item> items, Options options) throws Exception {
        List<String> assetKeys = assetKeys(items);
        ParquetSchema schema = itemTableSchema(options, assetKeys);
        try (ParquetFileWriter writer =
                ParquetFileWriter.create(Files.newOutputStream(file), schema, writeOptions(file, options))) {
            ParquetRecordBatchBuilder appender = writer.appender();
            for (Item item : items) {
                writeItem(appender, item, options, assetKeys);
            }
        }
        return file;
    }

    private static WriteOptions writeOptions(Path file, Options options) {
        WriteOptions.Builder builder = WriteOptions.builder().tempDir(file.getParent());
        if (options.stacGeoparquetKv() != null) {
            builder.keyValueMetadata(STAC_GEOPARQUET_KEY, options.stacGeoparquetKv());
        }
        return builder.build();
    }

    // --- row authoring ---

    private static void writeItem(
            ParquetRecordBatchBuilder appender, Item item, Options options, List<String> assetKeys) {
        appender.setString(TYPE, FEATURE_TYPE);
        appender.setString(STAC_VERSION, STAC_VERSION_VALUE);
        if (options.idColumn()) {
            writeString(appender, ID, item.id());
        }
        writeGeometry(appender, item);
        if (options.bboxColumn()) {
            writeBbox(appender, item, options.bboxCornerKind());
        }
        writeAssets(appender, item, assetKeys);
        if (options.collectionColumn()) {
            writeString(appender, COLLECTION, item.collection());
        }
        if (options.datetimeColumn()) {
            writeDatetime(appender, item);
        }
        appender.endRow();
    }

    private static void writeGeometry(ParquetRecordBatchBuilder appender, Item item) {
        if (!item.hasBbox()) {
            appender.setNull(GEOMETRY);
            return;
        }
        double x = (item.xmin() + item.xmax()) / 2;
        double y = (item.ymin() + item.ymax()) / 2;
        appender.setBinary(GEOMETRY, MemorySegment.ofArray(wkbPoint(x, y)).asReadOnly());
    }

    private static void writeBbox(ParquetRecordBatchBuilder appender, Item item, PrimitiveKind cornerKind) {
        if (!item.hasBbox()) {
            appender.setNull(BBOX);
            return;
        }
        appender.beginStruct(BBOX);
        writeCorner(appender, BBOX_XMIN, item.xmin(), cornerKind);
        writeCorner(appender, BBOX_YMIN, item.ymin(), cornerKind);
        writeCorner(appender, BBOX_XMAX, item.xmax(), cornerKind);
        writeCorner(appender, BBOX_YMAX, item.ymax(), cornerKind);
        appender.endStruct();
    }

    private static void writeCorner(
            ParquetRecordBatchBuilder appender, ColumnPath path, double value, PrimitiveKind cornerKind) {
        switch (cornerKind) {
            case DOUBLE -> appender.setDouble(path, value);
            case FLOAT -> appender.setFloat(path, (float) value);
            case BYTE_ARRAY -> appender.setString(path, Double.toString(value));
            default -> throw new IllegalArgumentException("bbox corners cannot be written as " + cornerKind);
        }
    }

    private static void writeAssets(ParquetRecordBatchBuilder appender, Item item, List<String> assetKeys) {
        if (assetKeys.isEmpty()) {
            return;
        }
        appender.beginStruct(ASSETS_PATH);
        for (String key : assetKeys) {
            Optional<Asset> asset = assetOf(item, key);
            if (asset.isPresent()) {
                writeAsset(appender, asset.orElseThrow());
            } else {
                appender.setNull(assetPath(key));
            }
        }
        appender.endStruct();
    }

    private static void writeAsset(ParquetRecordBatchBuilder appender, Asset asset) {
        String key = asset.key();
        appender.beginStruct(assetPath(key));
        writeString(appender, assetPath(key, ASSET_HREF), asset.href());
        writeString(appender, assetPath(key, ASSET_TYPE), asset.type());
        writeString(appender, assetPath(key, ASSET_TITLE), asset.title());
        writeRoles(appender, key, asset.roles());
        writeAlternate(appender, key, asset.alternateS3Href());
        appender.endStruct();
    }

    private static void writeRoles(ParquetRecordBatchBuilder appender, String key, List<String> roles) {
        ColumnPath path = assetPath(key, ASSET_ROLES);
        if (roles == null) {
            appender.setNull(path);
            return;
        }
        appender.beginList(path);
        for (String role : roles) {
            appender.addString(role);
        }
        appender.endList();
    }

    private static void writeAlternate(ParquetRecordBatchBuilder appender, String key, String s3Href) {
        if (s3Href == null) {
            appender.setNull(assetPath(key, ASSET_ALTERNATE));
            return;
        }
        appender.setString(assetPath(key, ASSET_ALTERNATE, ALTERNATE_S3, ASSET_HREF), s3Href);
    }

    private static void writeDatetime(ParquetRecordBatchBuilder appender, Item item) {
        if (item.datetime() == null) {
            appender.setNull(DATETIME);
            return;
        }
        Instant instant = Instant.parse(item.datetime());
        appender.setLong(DATETIME, ChronoUnit.MICROS.between(Instant.EPOCH, instant));
    }

    private static void writeString(ParquetRecordBatchBuilder appender, ColumnPath path, String value) {
        if (value == null) {
            appender.setNull(path);
            return;
        }
        appender.setString(path, value);
    }

    /** A 2D point in little-endian WKB: byte-order flag 1, geometry code 1 (Point), then the coordinates. */
    private static byte[] wkbPoint(double x, double y) {
        ByteBuffer buffer = ByteBuffer.allocate(21).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 1).putInt(1).putDouble(x).putDouble(y);
        return buffer.array();
    }

    // --- asset dictionary ---

    /** The asset keys of all items, in first-seen order; each becomes one sub-struct of the {@code assets} column. */
    private static List<String> assetKeys(List<Item> items) {
        Set<String> keys = new LinkedHashSet<>();
        for (Item item : items) {
            for (Asset asset : assetsOf(item)) {
                keys.add(asset.key());
            }
        }
        return List.copyOf(keys);
    }

    private static Optional<Asset> assetOf(Item item, String key) {
        return assetsOf(item).stream().filter(asset -> asset.key().equals(key)).findFirst();
    }

    private static List<Asset> assetsOf(Item item) {
        if (item.assets() == null) {
            return List.of();
        }
        return item.assets();
    }

    private static ColumnPath assetPath(String key, String... fields) {
        List<String> parts = new ArrayList<>();
        parts.add(ASSETS);
        parts.add(key);
        parts.addAll(List.of(fields));
        return ColumnPath.of(parts);
    }

    // --- schema ---

    private static ParquetSchema itemTableSchema(Options options, List<String> assetKeys) {
        List<SchemaNode> fields = new ArrayList<>();
        fields.add(stringLeaf("type"));
        fields.add(stringLeaf("stac_version"));
        if (options.idColumn()) {
            fields.add(stringLeaf("id"));
        }
        fields.add(wkbLeaf("geometry"));
        if (options.bboxColumn()) {
            fields.add(bboxGroup(options.bboxCornerKind()));
        }
        // Parquet has no empty group: a fixture whose items declare no asset at all omits the column entirely.
        if (!assetKeys.isEmpty()) {
            fields.add(assetsGroup(assetKeys));
        }
        if (options.collectionColumn()) {
            fields.add(stringLeaf("collection"));
        }
        if (options.datetimeColumn()) {
            fields.add(timestampLeaf("datetime"));
        }
        SchemaNode.Group root =
                new SchemaNode.Group("schema", Repetition.REQUIRED, List.copyOf(fields), Optional.empty(), -1);
        return new ParquetSchema(root);
    }

    private static SchemaNode.Group bboxGroup(PrimitiveKind cornerKind) {
        List<SchemaNode> corners = List.of(
                cornerLeaf("xmin", cornerKind),
                cornerLeaf("ymin", cornerKind),
                cornerLeaf("xmax", cornerKind),
                cornerLeaf("ymax", cornerKind));
        return new SchemaNode.Group("bbox", Repetition.OPTIONAL, corners, Optional.empty(), -1);
    }

    private static SchemaNode.Group assetsGroup(List<String> assetKeys) {
        List<SchemaNode> perKey = new ArrayList<>();
        for (String key : assetKeys) {
            perKey.add(assetGroup(key));
        }
        return new SchemaNode.Group(ASSETS, Repetition.OPTIONAL, List.copyOf(perKey), Optional.empty(), -1);
    }

    private static SchemaNode.Group assetGroup(String key) {
        List<SchemaNode> fields = List.of(
                stringLeaf(ASSET_HREF),
                stringLeaf(ASSET_TYPE),
                stringLeaf(ASSET_TITLE),
                rolesGroup(),
                alternateGroup());
        return new SchemaNode.Group(key, Repetition.OPTIONAL, fields, Optional.empty(), -1);
    }

    private static SchemaNode.Group rolesGroup() {
        SchemaNode.Primitive element = stringLeaf("element");
        SchemaNode.Group repeated =
                new SchemaNode.Group("list", Repetition.REPEATED, List.of(element), Optional.empty(), -1);
        return new SchemaNode.Group(
                ASSET_ROLES, Repetition.OPTIONAL, List.of(repeated), Optional.of(new LogicalType.ListType()), -1);
    }

    private static SchemaNode.Group alternateGroup() {
        SchemaNode.Group s3 = new SchemaNode.Group(
                ALTERNATE_S3, Repetition.OPTIONAL, List.of(stringLeaf(ASSET_HREF)), Optional.empty(), -1);
        return new SchemaNode.Group(ASSET_ALTERNATE, Repetition.OPTIONAL, List.of(s3), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive stringLeaf(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.BYTE_ARRAY,
                OptionalInt.empty(),
                Optional.of(new LogicalType.StringType()),
                -1);
    }

    private static SchemaNode.Primitive wkbLeaf(String name) {
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, PrimitiveKind.BYTE_ARRAY, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive cornerLeaf(String name, PrimitiveKind cornerKind) {
        if (cornerKind == PrimitiveKind.BYTE_ARRAY) {
            return stringLeaf(name);
        }
        return new SchemaNode.Primitive(
                name, Repetition.OPTIONAL, cornerKind, OptionalInt.empty(), Optional.empty(), -1);
    }

    private static SchemaNode.Primitive timestampLeaf(String name) {
        return new SchemaNode.Primitive(
                name,
                Repetition.OPTIONAL,
                PrimitiveKind.INT64,
                OptionalInt.empty(),
                Optional.of(new LogicalType.Timestamp(true, LogicalType.TimeUnit.MICROS)),
                -1);
    }
}
