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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SequencedMap;
import java.util.Set;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.format.LogicalType;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.schema.ParquetSchema;
import io.tileverse.parquetry.schema.PrimitiveKind;
import io.tileverse.parquetry.schema.SchemaNode;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCatalogReader;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacExtent;
import io.tileverse.stac.StacFormatException;
import io.tileverse.stac.StacGeoparquetMetadata;
import io.tileverse.stac.StacGeoparquetMetadata.EmbeddedCollection;
import io.tileverse.stac.StacItem;

/**
 * A {@link StacCatalogReader} over a stac-geoparquet item-table used as a catalog index: each row is a STAC item whose
 * assets point at external GeoParquet parts, the direct equivalent of a static JSON catalog. This reads the item-table
 * through parquetry and maps each row to a {@link StacItem}, producing the same model the JSON reader produces. An
 * item-table whose rows are feature data (not an index of external parts) is out of scope.
 *
 * <p>The file is read in the shape the stac-geoparquet specification defines. The columns consumed are {@code id}, the
 * {@code bbox} struct's four planar corners, the optional {@code collection} and timestamp {@code datetime}, and the
 * {@code assets} struct, whose named sub-structs each contribute one asset's {@code href}, {@code type}, {@code title}
 * and {@code roles}. Everything else a producer writes - the geometry, links, STAC extensions, an asset's alternate
 * href dictionary, property columns - is left out of the projection and never decoded.
 *
 * <p>The {@code stac-geoparquet} footer key-value metadata, when present, declares the collections the rows belong to;
 * its title and extent win over what the rows imply. A collection the rows name but the metadata does not declare gets
 * an extent aggregated from its items' bounding boxes, and a collection the metadata declares but no row names is
 * reported with no items. Rows that name no collection at all belong to the single declared collection, or to a
 * collection named after the item-table file.
 */
public final class GeoParquetStacReader implements StacCatalogReader {

    private static final String STAC_GEOPARQUET_KEY = "stac-geoparquet";
    private static final String PARQUET_SUFFIX = ".parquet";

    /** The id the exposed catalog takes; an item-table declares no catalog id of its own. */
    private static final String CATALOG_ID = "stac-geoparquet";

    private static final ColumnPath ID = ColumnPath.of("id");
    private static final ColumnPath COLLECTION = ColumnPath.of("collection");
    private static final ColumnPath DATETIME = ColumnPath.of("datetime");

    private static final String BBOX = "bbox";
    private static final List<String> BBOX_CORNERS = List.of("xmin", "ymin", "xmax", "ymax");

    private static final String ASSETS = "assets";
    private static final String HREF = "href";
    private static final String TYPE = "type";
    private static final String TITLE = "title";
    private static final String ROLES = "roles";
    private static final Set<String> ASSET_SCALARS = Set.of(HREF, TYPE, TITLE);

    @Override
    public StacCatalog open(URI itemTableUri, Storage storage) {
        Objects.requireNonNull(itemTableUri, "itemTableUri");
        Objects.requireNonNull(storage, "storage");
        List<StacCollection> collections = readCollections(itemTableUri, storage);
        return new StacCatalog(CATALOG_ID, null, List.of(), () -> collections, List::of);
    }

    @SuppressWarnings("MustBeClosedChecker") // the source borrows the RangeReader closed by the try-with-resources
    private List<StacCollection> readCollections(URI itemTableUri, Storage storage) {
        String key = storageKey(itemTableUri);
        try (RangeReader reader = storage.openRangeReader(key)) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            SequencedMap<String, EmbeddedCollection> declared = readDeclaredCollections(source);
            ItemTable table = itemTable(itemTableUri, key, source.schema(), declared);
            SequencedMap<String, List<StacItem>> byCollection = readItems(source, table);
            return assemble(byCollection, declared);
        } catch (IOException failure) {
            throw new UncheckedIOException("reading STAC item-table " + key, failure);
        }
    }

    private static SequencedMap<String, EmbeddedCollection> readDeclaredCollections(ParquetSource source) {
        return StacGeoparquetMetadata.parseCollections(source.keyValueMetadata().get(STAC_GEOPARQUET_KEY));
    }

    private static ItemTable itemTable(
            URI itemTableUri, String key, ParquetSchema schema, SequencedMap<String, EmbeddedCollection> declared) {
        return new ItemTable(
                key, itemTableUri.resolve("."), introspect(schema, key), fallbackCollectionId(declared, key));
    }

    /**
     * One item-table being read: the columns its schema exposes, plus the constants every row mapping needs - the file
     * name an error quotes, the container asset hrefs resolve against, and the collection id a row that names none
     * belongs to.
     */
    private record ItemTable(String key, URI container, ItemTableColumns columns, String fallbackCollectionId) {}

    /** The leaves one item-table's read projects, and how to reach the values behind them. */
    private record ItemTableColumns(
            List<ColumnPath> projected,
            List<BboxCorner> bbox,
            boolean hasCollection,
            Optional<LogicalType.Timestamp> datetime,
            SequencedMap<String, AssetColumns> assetsByKey) {}

    /** One planar bbox corner and the accessor its physical type dictates, resolved once when the file is opened. */
    private record BboxCorner(String name, ColumnPath leaf, CornerAccessor accessor) {

        double read(ParquetRecord row) {
            return accessor.read(row, leaf);
        }
    }

    /** Reads one bbox corner as a coordinate, whatever floating-point type the producer wrote it as. */
    @FunctionalInterface
    private interface CornerAccessor {
        double read(ParquetRecord row, ColumnPath leaf);
    }

    /** The columns one asset key exposes; a producer may write any subset of them. */
    private record AssetColumns(
            Optional<ColumnPath> href,
            Optional<ColumnPath> type,
            Optional<ColumnPath> title,
            Optional<ColumnPath> roles) {

        static final AssetColumns NONE =
                new AssetColumns(Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

        AssetColumns with(String field, ColumnPath leaf) {
            Optional<ColumnPath> path = Optional.of(leaf);
            return switch (field) {
                case HREF -> new AssetColumns(path, type, title, roles);
                case TYPE -> new AssetColumns(href, path, title, roles);
                case TITLE -> new AssetColumns(href, type, path, roles);
                case ROLES -> new AssetColumns(href, type, title, path);
                default -> this;
            };
        }
    }

    // --- schema introspection ---

    /** Resolves what to read from one item-table's schema, walking its leaves once. */
    private static ItemTableColumns introspect(ParquetSchema schema, String key) {
        List<ColumnPath> projected = new ArrayList<>();
        Map<String, BboxCorner> bboxCorners = new HashMap<>();
        SequencedMap<String, AssetColumns> assetsByKey = new LinkedHashMap<>();
        boolean hasId = false;
        boolean hasCollection = false;
        Optional<LogicalType.Timestamp> datetime = Optional.empty();
        for (ColumnPath leaf : schema.leafColumns()) {
            if (isStringColumn(schema, leaf, ID)) {
                hasId = true;
                projected.add(leaf);
            } else if (isBboxCorner(leaf)) {
                bboxCorners.put(leaf.name(), bboxCorner(schema, leaf, key));
                projected.add(leaf);
            } else if (isStringColumn(schema, leaf, COLLECTION)) {
                hasCollection = true;
                projected.add(leaf);
            } else if (DATETIME.equals(leaf)) {
                datetime = timestampOf(schema, leaf);
                if (datetime.isPresent()) {
                    projected.add(leaf);
                }
            } else if (isAssetField(schema, leaf)) {
                registerAssetField(assetsByKey, leaf);
                projected.add(leaf);
            }
        }
        requireIdColumn(hasId, key);
        return new ItemTableColumns(
                List.copyOf(projected),
                orderedCorners(bboxCorners, key),
                hasCollection,
                datetime,
                Collections.unmodifiableSequencedMap(assetsByKey));
    }

    /** An item-table with no id column describes no item this model can name. */
    private static void requireIdColumn(boolean hasId, String key) {
        if (!hasId) {
            throw new StacFormatException("stac-geoparquet item-table " + key + " has no id column");
        }
    }

    /** The four corners in bbox order; an item-table missing any of them describes no item this model can place. */
    private static List<BboxCorner> orderedCorners(Map<String, BboxCorner> bboxCorners, String key) {
        List<BboxCorner> ordered = new ArrayList<>(BBOX_CORNERS.size());
        for (String name : BBOX_CORNERS) {
            BboxCorner corner = bboxCorners.get(name);
            if (corner == null) {
                throw new StacFormatException("stac-geoparquet item-table " + key + " has no bbox." + name + " column");
            }
            ordered.add(corner);
        }
        return List.copyOf(ordered);
    }

    /**
     * A bbox corner and how to read it. Both floating-point types are accepted: GeoParquet covering bboxes are commonly
     * written as float32 and stac-geoparquet inherits that convention. Any other type is a corrupt item-table.
     */
    private static BboxCorner bboxCorner(ParquetSchema schema, ColumnPath leaf, String key) {
        if (schema.find(leaf).orElse(null) instanceof SchemaNode.Primitive corner) {
            return new BboxCorner(leaf.name(), leaf, cornerAccessor(corner.kind(), leaf, key));
        }
        throw unreadableCorner(leaf, key, "a group");
    }

    private static CornerAccessor cornerAccessor(PrimitiveKind kind, ColumnPath leaf, String key) {
        return switch (kind) {
            case DOUBLE -> ParquetRecord::getDouble;
            case FLOAT -> ParquetRecord::getFloat;
            default -> throw unreadableCorner(leaf, key, kind.name());
        };
    }

    private static StacFormatException unreadableCorner(ColumnPath leaf, String key, String found) {
        return new StacFormatException("stac-geoparquet item-table " + key + " has a " + leaf.dot() + " column of type "
                + found + "; a bbox corner must be DOUBLE or FLOAT");
    }

    private static boolean isStringColumn(ParquetSchema schema, ColumnPath leaf, ColumnPath expected) {
        return expected.equals(leaf) && isStringLeaf(schema, leaf);
    }

    private static boolean isBboxCorner(ColumnPath leaf) {
        return leaf.numParts() == 2 && BBOX.equals(leaf.part(0)) && BBOX_CORNERS.contains(leaf.part(1));
    }

    /**
     * Whether the leaf is one of the asset members the model reads. An asset key's own sub-structs - the alternate href
     * dictionary, storage references - sit deeper under names of their own and stay out of the read.
     */
    private static boolean isAssetField(ParquetSchema schema, ColumnPath leaf) {
        return leaf.numParts() >= 3 && ASSETS.equals(leaf.part(0)) && isAssetMember(leaf) && isStringLeaf(schema, leaf);
    }

    /** A scalar member sits directly under the asset key; a roles list wraps its elements in levels of its own. */
    private static boolean isAssetMember(ColumnPath leaf) {
        String field = leaf.part(2);
        if (ROLES.equals(field)) {
            return true;
        }
        return leaf.numParts() == 3 && ASSET_SCALARS.contains(field);
    }

    private static void registerAssetField(SequencedMap<String, AssetColumns> assetsByKey, ColumnPath leaf) {
        String assetKey = leaf.part(1);
        AssetColumns columns = assetsByKey.getOrDefault(assetKey, AssetColumns.NONE);
        assetsByKey.put(assetKey, columns.with(leaf.part(2), leaf));
    }

    /**
     * Whether the path names a text leaf. Producers emit an all-null column of another type for a member no item fills,
     * and reading such a column as text would fail the decode.
     */
    private static boolean isStringLeaf(ParquetSchema schema, ColumnPath leaf) {
        return schema.find(leaf).orElse(null) instanceof SchemaNode.Primitive primitive
                && primitive.kind() == PrimitiveKind.BYTE_ARRAY;
    }

    /** The leaf's timestamp type, or empty when it is annotated otherwise - some producers write datetime as text. */
    private static Optional<LogicalType.Timestamp> timestampOf(ParquetSchema schema, ColumnPath leaf) {
        if (schema.find(leaf).orElse(null) instanceof SchemaNode.Primitive primitive
                && primitive.logicalType().orElse(null) instanceof LogicalType.Timestamp timestamp) {
            return Optional.of(timestamp);
        }
        return Optional.empty();
    }

    // --- row mapping ---

    private static SequencedMap<String, List<StacItem>> readItems(ParquetSource source, ItemTable table) {
        SequencedMap<String, List<StacItem>> byCollection = new LinkedHashMap<>();
        Projection projection = Projection.ofPhysical(table.columns().projected());
        try (Stream<ParquetRecord> rows = source.read(Predicate.ALWAYS_TRUE, projection, ReadOptions.DEFAULTS)) {
            rows.forEach(row -> byCollection
                    .computeIfAbsent(collectionOf(row, table), id -> new ArrayList<>())
                    .add(toItem(row, table)));
        }
        return byCollection;
    }

    /** The collection a row belongs to: the value it names, or the item-table's fallback when it names none. */
    private static String collectionOf(ParquetRecord row, ItemTable table) {
        if (!table.columns().hasCollection() || row.isNull(COLLECTION)) {
            return table.fallbackCollectionId();
        }
        return row.getString(COLLECTION);
    }

    private static StacItem toItem(ParquetRecord row, ItemTable table) {
        String id = readId(row, table.key());
        double[] bbox = readBbox(row, table.columns().bbox(), id);
        Optional<String> datetime = readDatetime(row, table.columns());
        List<StacAsset> assets = readAssets(row, table, id);
        return new StacItem(id, bbox, datetime, assets, List.of());
    }

    private static String readId(ParquetRecord row, String key) {
        if (row.isNull(ID)) {
            throw new StacFormatException("stac-geoparquet item-table " + key + " has a row with a null id");
        }
        return row.getString(ID);
    }

    private static double[] readBbox(ParquetRecord row, List<BboxCorner> corners, String id) {
        double[] bbox = new double[corners.size()];
        for (int index = 0; index < bbox.length; index++) {
            BboxCorner corner = corners.get(index);
            if (row.isNull(corner.leaf())) {
                throw new StacFormatException("STAC item '" + id + "' has a null bbox." + corner.name());
            }
            bbox[index] = corner.read(row);
        }
        return bbox;
    }

    private static Optional<String> readDatetime(ParquetRecord row, ItemTableColumns columns) {
        Optional<LogicalType.Timestamp> timestamp = columns.datetime();
        if (timestamp.isEmpty() || row.isNull(DATETIME)) {
            return Optional.empty();
        }
        Instant instant =
                toInstant(row.getLong(DATETIME), timestamp.orElseThrow().unit());
        return Optional.of(DateTimeFormatter.ISO_INSTANT.format(instant));
    }

    private static Instant toInstant(long sinceEpoch, LogicalType.TimeUnit unit) {
        return switch (unit) {
            case MILLIS -> Instant.EPOCH.plus(sinceEpoch, ChronoUnit.MILLIS);
            case MICROS -> Instant.EPOCH.plus(sinceEpoch, ChronoUnit.MICROS);
            case NANOS -> Instant.EPOCH.plusNanos(sinceEpoch);
        };
    }

    private static List<StacAsset> readAssets(ParquetRecord row, ItemTable table, String id) {
        List<StacAsset> assets = new ArrayList<>();
        for (AssetColumns columns : table.columns().assetsByKey().values()) {
            readAsset(row, columns, table.container(), id).ifPresent(assets::add);
        }
        return assets;
    }

    /**
     * One asset of a row, or empty when the row declares no href under that key: the asset dictionary is per item, and
     * the file's columns are the union of the keys all its items declare.
     */
    private static Optional<StacAsset> readAsset(ParquetRecord row, AssetColumns columns, URI container, String id) {
        ColumnPath hrefColumn = columns.href().orElse(null);
        if (hrefColumn == null || row.isNull(hrefColumn)) {
            return Optional.empty();
        }
        String href = resolveAssetHref(container, id, row.getString(hrefColumn));
        String type = readString(row, columns.type());
        String title = readString(row, columns.title());
        List<String> roles = readRoles(row, columns.roles());
        return Optional.of(new StacAsset(href, type, title, roles));
    }

    /**
     * Resolves an asset href against the catalog container. A blank or malformed href is a corrupt index, not a
     * recoverable row, and fails loud naming the item rather than aborting the whole catalog open with an opaque error.
     */
    private static String resolveAssetHref(URI container, String itemId, String assetHref) {
        if (assetHref.isBlank()) {
            throw new StacFormatException("STAC item '" + itemId + "' has a blank asset href in the item-table");
        }
        try {
            return container.resolve(assetHref).toString();
        } catch (IllegalArgumentException malformed) {
            throw new StacFormatException(
                    "STAC item '" + itemId + "' has a malformed asset href '" + assetHref + "'", malformed);
        }
    }

    private static String readString(ParquetRecord row, Optional<ColumnPath> column) {
        ColumnPath leaf = column.orElse(null);
        if (leaf == null || row.isNull(leaf)) {
            return null;
        }
        return row.getString(leaf);
    }

    private static List<String> readRoles(ParquetRecord row, Optional<ColumnPath> column) {
        ColumnPath leaf = column.orElse(null);
        if (leaf == null) {
            return List.of();
        }
        List<String> roles = new ArrayList<>();
        for (Object role : row.multiValue(leaf)) {
            roles.add(utf8(role));
        }
        return roles;
    }

    /** The repeated string leaf hands each element out as its raw bytes. */
    private static String utf8(Object value) {
        MemorySegment bytes = (MemorySegment) value;
        return new String(bytes.toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
    }

    // --- collection assembly ---

    /** The collections the rows describe, in the order the rows name them, then those only the metadata declares. */
    private static List<StacCollection> assemble(
            SequencedMap<String, List<StacItem>> byCollection, SequencedMap<String, EmbeddedCollection> declared) {
        List<StacCollection> collections = new ArrayList<>();
        for (Map.Entry<String, List<StacItem>> group : byCollection.entrySet()) {
            collections.add(toCollection(group.getKey(), group.getValue(), declared.get(group.getKey())));
        }
        for (EmbeddedCollection empty : declared.values()) {
            if (!byCollection.containsKey(empty.id())) {
                collections.add(new StacCollection(empty.id(), empty.title(), empty.extent(), List.of(), List::of));
            }
        }
        return List.copyOf(collections);
    }

    private static StacCollection toCollection(String id, List<StacItem> items, EmbeddedCollection declared) {
        List<StacItem> owned = List.copyOf(items);
        if (declared == null) {
            return new StacCollection(id, null, derivedExtent(owned), List.of(), () -> owned);
        }
        return new StacCollection(id, declared.title(), declared.extent(), List.of(), () -> owned);
    }

    /** The extent an item-table implies when its metadata declares none: the union of the group's item bboxes. */
    private static Optional<StacExtent> derivedExtent(List<StacItem> items) {
        if (items.isEmpty()) {
            return Optional.empty();
        }
        double[] union = {
            Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY
        };
        for (StacItem item : items) {
            expand(union, item.bbox());
        }
        return Optional.of(new StacExtent(Optional.of(union), Optional.empty()));
    }

    private static void expand(double[] union, double[] bbox) {
        union[0] = Math.min(union[0], bbox[0]);
        union[1] = Math.min(union[1], bbox[1]);
        union[2] = Math.max(union[2], bbox[2]);
        union[3] = Math.max(union[3], bbox[3]);
    }

    /**
     * The collection id rows that name none belong to: the single collection the metadata declares, when it declares
     * exactly one, else the item-table's file name without its extension.
     */
    private static String fallbackCollectionId(SequencedMap<String, EmbeddedCollection> declared, String key) {
        if (declared.size() == 1) {
            return declared.firstEntry().getKey();
        }
        return fileStem(key);
    }

    private static String fileStem(String key) {
        if (key.endsWith(PARQUET_SUFFIX)) {
            return key.substring(0, key.length() - PARQUET_SUFFIX.length());
        }
        return key;
    }

    private static String storageKey(URI itemTableUri) {
        String path = itemTableUri.getPath();
        int lastSlash = path.lastIndexOf('/');
        return lastSlash < 0 ? path : path.substring(lastSlash + 1);
    }
}
