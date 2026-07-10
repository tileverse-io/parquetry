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
import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;

import io.tileverse.parquetry.data.ReadOptions;
import io.tileverse.parquetry.dataset.ParquetSource;
import io.tileverse.parquetry.filter.Predicate;
import io.tileverse.parquetry.filter.Projection;
import io.tileverse.parquetry.record.ParquetRecord;
import io.tileverse.parquetry.schema.ColumnPath;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCatalogReader;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacFormatException;
import io.tileverse.stac.StacItem;

/**
 * A {@link StacCatalogReader} over a stac-geoparquet item-table used as a catalog index: each row is a STAC item whose
 * data asset points at an external GeoParquet part, the direct equivalent of a static JSON catalog. This reads the
 * item-table through parquetry and maps each row to a {@link StacItem}, producing the same model the JSON reader
 * produces. An item-table whose rows are feature data (not an index of external parts) is out of scope.
 *
 * <p>The minimal item-table columns are {@code item_id}, {@code collection}, the flat bbox columns
 * {@code bbox_xmin/bbox_ymin/bbox_xmax/bbox_ymax}, and {@code asset_href}. The nested bbox struct and assets map of the
 * full spec are a planned extension.
 */
public final class GeoParquetStacReader implements StacCatalogReader {

    private static final ColumnPath ITEM_ID = ColumnPath.of("item_id");
    private static final ColumnPath COLLECTION = ColumnPath.of("collection");
    private static final ColumnPath ASSET_HREF = ColumnPath.of("asset_href");
    private static final ColumnPath BBOX_XMIN = ColumnPath.of("bbox_xmin");
    private static final ColumnPath BBOX_YMIN = ColumnPath.of("bbox_ymin");
    private static final ColumnPath BBOX_XMAX = ColumnPath.of("bbox_xmax");
    private static final ColumnPath BBOX_YMAX = ColumnPath.of("bbox_ymax");

    @Override
    public StacCatalog open(URI itemTableUri, Storage storage) {
        Objects.requireNonNull(itemTableUri, "itemTableUri");
        Objects.requireNonNull(storage, "storage");
        String key = storageKey(itemTableUri);
        URI container = itemTableUri.resolve(".");
        Map<String, List<StacItem>> byCollection = readItems(storage, key, container);
        List<StacCollection> collections = toCollections(byCollection);
        return new StacCatalog("stac-geoparquet", null, List.of(), () -> collections, List::of);
    }

    @SuppressWarnings("MustBeClosedChecker") // the dataset borrows the RangeReader closed by the try-with-resources
    private Map<String, List<StacItem>> readItems(Storage storage, String key, URI container) {
        Map<String, List<StacItem>> byCollection = new LinkedHashMap<>();
        try (RangeReader reader = storage.openRangeReader(key)) {
            ParquetSource source = ParquetSource.open(ByteRangeSources.from(reader));
            try (Stream<ParquetRecord> rows =
                    source.read(Predicate.ALWAYS_TRUE, Projection.ALL, ReadOptions.DEFAULTS)) {
                rows.forEach(row -> addItem(byCollection, row, container));
            }
        } catch (IOException failure) {
            throw new UncheckedIOException("reading STAC item-table " + key, failure);
        }
        return byCollection;
    }

    private void addItem(Map<String, List<StacItem>> byCollection, ParquetRecord row, URI container) {
        String id = row.getString(ITEM_ID);
        String collection = row.getString(COLLECTION);
        double[] bbox = {
            row.getDouble(BBOX_XMIN), row.getDouble(BBOX_YMIN), row.getDouble(BBOX_XMAX), row.getDouble(BBOX_YMAX)
        };
        String href = resolveAssetHref(container, id, row.getString(ASSET_HREF));
        StacAsset asset = new StacAsset(href, "application/vnd.apache.parquet", "data", List.of("data"));
        StacItem item = new StacItem(id, bbox, Optional.empty(), List.of(asset), List.of());
        byCollection.computeIfAbsent(collection, key -> new ArrayList<>()).add(item);
    }

    /**
     * Resolves an item-table row's asset href against the catalog container. The href is a required item-table value; a
     * missing or malformed one is a corrupt index, not a recoverable row, and fails loud naming the item rather than
     * aborting the whole catalog open with an opaque error.
     */
    private static String resolveAssetHref(URI container, String itemId, String assetHref) {
        if (assetHref == null || assetHref.isBlank()) {
            throw new StacFormatException("STAC item '" + itemId + "' has no asset_href in the item-table");
        }
        try {
            return container.resolve(assetHref).toString();
        } catch (IllegalArgumentException malformed) {
            throw new StacFormatException(
                    "STAC item '" + itemId + "' has a malformed asset_href '" + assetHref + "'", malformed);
        }
    }

    private List<StacCollection> toCollections(Map<String, List<StacItem>> byCollection) {
        List<StacCollection> collections = new ArrayList<>();
        for (Map.Entry<String, List<StacItem>> entry : byCollection.entrySet()) {
            List<StacItem> items = entry.getValue();
            collections.add(new StacCollection(entry.getKey(), null, Optional.empty(), List.of(), () -> items));
        }
        return collections;
    }

    private static String storageKey(URI itemTableUri) {
        String path = itemTableUri.getPath();
        int lastSlash = path.lastIndexOf('/');
        return lastSlash < 0 ? path : path.substring(lastSlash + 1);
    }
}
