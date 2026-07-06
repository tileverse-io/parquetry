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
package io.tileverse.parquetry.stac;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.tileverse.storage.RangeReader;
import io.tileverse.storage.Storage;

import io.tileverse.parquetry.catalog.CatalogCapabilities;
import io.tileverse.parquetry.catalog.CatalogCapabilities.SchemaSource;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;
import io.tileverse.parquetry.io.ByteRangeSource;
import io.tileverse.parquetry.tileverse.ByteRangeSources;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCatalogReader;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacItem;

/**
 * A {@link DatasetCatalog} over a STAC catalog: it enumerates the catalog's collections and exposes one
 * {@link StacDataset} per collection, named by the collection id verbatim. Each collection's items resolve to
 * GeoParquet parts (one part per item); the catalog opens each part's byte source once and owns it for its lifetime. A
 * per-query dataset borrows the survivor subset and never closes the shared sources. {@link #close()} releases every
 * source, every underlying reader, and the Storage.
 */
public final class StacDatasetCatalog implements DatasetCatalog {

    private final Map<String, StacDataset> datasets;
    private final List<ByteRangeSource> openSources;
    private final List<RangeReader> openReaders;
    private final Storage storage;

    private StacDatasetCatalog(
            Map<String, StacDataset> datasets,
            List<ByteRangeSource> openSources,
            List<RangeReader> openReaders,
            Storage storage) {
        this.datasets = datasets;
        this.openSources = openSources;
        this.openReaders = openReaders;
        this.storage = storage;
    }

    /** Opens a STAC catalog through {@code reader}, taking ownership of {@code storage}. */
    public static StacDatasetCatalog open(
            URI catalogRoot, Storage storage, StacCatalogReader reader, StacCatalogOptions options) {
        Objects.requireNonNull(catalogRoot, "catalogRoot");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(options, "options");

        List<ByteRangeSource> openedSources = new ArrayList<>();
        List<RangeReader> openedReaders = new ArrayList<>();
        try {
            StacCatalog root = reader.open(catalogRoot, storage);
            List<StacCollection> collections = allCollections(root);
            Map<String, StacDataset> built =
                    buildDatasets(catalogRoot, collections, storage, options, openedSources, openedReaders);
            return new StacDatasetCatalog(built, openedSources, openedReaders, storage);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(openedSources, openedReaders, storage);
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static List<StacCollection> allCollections(StacCatalog root) {
        List<StacCollection> collections = new ArrayList<>(root.collections());
        for (StacCatalog child : root.childCatalogs()) {
            collections.addAll(allCollections(child));
        }
        return collections;
    }

    private static Map<String, StacDataset> buildDatasets(
            URI catalogRoot,
            List<StacCollection> collections,
            Storage storage,
            StacCatalogOptions options,
            List<ByteRangeSource> openedSources,
            List<RangeReader> openedReaders) {
        Map<String, StacDataset> built = new LinkedHashMap<>();
        for (StacCollection collection : collections) {
            Optional<StacDataset> dataset =
                    buildDataset(catalogRoot, collection, storage, options, openedSources, openedReaders);
            dataset.ifPresent(present -> built.put(collection.id(), present));
        }
        return built;
    }

    /**
     * Builds the dataset for one collection, or {@link Optional#empty()} when the collection resolves to no GeoParquet
     * data parts (its items hold only non-parquet assets, such as PMTiles or COGs, or it has no items). Such a
     * collection is simply not exposed as a dataset; it opens zero sources and never reaches {@link StacDataset}'s
     * empty-parts guard.
     */
    private static Optional<StacDataset> buildDataset(
            URI catalogRoot,
            StacCollection collection,
            Storage storage,
            StacCatalogOptions options,
            List<ByteRangeSource> openedSources,
            List<RangeReader> openedReaders) {
        List<StacItemRef> refs = new ArrayList<>();
        List<double[]> bboxes = new ArrayList<>();
        List<ByteRangeSource> collectionSources = new ArrayList<>();
        for (StacItem item : collection.items()) {
            StacAsset data = parquetAsset(item, options);
            if (data == null) {
                continue;
            }
            String key = storageKey(catalogRoot, data.href());
            RangeReader reader = storage.openRangeReader(key);
            openedReaders.add(reader);
            ByteRangeSource source = ByteRangeSources.from(reader);
            openedSources.add(source);
            collectionSources.add(source);
            refs.add(new StacItemRef(item.id(), data.href()));
            bboxes.add(item.bbox());
        }
        if (collectionSources.isEmpty()) {
            return Optional.empty();
        }
        StacDataset dataset = new StacDataset(
                collection.id(),
                options.geometryColumn(),
                refs,
                bboxes,
                List.copyOf(collectionSources),
                OpenOptions.DEFAULTS);
        return Optional.of(dataset);
    }

    private static StacAsset parquetAsset(StacItem item, StacCatalogOptions options) {
        for (StacAsset asset : item.assets()) {
            if (options.isParquet(asset.type(), asset.href())) {
                return asset;
            }
        }
        return null;
    }

    /** The storage key for an absolute asset URI: its path relative to the catalog root's container. */
    private static String storageKey(URI catalogRoot, String href) {
        URI container = catalogRoot.resolve(".");
        URI hrefUri = URI.create(href);
        String relative = container.relativize(hrefUri).getPath();
        return relative.startsWith("/") ? relative.substring(1) : relative;
    }

    @Override
    public CatalogCapabilities capabilities() {
        return CatalogCapabilities.builder()
                .enumeratesDatasets(true)
                .timeTravel(false)
                .schemaSource(SchemaSource.COLLECTION)
                .build();
    }

    @Override
    public List<String> datasets() {
        return List.copyOf(datasets.keySet());
    }

    @Override
    public ParquetDataset dataset(String name) {
        StacDataset dataset = datasets.get(name);
        if (dataset == null) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have " + datasets.keySet() + ")");
        }
        return dataset;
    }

    @Override
    public void close() {
        RuntimeException failure = closeAll(openSources, openReaders, storage);
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeAll(
            List<ByteRangeSource> sources, List<RangeReader> readers, Storage storage) {
        RuntimeException failure = null;
        for (ByteRangeSource source : sources) {
            failure = closeChaining(source, failure);
        }
        for (RangeReader reader : readers) {
            failure = closeChaining(reader, failure);
        }
        return closeChaining(storage, failure);
    }

    private static RuntimeException closeChaining(AutoCloseable closeable, RuntimeException accumulated) {
        try {
            closeable.close();
            return accumulated;
        } catch (RuntimeException alreadyUnchecked) {
            return chain(accumulated, alreadyUnchecked);
        } catch (Exception checked) {
            return chain(accumulated, new IllegalStateException("closing " + closeable, checked));
        }
    }

    private static RuntimeException chain(RuntimeException accumulated, RuntimeException next) {
        if (accumulated == null) {
            return next;
        }
        accumulated.addSuppressed(next);
        return accumulated;
    }
}
