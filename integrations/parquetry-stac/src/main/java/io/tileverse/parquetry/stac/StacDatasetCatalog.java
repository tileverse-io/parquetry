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

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SequencedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import io.tileverse.storage.Storage;

import io.tileverse.parquetry.catalog.CatalogCapabilities;
import io.tileverse.parquetry.catalog.CatalogCapabilities.SchemaSource;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.dataset.OpenOptions;
import io.tileverse.parquetry.dataset.ParquetDataset;

import io.tileverse.stac.StacAsset;
import io.tileverse.stac.StacCatalog;
import io.tileverse.stac.StacCatalogReader;
import io.tileverse.stac.StacCollection;
import io.tileverse.stac.StacItem;

/**
 * A {@link DatasetCatalog} over a STAC catalog: it enumerates the catalog's collections and exposes one
 * {@link StacDataset} per collection, named by the collection id verbatim. Opening walks catalog and collection
 * documents alone, stopping at each collection: a collection's item documents are read, and its dataset built, only
 * when {@link #dataset(String)} first resolves it. A catalog document naming a {@code latest} child (the Overture
 * releases-catalog property) restricts the walk to that child; the other releases are never visited.
 *
 * <p>Each collection's items resolve to GeoParquet parts (one part per item), but the catalog opens no part at
 * resolution: a part's byte reader opens on the dataset's first read. Each asset is resolved through a per-container
 * {@link ContainerStorages Storage} keyed on the asset's own absolute URI, which lets assets sit on a different host
 * than the catalog. {@link #close()} releases each resolved dataset's opened readers, then the Storage registry.
 */
public final class StacDatasetCatalog implements DatasetCatalog {

    private static final System.Logger LOGGER = System.getLogger(StacDatasetCatalog.class.getName());

    private final List<String> datasetNames;
    private final Map<String, StacCollection> pendingByName;
    private final ConcurrentMap<String, StacDataset> datasetsByName = new ConcurrentHashMap<>();
    private final ContainerStorages storages;
    private final StacCatalogOptions options;

    private StacDatasetCatalog(
            SequencedMap<String, StacCollection> pendingByName,
            ContainerStorages storages,
            StacCatalogOptions options) {
        this.datasetNames = List.copyOf(pendingByName.keySet());
        this.pendingByName = Map.copyOf(pendingByName);
        this.storages = storages;
        this.options = options;
    }

    /** Opens a STAC catalog through {@code reader}, taking ownership of {@code storages}. */
    public static StacDatasetCatalog open(
            URI catalogRoot, ContainerStorages storages, StacCatalogReader reader, StacCatalogOptions options) {
        Objects.requireNonNull(catalogRoot, "catalogRoot");
        Objects.requireNonNull(storages, "storages");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(options, "options");

        try {
            Storage catalogStorage = storages.storageFor(catalogRoot.resolve("."));
            StacCatalog root = reader.open(catalogRoot, catalogStorage);
            SequencedMap<String, StacCollection> byId = new LinkedHashMap<>();
            collectCollections(root, byId);
            return new StacDatasetCatalog(byId, storages, options);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(Map.of(), storages);
            if (cleanup != null) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    /**
     * Walks the catalog tree gathering collections by id, first occurrence winning. A duplicate id is logged and
     * dropped: a releases catalog carries the same collection ids under every release, and without a {@code latest}
     * restriction the walk would otherwise silently replace one release's dataset with another's.
     */
    private static void collectCollections(StacCatalog catalog, SequencedMap<String, StacCollection> into) {
        for (StacCollection collection : catalog.collections()) {
            StacCollection existing = into.putIfAbsent(collection.id(), collection);
            if (existing != null) {
                LOGGER.log(
                        System.Logger.Level.WARNING,
                        "dropping duplicate STAC collection id ''{0}'': the first occurrence in walk order is kept",
                        collection.id());
            }
        }
        for (StacCatalog child : childrenToWalk(catalog)) {
            collectCollections(child, into);
        }
    }

    /**
     * The child catalogs the walk descends into: the one named by the catalog's {@code latest} property when present
     * and resolvable, every child otherwise. A {@code latest} naming no child is logged and ignored rather than hiding
     * the whole tree behind a stale pointer.
     */
    private static List<StacCatalog> childrenToWalk(StacCatalog catalog) {
        String latest = catalog.latest();
        List<StacCatalog> children = catalog.childCatalogs();
        if (latest == null) {
            return children;
        }
        for (StacCatalog child : children) {
            if (latest.equals(child.id())) {
                return List.of(child);
            }
        }
        LOGGER.log(
                System.Logger.Level.WARNING,
                "catalog ''{0}'' names latest child ''{1}'' but no child catalog has that id; walking every child",
                catalog.id(),
                latest);
        return children;
    }

    /**
     * Builds the dataset for one collection by reading its item documents. Fails when the collection resolves to no
     * GeoParquet data parts (its items hold only non-parquet assets, such as PMTiles or COGs, or it has no items): this
     * store serves GeoParquet, and a collection with none has no rows to offer. No part is opened here: the dataset
     * resolves each item's href to a byte reader lazily on first read.
     */
    private StacDataset buildDataset(StacCollection collection) {
        List<StacItemRef> refs = new ArrayList<>();
        List<double[]> bboxes = new ArrayList<>();
        for (StacItem item : collection.items()) {
            StacAsset data = parquetAsset(item, options);
            if (data == null) {
                continue;
            }
            refs.add(new StacItemRef(item.id(), data.href()));
            bboxes.add(item.bbox());
        }
        if (refs.isEmpty()) {
            throw new IllegalStateException(
                    "collection '" + collection.id() + "' resolves to no GeoParquet data assets");
        }
        return new StacDataset(collection.id(), options.geometryColumn(), refs, bboxes, storages, OpenOptions.DEFAULTS);
    }

    private static StacAsset parquetAsset(StacItem item, StacCatalogOptions options) {
        for (StacAsset asset : item.assets()) {
            if (options.isParquet(asset.type(), asset.href())) {
                return asset;
            }
        }
        return null;
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
        return datasetNames;
    }

    @Override
    public ParquetDataset dataset(String name) {
        StacDataset dataset = datasetsByName.get(name);
        if (dataset != null) {
            return dataset;
        }
        StacCollection pending = pendingByName.get(name);
        if (pending == null) {
            throw new IllegalArgumentException("no dataset named '" + name + "' (have " + datasets() + ")");
        }
        return datasetsByName.computeIfAbsent(name, unused -> buildDataset(pending));
    }

    @Override
    public void close() {
        RuntimeException failure = closeAll(datasetsByName, storages);
        if (failure != null) {
            throw failure;
        }
    }

    private static RuntimeException closeAll(Map<String, StacDataset> datasets, ContainerStorages storages) {
        RuntimeException failure = null;
        for (StacDataset dataset : datasets.values()) {
            failure = closeChaining(dataset::closeResources, failure);
        }
        return closeChaining(storages, failure);
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
