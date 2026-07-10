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
import java.util.Optional;

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
 * {@link StacDataset} per collection, named by the collection id verbatim. Each collection's items resolve to
 * GeoParquet parts (one part per item), but the catalog opens no part at registration: enumerating a collection reads
 * STAC metadata alone, and a part's byte reader opens on the dataset's first read. Each asset is resolved through a
 * per-container {@link ContainerStorages Storage} keyed on the asset's own absolute URI, which lets assets sit on a
 * different host than the catalog. {@link #close()} releases each dataset's opened readers, then the Storage registry.
 */
public final class StacDatasetCatalog implements DatasetCatalog {

    private final Map<String, StacDataset> datasets;
    private final ContainerStorages storages;

    private StacDatasetCatalog(Map<String, StacDataset> datasets, ContainerStorages storages) {
        this.datasets = datasets;
        this.storages = storages;
    }

    /** Opens a STAC catalog through {@code reader}, taking ownership of {@code storages}. */
    public static StacDatasetCatalog open(
            URI catalogRoot, ContainerStorages storages, StacCatalogReader reader, StacCatalogOptions options) {
        Objects.requireNonNull(catalogRoot, "catalogRoot");
        Objects.requireNonNull(storages, "storages");
        Objects.requireNonNull(reader, "reader");
        Objects.requireNonNull(options, "options");

        Map<String, StacDataset> built = new LinkedHashMap<>();
        try {
            Storage catalogStorage = storages.storageFor(catalogRoot.resolve("."));
            StacCatalog root = reader.open(catalogRoot, catalogStorage);
            List<StacCollection> collections = allCollections(root);
            buildDatasets(built, collections, storages, options);
            return new StacDatasetCatalog(built, storages);
        } catch (RuntimeException failure) {
            RuntimeException cleanup = closeAll(built, storages);
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

    private static void buildDatasets(
            Map<String, StacDataset> into,
            List<StacCollection> collections,
            ContainerStorages storages,
            StacCatalogOptions options) {
        for (StacCollection collection : collections) {
            Optional<StacDataset> dataset = buildDataset(collection, storages, options);
            dataset.ifPresent(present -> into.put(collection.id(), present));
        }
    }

    /**
     * Builds the dataset for one collection, or {@link Optional#empty()} when the collection resolves to no GeoParquet
     * data parts (its items hold only non-parquet assets, such as PMTiles or COGs, or it has no items). Such a
     * collection is simply not exposed as a dataset; it never reaches {@link StacDataset}'s empty-parts guard. No part
     * is opened here: the dataset resolves each item's href to a byte reader lazily on first read.
     */
    private static Optional<StacDataset> buildDataset(
            StacCollection collection, ContainerStorages storages, StacCatalogOptions options) {
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
            return Optional.empty();
        }
        StacDataset dataset = new StacDataset(
                collection.id(), options.geometryColumn(), refs, bboxes, storages, OpenOptions.DEFAULTS);
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
        RuntimeException failure = closeAll(datasets, storages);
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
