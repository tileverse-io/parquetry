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

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.geotools.api.feature.type.Name;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;

import io.tileverse.parquetry.data.ParquetDatasetCatalog;

/**
 * Read-only GeoTools DataStore over a parquetry {@link ParquetDatasetCatalog}.
 *
 * <p>Each dataset name in the catalog appears as a type name. The store delegates schema, count, and bounds resolution
 * to {@link GeoParquetFeatureSource}; actual record reading is not implemented in this increment.
 *
 * <p>Implements {@link AutoCloseable} so callers can use try-with-resources; {@link #close()} delegates to
 * {@link #dispose()}, which also closes the underlying catalog.
 */
public final class GeoParquetDataStore extends ContentDataStore implements AutoCloseable {

    private final ParquetDatasetCatalog catalog;

    /** Creates a store backed by the given catalog. The store takes ownership of the catalog. */
    public GeoParquetDataStore(ParquetDatasetCatalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        return catalog.datasetNames().stream()
                .map(name -> (Name) new NameImpl(getNamespaceURI(), name))
                .toList();
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new GeoParquetFeatureSource(entry, catalog);
    }

    /** Returns the catalog backing this store. */
    ParquetDatasetCatalog catalog() {
        return catalog;
    }

    /**
     * Closes the store and its underlying catalog.
     *
     * <p>Calls {@link #dispose()}, which also closes the catalog. Both paths arrive at the same cleanup logic.
     */
    @Override
    public void close() {
        dispose();
    }

    /**
     * Disposes this store and closes the underlying catalog.
     *
     * <p>Runs the parent disposal first to invalidate cached entries, then closes the catalog regardless of whether the
     * parent disposal threw.
     */
    @Override
    public void dispose() {
        try {
            super.dispose();
        } finally {
            catalog.close();
        }
    }
}
