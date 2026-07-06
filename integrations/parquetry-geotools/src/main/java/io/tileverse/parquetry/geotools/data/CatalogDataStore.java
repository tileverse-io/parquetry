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
package io.tileverse.parquetry.geotools.data;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import org.geotools.api.feature.type.Name;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.feature.NameImpl;

import io.tileverse.parquetry.catalog.DatasetCatalog;

/**
 * Read-only GeoTools DataStore over a parquetry {@link DatasetCatalog}; subclasses decide dataset validation.
 *
 * <p>Each dataset name in the catalog appears as a type name. The store delegates schema, count, and bounds resolution
 * to {@link CatalogFeatureSource}, which reads GeoParquet metadata only when the dataset provides it. The base store
 * accepts any {@link io.tileverse.parquetry.dataset.ParquetDataset}; a per-backend subclass adds whatever eager
 * validation it needs in its constructor.
 *
 * <p>Implements {@link AutoCloseable} so callers can use try-with-resources; {@link #close()} delegates to
 * {@link #dispose()}, which also closes the underlying catalog.
 */
public abstract class CatalogDataStore extends ContentDataStore implements AutoCloseable {

    private final DatasetCatalog catalog;

    /** Name of the column to use as the feature id, or null to auto-detect a column named {@code "id"}. */
    private volatile String fidColumn;

    /** Creates a store backed by the given catalog. The store takes ownership of the catalog. */
    protected CatalogDataStore(DatasetCatalog catalog) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
    }

    /** Sets the column to use as the feature id; null restores auto-detection of a column named {@code "id"}. */
    public void setFidColumn(String fidColumn) {
        this.fidColumn = fidColumn;
    }

    /** The configured feature id column name, or null when none is set. */
    public String fidColumn() {
        return fidColumn;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        return catalog.datasets().stream()
                .map(name -> (Name) new NameImpl(getNamespaceURI(), name))
                .toList();
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new CatalogFeatureSource(entry, catalog);
    }

    /** Returns the catalog backing this store. */
    public DatasetCatalog catalog() {
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
