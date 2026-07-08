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
package io.tileverse.parquetry.geotools.parquet;

import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;

import io.tileverse.parquetry.geotools.data.StorageParams;
import io.tileverse.parquetry.stac.ContainerStorages;
import io.tileverse.parquetry.stac.GeoParquetStacReader;
import io.tileverse.parquetry.stac.StacCatalogOptions;
import io.tileverse.parquetry.stac.StacDatasetCatalog;

import io.tileverse.stac.JsonStacReader;
import io.tileverse.stac.StacCatalogReader;

/**
 * Opens a read-only GeoTools {@link DataStore} over a STAC catalog. Each STAC collection becomes one feature type: the
 * factory builds a {@link StacDatasetCatalog} over the catalog URI and hands it to a {@link StacDataStore}, whose
 * {@code createTypeNames()} maps every dataset to a type.
 */
public final class StacDataStoreFactory implements DataStoreFactorySpi {

    // A required URI key named after the store type. Its presence is what tells DataStoreFinder this factory, and no
    // other, can open the parameters - no separate discriminator param is needed.
    public static final Param STAC_URI = new Param(
            "geoparquet-stac",
            String.class,
            "URI of a STAC catalog.json or a stac-geoparquet item-table (*.parquet)",
            true);
    public static final Param NAMESPACE = new Param("namespace", String.class, "Feature type namespace", false);

    @Override
    public String getDisplayName() {
        return "STAC GeoParquet";
    }

    @Override
    public String getDescription() {
        return "Read-only STAC catalog of GeoParquet collections (local, S3, Azure, GCS, HTTP)";
    }

    @Override
    public Param[] getParametersInfo() {
        return StorageParams.withStorageParamsNoProvider(STAC_URI, NAMESPACE);
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        try {
            return STAC_URI.lookUp(params) != null;
        } catch (IOException cannotRead) {
            return false;
        }
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public DataStore createDataStore(Map<String, ?> params) throws IOException {
        String uriText = (String) STAC_URI.lookUp(params);
        URI catalogUri = catalogDocumentUri(parseCatalogUri(uriText));
        String namespace = (String) NAMESPACE.lookUp(params);

        Properties storageProps = StorageParams.toProperties(params);
        ContainerStorages storages = new ContainerStorages(storageProps);
        StacCatalogReader reader = readerFor(catalogUri);
        StacDatasetCatalog catalog =
                StacDatasetCatalog.open(catalogUri, storages, reader, StacCatalogOptions.defaults());

        StacDataStore store = new StacDataStore(catalog);
        if (namespace != null) {
            store.setNamespaceURI(namespace);
        }
        return store;
    }

    /**
     * Parses the store's URI parameter, reporting a malformed value as the {@link IOException} a store factory owes
     * GeoServer.
     */
    private static URI parseCatalogUri(String uriText) throws IOException {
        try {
            return URI.create(uriText);
        } catch (IllegalArgumentException malformed) {
            throw new IOException("invalid geoparquet-stac URI: " + uriText, malformed);
        }
    }

    /**
     * A container URL (empty path or a path ending in {@code /}) points at the catalog directory, not a document;
     * appending {@code catalog.json} resolves {@code https://host/} to {@code https://host/catalog.json}. A path that
     * already names a document (a {@code .json} or {@code .parquet} leaf) is returned unchanged.
     */
    private static URI catalogDocumentUri(URI uri) {
        String path = uri.getPath();
        boolean container = path == null || path.isEmpty() || path.endsWith("/");
        return container ? uri.resolve("catalog.json") : uri;
    }

    /** A {@code .parquet} URI is a stac-geoparquet item-table; anything else is a JSON catalog document. */
    private static StacCatalogReader readerFor(URI catalogUri) {
        String path = catalogUri.getPath();
        boolean itemTable = path != null && path.toLowerCase(Locale.ROOT).endsWith(".parquet");
        return itemTable ? new GeoParquetStacReader() : new JsonStacReader();
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException("STAC GeoParquet is read-only");
    }
}
