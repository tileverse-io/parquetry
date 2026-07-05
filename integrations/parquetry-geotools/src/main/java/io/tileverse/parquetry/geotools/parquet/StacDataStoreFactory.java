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

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;
import org.geotools.api.data.Parameter;

import io.tileverse.storage.Storage;
import io.tileverse.storage.StorageFactory;

import io.tileverse.parquetry.geotools.data.CatalogDataStore;
import io.tileverse.parquetry.geotools.data.StorageParams;
import io.tileverse.parquetry.stac.StacCatalogOptions;
import io.tileverse.parquetry.stac.StacDatasetCatalog;

import io.tileverse.stac.JsonStacReader;

/**
 * Opens a read-only GeoTools {@link DataStore} over a STAC catalog. Each STAC collection becomes one feature type: the
 * factory builds a {@link StacDatasetCatalog} over the catalog URI and hands it to the existing
 * {@link CatalogDataStore}, whose {@code createTypeNames()} maps every dataset to a type.
 */
public final class StacDataStoreFactory implements DataStoreFactorySpi {

    // LEVEL "program" hides this fixed discriminator from the GeoServer store UI, mirroring how the GeoParquet store
    // hides its "filetype". The value is supplied from the default and never edited by the user.
    public static final Param FILETYPE =
            new Param("filetype", String.class, "Must be 'stac'", true, "stac", Map.of(Parameter.LEVEL, "program"));
    public static final Param URIP = new Param("uri", String.class, "URI of a STAC catalog.json", true);
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
        return StorageParams.withStorageParams(FILETYPE, URIP, NAMESPACE);
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        try {
            return "stac".equals(FILETYPE.lookUp(params)) && URIP.lookUp(params) != null;
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
        String uriText = (String) URIP.lookUp(params);
        URI catalogUri = URI.create(uriText);
        String namespace = (String) NAMESPACE.lookUp(params);

        URI container = catalogUri.resolve(".");
        Storage storage = StorageFactory.open(container);
        StacDatasetCatalog catalog =
                StacDatasetCatalog.open(catalogUri, storage, new JsonStacReader(), StacCatalogOptions.defaults());

        CatalogDataStore store = new CatalogDataStore(catalog);
        if (namespace != null) {
            store.setNamespaceURI(namespace);
        }
        return store;
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException("STAC GeoParquet is read-only");
    }
}
