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
import java.net.URI;
import java.util.Map;
import java.util.Properties;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.ParquetDatasetCatalog;
import io.tileverse.parquetry.tileverse.StorageFileSource;

/** Opens a read-only GeoParquet {@link DataStore} from a dataset URI. */
public final class GeoParquetDataStoreFactory implements DataStoreFactorySpi {

    // LEVEL "program" hides this fixed discriminator from the GeoServer store UI, mirroring how JDBC stores hide
    // their "dbtype". The value is supplied from the default and never edited by the user.
    public static final Param FILETYPE = new Param(
            "filetype", String.class, "Must be 'geoparquet'", true, "geoparquet", Map.of(Param.LEVEL, "program"));
    public static final Param URIP = new Param("uri", String.class, "URI of a GeoParquet file (or directory)", true);
    public static final Param NAMESPACE = new Param("namespace", String.class, "Feature type namespace", false);
    public static final Param FID = new Param(
            "fid",
            String.class,
            "Column to use as the feature id (defaults to a column named 'id' when present; otherwise feature ids are synthetic and Id filters are rejected)",
            false);

    @Override
    public String getDisplayName() {
        return "GeoParquet";
    }

    @Override
    public String getDescription() {
        return "Read-only GeoParquet (local, S3, Azure, GCS, HTTP)";
    }

    @Override
    public Param[] getParametersInfo() {
        return StorageParams.withStorageParams(FILETYPE, URIP, NAMESPACE, FID);
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        try {
            return "geoparquet".equals(FILETYPE.lookUp(params)) && URIP.lookUp(params) != null;
        } catch (IOException e) {
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
        URI datasetUri = URI.create(uriText);
        String namespace = (String) NAMESPACE.lookUp(params);
        String fidColumn = (String) FID.lookUp(params);

        URI base = baseContainer(datasetUri);
        String pattern = filePattern(datasetUri);
        Properties storageProps = StorageParams.toProperties(params);

        StorageFileSource source = StorageFileSource.open(base, pattern, storageProps);
        ParquetDatasetCatalog catalog = ParquetDatasetCatalog.open(source, CatalogOptions.defaults());

        GeoParquetDataStore store = new GeoParquetDataStore(catalog);
        if (namespace != null) {
            store.setNamespaceURI(namespace);
        }
        if (fidColumn != null) {
            store.setFidColumn(fidColumn);
        }
        return store;
    }

    @Override
    public DataStore createNewDataStore(Map<String, ?> params) throws IOException {
        throw new UnsupportedOperationException("GeoParquet is read-only");
    }

    /**
     * A single-file URI is opened by rooting the Storage at the parent directory. A directory URI roots there directly.
     */
    private static URI baseContainer(URI uri) {
        String path = uri.getPath();
        boolean isFile = path != null && path.toLowerCase().endsWith(".parquet");
        if (!isFile) {
            return uri;
        }
        return uri.resolve(".");
    }

    /**
     * Returns the glob pattern to use when listing files under the base container.
     *
     * <p>For a directory URI, the pattern is {@code *.parquet}. For a single-file URI, the file name is wrapped in
     * brace-expansion syntax (e.g. {@code {example.parquet}}) so that the Storage backend's pattern parser treats it as
     * a glob rather than as a directory prefix, which would cause it to look for a subdirectory named after the file.
     */
    private static String filePattern(URI uri) {
        String path = uri.getPath();
        boolean isFile = path != null && path.toLowerCase().endsWith(".parquet");
        if (!isFile) {
            return "*.parquet";
        }
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        return "{" + fileName + "}";
    }
}
