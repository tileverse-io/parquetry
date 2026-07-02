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
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;
import org.geotools.api.data.Parameter;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.tileverse.ParquetFileSources;

/** Opens a read-only GeoParquet {@link DataStore} from a dataset URI. */
public final class GeoParquetDataStoreFactory implements DataStoreFactorySpi {

    // LEVEL "program" hides this fixed discriminator from the GeoServer store UI, mirroring how JDBC stores hide
    // their "dbtype". The value is supplied from the default and never edited by the user.
    public static final Param FILETYPE = new Param(
            "filetype", String.class, "Must be 'geoparquet'", true, "geoparquet", Map.of(Parameter.LEVEL, "program"));
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

        Properties storageProps = StorageParams.toProperties(params);
        FileSource source = openSource(datasetUri, storageProps);
        FilesetCatalog catalog = FilesetCatalog.open(source, CatalogOptions.defaults());

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
     * Opens the file source for the store's URI. A single-file URI opens the object directly by key (a GET, with no
     * directory listing), which lets a remote store work over HTTP and with a GET-only credential; a directory or glob
     * URI lists its container for the files to merge.
     */
    private static FileSource openSource(URI datasetUri, Properties storageProps) {
        if (isSingleFileUri(datasetUri)) {
            return ParquetFileSources.openObject(datasetUri, storageProps);
        }
        URI base = baseContainer(datasetUri);
        String pattern = filePattern(datasetUri);
        return ParquetFileSources.open(base, pattern, storageProps);
    }

    /**
     * A URI whose last path segment is a {@code .parquet} file name (a single file, or a {@code *.parquet} glob) roots
     * the Storage at the parent directory. A directory URI roots there directly.
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
     * <p>A directory URI matches every {@code .parquet} at any depth (recursive, the Ant/DuckDB convention), letting a
     * flat directory of part files and a Hive-partitioned tree both resolve as one dataset. The pattern is
     * {@code **}{@code /*.parquet}: {@code *.parquet} alone would match only the top level (one segment), while the
     * JDK-misleading {@code **}{@code /} requires at least one directory; {@code **}{@code /*.parquet} matches the
     * top-level file and the nested one alike.
     *
     * <p>For a single-file URI, the file name is wrapped in brace-expansion syntax (e.g. {@code {example.parquet}}) to
     * make the Storage backend's pattern parser treat it as a glob rather than as a directory prefix, which would cause
     * it to look for a subdirectory named after the file.
     */
    private static String filePattern(URI uri) {
        String path = uri.getPath();
        boolean isFile = path != null && path.toLowerCase().endsWith(".parquet");
        if (!isFile) {
            return "**/*.parquet";
        }
        String fileName = path.substring(path.lastIndexOf('/') + 1);
        return "{" + fileName + "}";
    }

    /**
     * Whether the URI names a single object to open directly, rather than a container to list. True when the path ends
     * in {@code .parquet} and holds no glob metacharacter - a plain file name like {@code .../place.parquet}. A glob
     * such as {@code .../*.parquet}, a recursive {@code .../}{@code **}{@code /*.parquet}, or a directory is listed
     * instead.
     */
    static boolean isSingleFileUri(URI uri) {
        String path = uri.getPath();
        if (path == null) {
            return false;
        }
        return path.toLowerCase(Locale.ROOT).endsWith(".parquet") && !hasGlobCharacters(path);
    }

    private static boolean hasGlobCharacters(String path) {
        for (int index = 0; index < path.length(); index++) {
            if ("*?{}[]".indexOf(path.charAt(index)) >= 0) {
                return true;
            }
        }
        return false;
    }
}
