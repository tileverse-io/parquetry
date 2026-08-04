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
package io.tileverse.parquetry.geotools.parquet;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.geotools.api.data.DataAccessFactory.Param;
import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFactorySpi;
import org.geotools.api.data.Parameter;

import io.tileverse.parquetry.catalog.CatalogOptions;
import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.catalog.FilesetCatalog;
import io.tileverse.parquetry.geotools.data.StorageParams;
import io.tileverse.parquetry.io.FileSource;
import io.tileverse.parquetry.tileverse.ParquetFileSources;

/** Opens a read-only GeoParquet {@link DataStore} from a dataset URI. */
public final class GeoParquetDataStoreFactory implements DataStoreFactorySpi {

    // A required URI key named after the store type. Its presence is what tells DataStoreFinder this factory, and no
    // other, can open the parameters - no separate discriminator param is needed.
    public static final Param GEOPARQUET_URI =
            new Param("geoparquet", String.class, "URI of a GeoParquet file (or directory)", true);
    public static final Param NAMESPACE = new Param("namespace", String.class, "Feature type namespace", false);
    public static final Param FID = new Param(
            "fid",
            String.class,
            "Column to use as the feature id (defaults to a column named 'id' when present; otherwise feature ids are synthetic and Id filters are rejected)",
            false);
    public static final Param LAYER_GROUPING = new Param(
            "layer-grouping",
            String.class,
            "Layer grouping for a directory URI: 'merged' reads all files as one layer (files must share a schema);"
                    + " 'file' publishes each top-level .parquet file as its own layer. Absent means 'merged'.",
            false,
            null,
            // GeoServer's store edit page (ParamInfo) sorts the options list in place; it must be sortable
            Map.of(Parameter.OPTIONS, Arrays.asList("merged", "file")));

    private static final String PARQUET_EXTENSION = ".parquet";

    // Deliberately "Parquet", not "GeoParquet": the store reads any Parquet file (geo is first class but optional),
    // and GeoServer resolves a saved store's factory by display name, where the deprecated DuckDB-based geoparquet
    // module already claims "GeoParquet".
    @Override
    public String getDisplayName() {
        return "Parquet";
    }

    @Override
    public String getDescription() {
        return "Read-only Parquet and GeoParquet files and directories on local, S3, Azure, GCS, or HTTP storage"
                + " (Parquetry engine)";
    }

    @Override
    public Param[] getParametersInfo() {
        return StorageParams.withStorageParams(GEOPARQUET_URI, NAMESPACE, FID, LAYER_GROUPING);
    }

    @Override
    public boolean canProcess(Map<String, ?> params) {
        try {
            return GEOPARQUET_URI.lookUp(params) != null;
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
        String uriText = (String) GEOPARQUET_URI.lookUp(params);
        URI datasetUri = URI.create(uriText);
        String namespace = (String) NAMESPACE.lookUp(params);
        String fidColumn = (String) FID.lookUp(params);
        LayerGrouping layers = LayerGrouping.parse((String) LAYER_GROUPING.lookUp(params));

        Properties storageProps = StorageParams.toProperties(params);
        DatasetCatalog catalog = openCatalog(datasetUri, layers, storageProps);

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

    /** How a directory URI maps to layers: one merged dataset over all files, or one dataset per top-level file. */
    enum LayerGrouping {
        MERGED,
        FILE;

        /** Parses the {@code layer-grouping} parameter value; absent or blank means {@link #MERGED}. */
        static LayerGrouping parse(String value) throws IOException {
            if (value == null || value.isBlank()) {
                return MERGED;
            }
            return switch (value.trim().toLowerCase(Locale.ROOT)) {
                case "merged" -> MERGED;
                case "file" -> FILE;
                default ->
                    throw new IOException("unsupported layer-grouping value '" + value + "': use 'merged' or 'file'");
            };
        }
    }

    /**
     * Opens the catalog for the store's URI. A single-file URI opens the object directly by key (a GET, with no
     * directory listing), which lets a remote store work over HTTP and with a GET-only credential; it is one layer by
     * definition and ignores the layer grouping. A directory or glob URI lists its container: {@code merged} resolves
     * every matched file (recursively) into one dataset, {@code file} lists the top level only and publishes one
     * dataset per file.
     */
    private static DatasetCatalog openCatalog(URI datasetUri, LayerGrouping layers, Properties storageProps) {
        if (isSingleFileUri(datasetUri)) {
            FileSource object = ParquetFileSources.openObject(datasetUri, storageProps);
            return FilesetCatalog.open(object, CatalogOptions.defaults());
        }
        if (layers == LayerGrouping.FILE) {
            FileSource topLevel = ParquetFileSources.open(fileModeContainer(datasetUri), "*.parquet", storageProps);
            return FilesetCatalog.openPerFile(topLevel);
        }
        FileSource merged = ParquetFileSources.open(baseContainer(datasetUri), filePattern(datasetUri), storageProps);
        return FilesetCatalog.open(merged, CatalogOptions.defaults());
    }

    /**
     * A URI whose last path segment is a {@code .parquet} file name (a single file, or a {@code *.parquet} glob) roots
     * the Storage at the parent directory. A directory URI roots there directly.
     */
    private static URI baseContainer(URI uri) {
        String path = uri.getPath();
        boolean isFile = path != null && path.toLowerCase().endsWith(PARQUET_EXTENSION);
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
        boolean isFile = path != null && path.toLowerCase().endsWith(PARQUET_EXTENSION);
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
        return path.toLowerCase(Locale.ROOT).endsWith(PARQUET_EXTENSION) && !hasGlobCharacters(path);
    }

    /**
     * The container listed in {@code layer-grouping=file} mode: the directory itself for a directory URI, the parent
     * directory for a glob URI. Any glob tail ({@code *.parquet}, {@code **}{@code /*.parquet}) is dropped: file mode
     * always lists the top level, one dataset per file, and never recurses.
     */
    static URI fileModeContainer(URI uri) {
        String path = uri.getPath();
        if (path == null) {
            return uri;
        }
        int firstGlob = indexOfGlobCharacter(path);
        if (firstGlob < 0) {
            return baseContainer(uri);
        }
        String containerPath = path.substring(0, path.lastIndexOf('/', firstGlob) + 1);
        return uri.resolve(containerPath);
    }

    private static boolean hasGlobCharacters(String path) {
        return indexOfGlobCharacter(path) >= 0;
    }

    private static int indexOfGlobCharacter(String path) {
        for (int index = 0; index < path.length(); index++) {
            if ("*?{}[]".indexOf(path.charAt(index)) >= 0) {
                return index;
            }
        }
        return -1;
    }
}
