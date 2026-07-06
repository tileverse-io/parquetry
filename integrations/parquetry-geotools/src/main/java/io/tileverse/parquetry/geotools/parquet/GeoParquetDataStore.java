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

import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.geotools.data.CatalogDataStore;

/**
 * Read-only DataStore over GeoParquet datasets. Rejects a catalog with a non-GeoParquet dataset at construction rather
 * than at first query; fileset catalogs pre-materialize their datasets, making the check cheap.
 */
public final class GeoParquetDataStore extends CatalogDataStore {

    public GeoParquetDataStore(DatasetCatalog catalog) {
        super(catalog);
        ParquetDatasets.requireGeoParquet(catalog);
    }
}
