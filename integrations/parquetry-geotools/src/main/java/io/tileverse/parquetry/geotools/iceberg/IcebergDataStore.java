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
package io.tileverse.parquetry.geotools.iceberg;

import io.tileverse.parquetry.catalog.DatasetCatalog;
import io.tileverse.parquetry.geotools.data.CatalogDataStore;

/**
 * Read-only DataStore over Apache Iceberg tables read natively by parquetry. No dataset validation at construction: a
 * warehouse catalog opens tables lazily on first access, and Iceberg datasets present geometry through the native
 * GEOMETRY/GEOGRAPHY logical types rather than GeoParquet metadata. Snapshot selection params are deliberately absent
 * (sequenced after the REST catalog); the future REST-catalog-backed store is a separate implementation.
 */
public final class IcebergDataStore extends CatalogDataStore {

    public IcebergDataStore(DatasetCatalog catalog) {
        super(catalog);
    }
}
