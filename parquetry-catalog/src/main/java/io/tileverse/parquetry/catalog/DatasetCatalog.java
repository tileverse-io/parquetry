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
package io.tileverse.parquetry.catalog;

import java.util.List;

import io.tileverse.parquetry.dataset.ParquetDataset;

/**
 * Entry point to a set of datasets behind one connection. A pure-parquet catalog resolves 1..N datasets from a file
 * listing; an Iceberg catalog exposes the catalog's tables. Built once and reused; {@link #close()} releases backend
 * resources (open byte sources, an underlying tileverse {@code Storage}).
 */
public interface DatasetCatalog extends AutoCloseable {

    CatalogCapabilities capabilities();

    /** Names of the datasets available through this catalog, in stable order. */
    List<String> datasets();

    /**
     * Returns the dataset with the given name.
     *
     * @throws IllegalArgumentException if no dataset with that name exists
     */
    ParquetDataset dataset(String name);

    @Override
    void close();
}
