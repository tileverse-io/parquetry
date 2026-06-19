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
/**
 * GeoTools DataStore for GeoParquet datasets.
 *
 * <p>This DataStore reads a single GeoParquet dataset as one feature type: a single file, or a directory/glob of
 * same-schema files merged into one layer. Hive {@code key=value} path segments are a physical-column pruning aid, not
 * a feature-type discriminator. Multiple feature types come from catalog-backed DataStores (Iceberg, STAC) that reuse
 * this package's feature plumbing through the {@code DatasetCatalog} SPI. This differs from GeoTools' DuckDB-based
 * GeoParquet community module, which uses Hive partitions as feature-type discriminators.
 */
package io.tileverse.parquetry.geotools;
