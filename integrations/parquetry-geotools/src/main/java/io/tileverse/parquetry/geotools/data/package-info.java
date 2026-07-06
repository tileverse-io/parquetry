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
 * Shared catalog-backed store core for the GeoTools DataStore family.
 *
 * <p>{@link io.tileverse.parquetry.geotools.data.CatalogDataStore} maps each dataset in a parquetry
 * {@code DatasetCatalog} to one feature type and delegates schema, count, bounds, and record reading to
 * {@link io.tileverse.parquetry.geotools.data.CatalogFeatureSource}. The base store validates nothing; per-backend
 * stores (GeoParquet, STAC, and future catalog-backed stores such as Iceberg) subclass it and add their own dataset
 * checks. A single dataset reads as one feature type - a single file, or a directory/glob of same-schema files merged
 * into one layer; Hive {@code key=value} path segments are a physical-column pruning aid, not a feature-type
 * discriminator. This differs from GeoTools' DuckDB-based GeoParquet community module, which uses Hive partitions as
 * feature-type discriminators.
 */
package io.tileverse.parquetry.geotools.data;
