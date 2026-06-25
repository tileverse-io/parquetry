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
 * A backend-neutral model of a SpatioTemporal Asset Catalog (STAC) and the reader SPI over it.
 *
 * <p>This package has no dependency on any {@code io.tileverse.parquetry.*} type. It models a STAC catalog, its
 * collections, items, assets, and extents as records, and defines {@code StacCatalogReader} as the single entry point
 * for enumerating that model over tileverse-storage. The Parquet binding that reads a collection's GeoParquet parts
 * lives in the separate {@code io.tileverse.parquetry.stac} package.
 */
package io.tileverse.stac;
