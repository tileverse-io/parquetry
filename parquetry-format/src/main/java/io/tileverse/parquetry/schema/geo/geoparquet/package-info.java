/*
 * Copyright (c) 2026 Tileverse.io
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
 * Typed records mirroring the GeoParquet {@code "geo"} key-value metadata. Three concrete shapes
 * ({@link io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata.V1_0},
 * {@link io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata.V1_1},
 * {@link io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata.V2}) cover the live GeoParquet spec versions
 * 1.0, 1.1 (and 1.1+p1), and 2.0-dev / 2.0.
 *
 * <h2>Forward compatibility</h2>
 *
 * <p>Unknown {@code version} strings fall back to
 * {@link io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetMetadata.V2} because the GeoParquet spec's
 * compatibility direction is additive: new optional fields are tolerated. A future GeoParquet 2.1 file is parsed as a
 * {@code V2} record, with new fields silently ignored via {@code @JsonIgnoreProperties(ignoreUnknown = true)}.
 *
 * <h2>Module registration</h2>
 *
 * <p>{@link io.tileverse.parquetry.schema.geo.geoparquet.GeoParquetModule} is not auto-registered via
 * {@code META-INF/services}. Register it by hand on the mappers parquetry owns; the module activates the
 * {@link io.tileverse.parquetry.schema.geo.projjson.ProjJsonModule} transitively so the typed CRS surface is available
 * wherever GeoParquet metadata is parsed.
 */
package io.tileverse.parquetry.schema.geo.geoparquet;
