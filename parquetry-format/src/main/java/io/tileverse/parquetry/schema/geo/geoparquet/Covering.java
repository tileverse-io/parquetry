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
package io.tileverse.parquetry.schema.geo.geoparquet;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * GeoParquet 1.1+ {@code covering} metadata: a sidecar group of regular numeric columns that mirrors the geometry
 * column's bounding box per row, enabling row-group-level spatial pruning without native
 * {@link io.tileverse.parquetry.format.GeospatialStatistics}. Currently the spec only defines the {@link #bbox()}
 * covering kind.
 *
 * @param bbox per-row bbox sidecar columns; the spec mandates this when {@code covering} is present
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Covering(BboxCovering bbox) {}
