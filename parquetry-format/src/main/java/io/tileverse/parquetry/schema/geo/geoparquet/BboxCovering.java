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
package io.tileverse.parquetry.schema.geo.geoparquet;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import io.tileverse.parquetry.schema.ColumnPath;

/**
 * GeoParquet 1.1 {@code covering.bbox} structure: maps each of the four (or six) bbox extrema to the typed
 * {@link ColumnPath} that stores it. The referenced columns are regular numeric columns (DOUBLE / FLOAT) whose
 * {@code Statistics} carry the per-row-group min/max, enabling spatial pruning on a 1.1 file without native
 * {@code GeospatialStatistics}.
 *
 * <p>Example: a covering group named {@code "bbox"} with {@code xmin}, {@code ymin}, {@code xmax}, {@code ymax} leaves
 * is encoded as four {@code ["bbox", "xmin"]} / {@code ["bbox", "ymin"]} / {@code ["bbox", "xmax"]} / {@code ["bbox",
 * "ymax"]} paths.
 *
 * @param xmin path to the numeric column carrying the per-row xmin
 * @param xmax path to the numeric column carrying the per-row xmax
 * @param ymin path to the numeric column carrying the per-row ymin
 * @param ymax path to the numeric column carrying the per-row ymax
 * @param zmin optional path to the per-row zmin column when the geometry carries Z
 * @param zmax optional path to the per-row zmax column when the geometry carries Z
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BboxCovering(
        ColumnPath xmin,
        ColumnPath xmax,
        ColumnPath ymin,
        ColumnPath ymax,
        Optional<ColumnPath> zmin,
        Optional<ColumnPath> zmax) {}
