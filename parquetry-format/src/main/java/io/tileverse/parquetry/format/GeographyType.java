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
package io.tileverse.parquetry.format;

import java.util.Optional;

/**
 * GeoParquet 2.0 logical-type marker for geography columns; mirror of {@code GeographyType} in {@code parquet.thrift}.
 *
 * <p>Carries the optional CRS identifier and the {@link EdgeInterpolationAlgorithm} used to interpret edges between
 * vertices on the ellipsoid. Sibling of {@link GeometryType}, which uses planar (Cartesian) edges.
 *
 * @param crs custom geographic CRS identifier; empty implies the spec default of {@code OGC:CRS84} (WGS 84 lon/lat)
 * @param algorithm edge-interpolation algorithm to use between vertices; empty implies the spec default of
 *     {@link EdgeInterpolationAlgorithm#SPHERICAL}
 */
public record GeographyType(Optional<String> crs, Optional<EdgeInterpolationAlgorithm> algorithm) {}
