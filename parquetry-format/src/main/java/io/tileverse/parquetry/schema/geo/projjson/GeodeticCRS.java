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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A geocentric (X/Y/Z) coordinate reference system anchored on a {@link GeodeticReferenceFrame}. Distinct from
 * {@link GeographicCRS} in that its axes are Cartesian rather than lat/lon. PROJ uses {@code GeodeticCRS} for the base
 * CRS of certain projections (e.g. some EPSG operation methods that operate on X/Y/Z rather than latitude / longitude).
 *
 * @param name human-readable CRS name; empty when the writer did not record one
 * @param id registry {@link Identifier}; empty when the CRS is defined inline only
 * @param datum {@link GeodeticReferenceFrame} the X/Y/Z values are referenced to
 * @param coordinateSystem Cartesian axis definitions (typically X / Y / Z in metres)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GeodeticCRS(
        Optional<String> name,
        Optional<Identifier> id,
        Optional<GeodeticReferenceFrame> datum,
        @JsonProperty("coordinate_system") Optional<CoordinateSystem> coordinateSystem)
        implements CoordinateReferenceSystem {}
