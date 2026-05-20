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
 * A latitude/longitude coordinate reference system anchored on a {@link GeodeticReferenceFrame}. The dominant case for
 * GeoParquet: OGC:CRS84 (and its EPSG sibling, EPSG:4326) are both {@code GeographicCRS}.
 *
 * @param name human-readable CRS name (e.g. {@code "WGS 84"}); empty when the writer did not record one
 * @param id registry {@link Identifier} (typically {@code EPSG:4326}); empty when the CRS is defined inline only
 * @param datum {@link GeodeticReferenceFrame} the lat/lon values are referenced to; empty when only the name is given
 * @param coordinateSystem axis definitions (order, units, direction); empty when defaulted
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GeographicCRS(
        Optional<String> name,
        Optional<Identifier> id,
        Optional<GeodeticReferenceFrame> datum,
        @JsonProperty("coordinate_system") Optional<CoordinateSystem> coordinateSystem)
        implements CoordinateReferenceSystem {}
