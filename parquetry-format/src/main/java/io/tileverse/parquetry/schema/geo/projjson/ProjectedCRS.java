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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import tools.jackson.databind.JsonNode;

/**
 * A planar (easting/northing) coordinate reference system derived from a {@link #baseCrs()} via a {@link Conversion}.
 * The most common example in real data is the Web Mercator family (EPSG:3857) and the UTM zones.
 *
 * @param name human-readable CRS name (e.g. {@code "WGS 84 / Pseudo-Mercator"}); empty when not recorded
 * @param id registry {@link Identifier} (typically {@code EPSG:<code>}); empty when the CRS is inline only
 * @param baseCrs the source CRS the {@link #conversion} maps from (typically a {@link GeographicCRS})
 * @param conversion projection method + parameters that turn {@link #baseCrs} coordinates into this CRS's planar
 *     coordinates
 * @param coordinateSystem easting / northing axis definitions
 * @param raw the source document of this CRS, empty for a CRS built in code; see
 *     {@link CoordinateReferenceSystem#raw()}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProjectedCRS(
        Optional<String> name,
        Optional<Identifier> id,
        @JsonProperty("base_crs") Optional<CoordinateReferenceSystem> baseCrs,
        Optional<Conversion> conversion,
        @JsonProperty("coordinate_system") Optional<CoordinateSystem> coordinateSystem,
        @JsonIgnore Optional<JsonNode> raw)
        implements CoordinateReferenceSystem {

    public ProjectedCRS {
        raw = raw == null ? Optional.empty() : raw;
    }

    /** Builds a CRS from its properties, with no parsed document to write back. */
    public ProjectedCRS(
            Optional<String> name,
            Optional<Identifier> id,
            Optional<CoordinateReferenceSystem> baseCrs,
            Optional<Conversion> conversion,
            Optional<CoordinateSystem> coordinateSystem) {
        this(name, id, baseCrs, conversion, coordinateSystem, Optional.empty());
    }

    @Override
    public ProjectedCRS withRaw(JsonNode document) {
        return new ProjectedCRS(name, id, baseCrs, conversion, coordinateSystem, Optional.of(document));
    }

    /** The parsed document takes no part: a parsed CRS equals a CRS built with the same properties. */
    @Override
    public boolean equals(Object o) {
        return o instanceof ProjectedCRS other
                && Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(baseCrs, other.baseCrs)
                && Objects.equals(conversion, other.conversion)
                && Objects.equals(coordinateSystem, other.coordinateSystem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, baseCrs, conversion, coordinateSystem);
    }
}
