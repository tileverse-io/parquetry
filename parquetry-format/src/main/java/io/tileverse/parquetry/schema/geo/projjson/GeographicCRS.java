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
 * A coordinate reference system of angular coordinates anchored on a {@link GeodeticReferenceFrame}. The dominant case
 * for GeoParquet: OGC:CRS84 and its EPSG sibling EPSG:4326 are both {@code GeographicCRS}. They differ in axis order,
 * stated by {@link #coordinateSystem()}: CRS84 is lon/lat and EPSG:4326 is lat/lon.
 *
 * <p>PROJ defines both of those with a {@code datum_ensemble} rather than a {@code datum}, which no record in this
 * package models. {@link #raw()} is what keeps such a CRS writable; see {@link CoordinateReferenceSystem#raw()}.
 *
 * @param name human-readable CRS name (e.g. {@code "WGS 84"}); empty when the writer did not record one
 * @param id registry {@link Identifier} (typically {@code EPSG:4326}); empty when the CRS is defined inline only
 * @param datum {@link GeodeticReferenceFrame} the coordinates are referenced to; empty when only the name is given, and
 *     empty as well when the CRS defines its frame with a {@code datum_ensemble}
 * @param coordinateSystem axis definitions (order, units, direction); empty when defaulted
 * @param raw the source document of this CRS, empty for a CRS built in code
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GeographicCRS(
        Optional<String> name,
        Optional<Identifier> id,
        Optional<GeodeticReferenceFrame> datum,
        @JsonProperty("coordinate_system") Optional<CoordinateSystem> coordinateSystem,
        @JsonIgnore Optional<JsonNode> raw)
        implements CoordinateReferenceSystem {

    public GeographicCRS {
        raw = raw == null ? Optional.empty() : raw;
    }

    /** Builds a CRS from its properties, with no parsed document to write back. */
    public GeographicCRS(
            Optional<String> name,
            Optional<Identifier> id,
            Optional<GeodeticReferenceFrame> datum,
            Optional<CoordinateSystem> coordinateSystem) {
        this(name, id, datum, coordinateSystem, Optional.empty());
    }

    @Override
    public GeographicCRS withRaw(JsonNode document) {
        return new GeographicCRS(name, id, datum, coordinateSystem, Optional.of(document));
    }

    /** The parsed document takes no part: a parsed CRS equals a CRS built with the same properties. */
    @Override
    public boolean equals(Object o) {
        return o instanceof GeographicCRS other
                && Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(datum, other.datum)
                && Objects.equals(coordinateSystem, other.coordinateSystem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, datum, coordinateSystem);
    }
}
