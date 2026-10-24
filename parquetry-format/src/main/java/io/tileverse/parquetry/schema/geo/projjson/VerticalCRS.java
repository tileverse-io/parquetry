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
 * A one-dimensional coordinate reference system for elevation or depth measurements. Combined with a horizontal CRS
 * inside a {@link CompoundCRS} to express 3D points; on its own it defines only the elevation axis.
 *
 * <p>The PROJJSON {@code vertical_datum} is not modeled here, and neither is {@code datum_ensemble}. A CRS parsed from
 * a document keeps both in {@link #raw()} and writes them back unchanged.
 *
 * @param name human-readable CRS name (e.g. {@code "EGM2008 height"}); empty when not recorded
 * @param id registry {@link Identifier}; empty when the CRS is defined inline only
 * @param coordinateSystem the elevation axis definition (direction up / down, unit)
 * @param raw the source document of this CRS, empty for a CRS built in code; see
 *     {@link CoordinateReferenceSystem#raw()}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VerticalCRS(
        Optional<String> name,
        Optional<Identifier> id,
        @JsonProperty("coordinate_system") Optional<CoordinateSystem> coordinateSystem,
        @JsonIgnore Optional<JsonNode> raw)
        implements CoordinateReferenceSystem {

    public VerticalCRS {
        raw = raw == null ? Optional.empty() : raw;
    }

    /** Builds a CRS from its properties, with no parsed document to write back. */
    public VerticalCRS(Optional<String> name, Optional<Identifier> id, Optional<CoordinateSystem> coordinateSystem) {
        this(name, id, coordinateSystem, Optional.empty());
    }

    @Override
    public VerticalCRS withRaw(JsonNode document) {
        return new VerticalCRS(name, id, coordinateSystem, Optional.of(document));
    }

    /** The parsed document takes no part: a parsed CRS equals a CRS built with the same properties. */
    @Override
    public boolean equals(Object o) {
        return o instanceof VerticalCRS other
                && Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(coordinateSystem, other.coordinateSystem);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, coordinateSystem);
    }
}
