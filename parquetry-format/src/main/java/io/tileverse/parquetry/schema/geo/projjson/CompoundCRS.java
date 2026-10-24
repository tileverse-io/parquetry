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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import tools.jackson.databind.JsonNode;

/**
 * A coordinate reference system composed of independent sub-CRSes. The canonical case is a horizontal CRS combined with
 * a vertical CRS, e.g. WGS 84 (lat/lon) + EGM2008 height.
 *
 * @param name human-readable CRS name; empty when not recorded
 * @param id registry {@link Identifier}; empty when the CRS is defined inline only
 * @param components the ordered list of sub-CRSes; PROJJSON convention puts the horizontal CRS first
 * @param raw the source document of this CRS, empty for a CRS built in code; see
 *     {@link CoordinateReferenceSystem#raw()}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CompoundCRS(
        Optional<String> name,
        Optional<Identifier> id,
        List<CoordinateReferenceSystem> components,
        @JsonIgnore Optional<JsonNode> raw)
        implements CoordinateReferenceSystem {

    public CompoundCRS {
        components = components == null ? List.of() : List.copyOf(components);
        raw = raw == null ? Optional.empty() : raw;
    }

    /** Builds a CRS from its properties, with no parsed document to write back. */
    public CompoundCRS(Optional<String> name, Optional<Identifier> id, List<CoordinateReferenceSystem> components) {
        this(name, id, components, Optional.empty());
    }

    @Override
    public CompoundCRS withRaw(JsonNode document) {
        return new CompoundCRS(name, id, components, Optional.of(document));
    }

    /** The parsed document takes no part: a parsed CRS equals a CRS built with the same properties. */
    @Override
    public boolean equals(Object o) {
        return o instanceof CompoundCRS other
                && Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(components, other.components);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, components);
    }
}
