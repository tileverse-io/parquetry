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
 * A CRS paired with a {@link Transformation} to a reference CRS. PROJ emits {@code BoundCRS} when transforming a
 * dataset from a regional CRS to WGS 84 without altering the source CRS definition itself.
 *
 * @param name human-readable CRS name; empty when not recorded
 * @param id registry {@link Identifier}; empty when the CRS is defined inline only
 * @param sourceCrs the CRS the data is in (the "bound" CRS)
 * @param targetCrs the CRS the {@link #transformation} maps the source data to
 * @param transformation the operation that takes {@link #sourceCrs} coordinates to {@link #targetCrs} coordinates
 * @param raw the source document of this CRS, empty for a CRS built in code; see
 *     {@link CoordinateReferenceSystem#raw()}
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BoundCRS(
        Optional<String> name,
        Optional<Identifier> id,
        @JsonProperty("source_crs") Optional<CoordinateReferenceSystem> sourceCrs,
        @JsonProperty("target_crs") Optional<CoordinateReferenceSystem> targetCrs,
        Optional<Transformation> transformation,
        @JsonIgnore Optional<JsonNode> raw)
        implements CoordinateReferenceSystem {

    public BoundCRS {
        raw = raw == null ? Optional.empty() : raw;
    }

    /** Builds a CRS from its properties, with no parsed document to write back. */
    public BoundCRS(
            Optional<String> name,
            Optional<Identifier> id,
            Optional<CoordinateReferenceSystem> sourceCrs,
            Optional<CoordinateReferenceSystem> targetCrs,
            Optional<Transformation> transformation) {
        this(name, id, sourceCrs, targetCrs, transformation, Optional.empty());
    }

    @Override
    public BoundCRS withRaw(JsonNode document) {
        return new BoundCRS(name, id, sourceCrs, targetCrs, transformation, Optional.of(document));
    }

    /** The parsed document takes no part: a parsed CRS equals a CRS built with the same properties. */
    @Override
    public boolean equals(Object o) {
        return o instanceof BoundCRS other
                && Objects.equals(name, other.name)
                && Objects.equals(id, other.id)
                && Objects.equals(sourceCrs, other.sourceCrs)
                && Objects.equals(targetCrs, other.targetCrs)
                && Objects.equals(transformation, other.transformation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, sourceCrs, targetCrs, transformation);
    }
}
