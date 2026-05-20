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
import java.util.OptionalDouble;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A PROJJSON ellipsoid (figure of the Earth approximation).
 *
 * <p>Exactly one of {@code semi_minor_axis} or {@code inverse_flattening} is canonically provided; the schema permits
 * both for redundancy. {@code semi_major_axis} is always present on a valid PROJJSON ellipsoid.
 *
 * @param name human-readable ellipsoid name (e.g. {@code "WGS 84"}); empty when not recorded
 * @param semiMajorAxis equatorial radius in metres; required by the PROJJSON schema
 * @param semiMinorAxis polar radius in metres; empty when the writer only recorded {@link #inverseFlattening}
 * @param inverseFlattening reciprocal of the flattening; empty when the writer only recorded {@link #semiMinorAxis}
 * @param id registry {@link Identifier} (typically an EPSG ellipsoid code); empty when defined inline only
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Ellipsoid(
        Optional<String> name,
        @JsonProperty("semi_major_axis") double semiMajorAxis,
        @JsonProperty("semi_minor_axis") OptionalDouble semiMinorAxis,
        @JsonProperty("inverse_flattening") OptionalDouble inverseFlattening,
        Optional<Identifier> id) {}
