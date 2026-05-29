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
package io.tileverse.parquetry.schema.geo.projjson;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A PROJJSON coordinate operation that converts coordinates from one CRS to another. Carried by {@link BoundCRS} via
 * {@link BoundCRS#transformation()}, and also valid as a top-level PROJJSON document (the PROJJSON schema treats
 * {@code Transformation} as a CRS-shaped object).
 *
 * @param name human-readable transformation name; empty when not recorded
 * @param sourceCrs CRS the operation reads coordinates from
 * @param targetCrs CRS the operation writes coordinates to
 * @param method the named operation method (e.g. {@code "Geocentric translations (geog2D domain)"}, NTv2 grid shift,
 *     NADCON file); empty when the writer only recorded parameters
 * @param parameters method-specific numeric or file-reference parameters; defensively copied
 * @param accuracy stated accuracy of the operation in metres, as a free-form string from the writer
 * @param id registry {@link Identifier} (typically an EPSG operation code); empty when defined inline only
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Transformation(
        Optional<String> name,
        @JsonProperty("source_crs") Optional<CoordinateReferenceSystem> sourceCrs,
        @JsonProperty("target_crs") Optional<CoordinateReferenceSystem> targetCrs,
        Optional<Conversion.Method> method,
        List<Conversion.Parameter> parameters,
        Optional<String> accuracy,
        Optional<Identifier> id)
        implements CoordinateReferenceSystem {

    public Transformation {
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
    }
}
