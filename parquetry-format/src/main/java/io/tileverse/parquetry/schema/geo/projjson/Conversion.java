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

import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import tools.jackson.databind.JsonNode;

/**
 * A PROJJSON conversion: the mathematical operation that derives a {@link ProjectedCRS} from its {@code base_crs}.
 *
 * <p>{@link #method()} names the algorithm (e.g. {@code "Transverse Mercator"}); {@link #parameters()} carries the
 * algorithm-specific values (false easting, scale factor, etc.). The parameter set is open-ended (EPSG plus PROJ
 * extensions), so it's modeled as a list of name-tagged records rather than typed per-method.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Conversion(Optional<String> name, Method method, List<Parameter> parameters) {

    public Conversion {
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
    }

    /**
     * The named operation method (Transverse Mercator, Lambert Conic Conformal, etc.). The {@link #id()} identifier,
     * when present, points at an EPSG operation method code (e.g. EPSG:9807 for Transverse Mercator).
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Method(Optional<String> name, Optional<Identifier> id) {}

    /**
     * One parameter of a conversion. PROJJSON is permissive about {@code value} and {@code unit}: most parameters carry
     * a numeric value with a unit string ({@code "degree"}, {@code "metre"}), but grid-based transformations (NADCON,
     * NTv2) store grid file paths as the {@code value} string, and the schema permits {@code unit} to be a structured
     * unit object. Both fields are kept as raw {@link JsonNode} to preserve every shape losslessly;
     * {@link #valueAsDouble()} and {@link #unitName()} cover the common typed accesses.
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Parameter(
            Optional<String> name, Optional<JsonNode> value, Optional<JsonNode> unit, Optional<Identifier> id) {

        /** Returns {@link #value()} as a {@code double} when the underlying JSON node is numeric; otherwise empty. */
        public OptionalDouble valueAsDouble() {
            return value.filter(JsonNode::isNumber)
                    .map(JsonNode::asDouble)
                    .map(OptionalDouble::of)
                    .orElse(OptionalDouble.empty());
        }

        /**
         * Returns the unit name as a string when {@link #unit()} is a JSON string, or when it is a JSON object with a
         * {@code "name"} property. Otherwise empty.
         */
        public Optional<String> unitName() {
            return unit.flatMap(node -> {
                if (node.isString()) {
                    return Optional.of(node.asString());
                }
                JsonNode nameNode = node.get("name");
                if (nameNode != null && nameNode.isString()) {
                    return Optional.of(nameNode.asString());
                }
                return Optional.empty();
            });
        }
    }
}
