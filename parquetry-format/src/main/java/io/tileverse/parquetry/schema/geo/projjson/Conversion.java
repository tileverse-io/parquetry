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
 *
 * @param name human-readable conversion name; empty when not recorded
 * @param method the named operation method (Transverse Mercator, Lambert Conic Conformal, ...)
 * @param parameters method-specific parameters; defensively copied
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Conversion(Optional<String> name, Method method, List<Parameter> parameters) {

    public Conversion {
        parameters = parameters == null ? List.of() : List.copyOf(parameters);
    }

    /**
     * The named operation method (Transverse Mercator, Lambert Conic Conformal, etc.). The {@link #id()} identifier,
     * when present, points at an EPSG operation method code (e.g. EPSG:9807 for Transverse Mercator).
     *
     * @param name human-readable method name; empty when not recorded
     * @param id registry {@link Identifier} (typically an EPSG operation method code); empty when defined inline only
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Method(Optional<String> name, Optional<Identifier> id) {}

    /**
     * One parameter of a conversion. PROJJSON is permissive about {@code value} and {@code unit}: most parameters carry
     * a numeric value with a unit string ({@code "degree"}, {@code "metre"}), but grid-based transformations (NADCON,
     * NTv2) store grid file paths as the {@code value} string, and the schema permits {@code unit} to be a structured
     * unit object. Both fields are kept as raw {@link JsonNode} to preserve every shape losslessly;
     * {@link #valueAsDouble()} and {@link #unitName()} cover the common typed accesses.
     *
     * @param name parameter name (e.g. {@code "False easting"}, {@code "Scale factor at natural origin"}); empty when
     *     not recorded
     * @param value parameter value as a raw JSON node so numeric, string (file path), and structured shapes all round
     *     trip; use {@link #valueAsDouble()} for the numeric case
     * @param unit unit annotation as a raw JSON node (string or structured object); use {@link #unitName()} for the
     *     simple string accessor
     * @param id registry {@link Identifier} (typically an EPSG parameter code); empty when defined inline only
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
