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

import tools.jackson.databind.JsonNode;

/**
 * A PROJJSON prime meridian: the reference longitude (Greenwich by default). The {@code unit} field can be either a
 * plain string or a structured unit object per the PROJJSON schema; both are preserved as a raw {@link JsonNode}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PrimeMeridian(
        Optional<String> name, double longitude, Optional<JsonNode> unit, Optional<Identifier> id) {}
