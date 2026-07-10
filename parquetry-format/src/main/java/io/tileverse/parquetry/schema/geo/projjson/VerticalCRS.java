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

import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A one-dimensional coordinate reference system for elevation or depth measurements. Combined with a horizontal CRS
 * inside a {@link CompoundCRS} to express 3D points; on its own it carries only the elevation axis.
 *
 * <p>The {@code datum} is currently retained as a raw {@link Object} because the PROJJSON {@code VerticalDatum} type is
 * not modeled here; it surfaces as a generic node so callers that need it can inspect it. Future work can promote this
 * to a typed record if a parquetry-side consumer requires it.
 *
 * @param name human-readable CRS name (e.g. {@code "EGM2008 height"}); empty when not recorded
 * @param id registry {@link Identifier}; empty when the CRS is defined inline only
 * @param coordinateSystem the elevation axis definition (direction up / down, unit)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record VerticalCRS(
        Optional<String> name,
        Optional<Identifier> id,
        @JsonProperty("coordinate_system") Optional<CoordinateSystem> coordinateSystem)
        implements CoordinateReferenceSystem {}
