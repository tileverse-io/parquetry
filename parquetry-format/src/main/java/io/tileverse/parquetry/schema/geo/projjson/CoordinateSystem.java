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

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A PROJJSON coordinate system: the ordered set of {@link Axis}es a CRS uses. {@link #subtype()} carries the schema's
 * coordinate-system subtype identifier (commonly {@code "ellipsoidal"}, {@code "Cartesian"}, or {@code "vertical"}).
 *
 * <p>The axis array is serialized as {@code "axis"} per canonical PROJJSON v0.7. Older parquetry fixtures and snapshots
 * used {@code "axes"}; {@link JsonAlias} keeps that form readable so existing test resources continue to load.
 *
 * @param subtype coordinate-system family ({@code "ellipsoidal"}, {@code "Cartesian"}, {@code "vertical"}, ...); empty
 *     when not recorded
 * @param axes ordered axis definitions; the position in this list defines the coordinate order in stored geometries
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CoordinateSystem(
        Optional<String> subtype,
        @JsonProperty("axis") @JsonAlias("axes") List<Axis> axes) {

    public CoordinateSystem {
        axes = axes == null ? List.of() : List.copyOf(axes);
    }
}
